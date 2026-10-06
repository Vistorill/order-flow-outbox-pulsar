import { and, eq, inArray, sql } from 'drizzle-orm';
import { fromCents } from '../common/money';
import type { DbOrTx } from '../db/db.module';
import {
  idempotencyKeys,
  inboundWebhooks,
  orders,
  outbox,
  processedEvents,
  shipments,
  type Order,
} from '../db/schema';
import {
  EventTypes,
  type DeliveryStatus,
  type PaymentStatus,
} from '../events/topics';
import { deliveryStatusOf, paymentStatusOf } from './order-status';

export interface ReportLine {
  n: number;
  at: string | null;
  text: string;
}

export interface OrderReport {
  orderId: string;
  result: Order['status'];
  paymentStatus: PaymentStatus;
  paidAt: string | null;
  deliveryStatus: DeliveryStatus;
  customerEmail: string;
  amount: string;
  idempotencyKey: string | null;
  startedAt: string;
  finishedAt: string | null;
  durationMs: number | null;
  duplicates: {
    /** Reenvios da requisição com a mesma Idempotency-Key (recusados com 409). */
    requests: number;
    /** Cópias de eventos entregues de novo pelo broker (descartadas pela inbox). */
    brokerRedeliveries: number;
    /** Reenvios do webhook de entrega pela transportadora (ignorados). */
    carrierWebhooks: number;
    total: number;
  };
  lines: ReportLine[];
  summary: string;
  /** Igual ao summary: o painel e o receptor de demo mostram `message`. */
  message: string;
}

const TZ = 'America/Sao_Paulo';
const hour = (d: Date) => d.toLocaleTimeString('pt-BR', { timeZone: TZ });
const brl = (cents: bigint) => `R$ ${fromCents(cents).replace('.', ',')}`;
const elapsed = (from: Date, to: Date) => {
  const ms = to.getTime() - from.getTime();
  return ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(1).replace('.', ',')}s`;
};
const times = (n: number) => `${n} ${n === 1 ? 'vez' : 'vezes'}`;
const ordinal = (n: number) => `${n}ª tentativa`;

/**
 * Relatório do pedido em linguagem simples e passos numerados: como a
 * requisição começou, o que aconteceu no meio e se algo chegou duplicado.
 * Lido do banco (funciona dentro de uma transação aberta, passando o `tx`).
 * Usado no passo 11 do painel e no webhook final `order.completed`.
 */
export async function buildOrderReport(
  db: DbOrTx,
  orderId: string,
): Promise<OrderReport | null> {
  const [found] = await db.select().from(orders).where(eq(orders.id, orderId));
  if (!found) return null;
  const o: Order = found;

  const [idem] = await db
    .select()
    .from(idempotencyKeys)
    .where(sql`${idempotencyKeys.responseBody}->>'id' = ${o.id}`);
  const events = await db
    .select()
    .from(outbox)
    .where(eq(outbox.aggregateId, o.id));
  const created = events.find((e) => e.eventType === EventTypes.OrderCreated);
  const confirmedEvt = events.find(
    (e) => e.eventType === EventTypes.OrderConfirmed,
  );
  const processed = events.length
    ? await db
        .select()
        .from(processedEvents)
        .where(
          inArray(
            processedEvents.eventId,
            events.map((e) => e.id),
          ),
        )
    : [];
  const [shipment] = await db
    .select()
    .from(shipments)
    .where(eq(shipments.orderId, o.id));
  const [inbound] = await db
    .select()
    .from(inboundWebhooks)
    .where(
      and(
        eq(inboundWebhooks.eventType, 'shipment.delivered'),
        sql`${inboundWebhooks.payload}->'data'->>'reference' = ${o.id}`,
      ),
    )
    .limit(1);

  const dupRequests = idem?.duplicateRequests ?? 0;
  const dupBroker = processed.reduce((acc, p) => acc + p.duplicates, 0);
  const dupCarrier = inbound?.duplicates ?? 0;
  const dupTotal = dupRequests + dupBroker + dupCarrier;
  const start = o.createdAt;
  const lines: Omit<ReportLine, 'n'>[] = [];
  const add = (at: Date | null | undefined, text: string) =>
    lines.push({ at: at ? at.toISOString() : null, text });

  // 1. Começo da requisição
  add(
    start,
    `Às ${hour(start)} o cliente enviou POST /orders (${brl(o.amountCents)}, ${o.customerEmail})${idem ? ` com a Idempotency-Key ${idem.key}` : ''}.`,
  );

  // 2. Meio da transação
  add(
    created?.createdAt ?? start,
    `Numa única transação, o pedido ${o.id.slice(0, 8)} foi criado como PENDING e o evento OrderCreated foi gravado na outbox. COMMIT: os dois juntos, ou nenhum.`,
  );

  // 3. Duplicidade da requisição
  add(
    idem?.lastDuplicateAt ?? null,
    dupRequests
      ? `A mesma requisição chegou de novo ${times(dupRequests)} com a mesma chave. ${dupRequests === 1 ? 'Foi recusada' : 'Todas foram recusadas'} com 409 (transação duplicada) e nenhum pedido extra foi criado.`
      : 'Nenhuma requisição duplicada: a chave foi usada uma única vez.',
  );

  // 4. Relay
  if (created?.status === 'published' && created.publishedAt) {
    add(
      created.publishedAt,
      `O relay publicou OrderCreated no Pulsar em +${elapsed(start, created.publishedAt)}, na ${ordinal(created.attempts)}${created.attempts > 1 ? ' (as anteriores falharam e foram retentadas com backoff)' : ''}.`,
    );
  } else if (created) {
    add(
      null,
      `Em andamento: o relay ainda vai publicar OrderCreated (${created.attempts} tentativa(s) até agora${created.lastError ? `, último erro: ${created.lastError}` : ''}).`,
    );
    return finish();
  }

  // 5. Pagamento
  if (o.confirmedAt) {
    const copies =
      processed.find((p) => p.eventId === created?.id)?.duplicates ?? 0;
    add(
      o.confirmedAt,
      `O consumidor confirmou o pedido em +${elapsed(start, o.confirmedAt)}: pagamento PAID.${copies ? ` O broker entregou o mesmo evento mais ${times(copies)} e as cópias foram descartadas pela inbox (processed_events).` : ''}`,
    );
  } else {
    add(null, 'Em andamento: aguardando o consumidor confirmar o pagamento.');
    return finish();
  }

  // 6. Transportadora
  if (shipment?.status === 'ACCEPTED' || shipment?.status === 'DELIVERED') {
    add(
      shipment.acceptedAt,
      `O serviço de envio chamou a ${shipment.carrier}: remessa ${shipment.externalId} aceita (${shipment.lastStatusCode ?? 202}) em +${elapsed(start, shipment.acceptedAt!)}, na ${ordinal(shipment.attempts)}${shipment.attempts > 1 ? ', sempre com a mesma Idempotency-Key: nenhuma remessa duplicada' : ''}.`,
    );
  } else {
    add(
      null,
      shipment?.lastError
        ? `Em andamento: a transportadora falhou (${shipment.lastError}) e a chamada será repetida após o NACK (${shipment.attempts} tentativa(s)).`
        : `Em andamento: ${confirmedEvt?.status === 'published' ? 'chamando' : 'aguardando o relay publicar OrderConfirmed para chamar'} a transportadora.`,
    );
    return finish();
  }

  // 7. Entrega
  if (inbound) {
    add(
      inbound.receivedAt,
      `A transportadora entregou e avisou por webhook assinado em +${elapsed(start, inbound.receivedAt)} (assinatura válida).${dupCarrier ? ` O mesmo aviso chegou mais ${times(dupCarrier)} e foi ignorado.` : ''}`,
    );
  } else {
    add(null, 'Em andamento: a transportadora está fazendo a entrega.');
    return finish();
  }

  return finish();

  function finish(): OrderReport {
    const done = o.status === 'DELIVERED' && o.deliveredAt;
    const finishedAt = done ? o.deliveredAt : null;
    const verdict = dupTotal
      ? `Houve ${dupTotal === 1 ? '1 duplicidade, neutralizada' : `${dupTotal} duplicidades, todas neutralizadas`}: um pedido, um pagamento, uma remessa.`
      : 'Sem duplicidades.';
    const summary = done
      ? `Pedido ${o.id.slice(0, 8)} pago e entregue em ${elapsed(start, finishedAt!)}. ${verdict}`
      : `Pedido ${o.id.slice(0, 8)} em andamento (${o.status}). ${verdict}`;
    if (done)
      add(
        finishedAt,
        `Resultado final: pedido DELIVERED e PAID em ${elapsed(start, finishedAt!)}. ${verdict}`,
      );
    return {
      orderId: o.id,
      result: o.status,
      paymentStatus: paymentStatusOf(o),
      paidAt: o.confirmedAt ? o.confirmedAt.toISOString() : null,
      deliveryStatus: deliveryStatusOf(o),
      customerEmail: o.customerEmail,
      amount: fromCents(o.amountCents),
      idempotencyKey: idem?.key ?? null,
      startedAt: start.toISOString(),
      finishedAt: finishedAt ? finishedAt.toISOString() : null,
      durationMs: finishedAt ? finishedAt.getTime() - start.getTime() : null,
      duplicates: {
        requests: dupRequests,
        brokerRedeliveries: dupBroker,
        carrierWebhooks: dupCarrier,
        total: dupTotal,
      },
      lines: lines.map((l, i) => ({ n: i + 1, ...l })),
      summary,
      message: summary,
    };
  }
}
