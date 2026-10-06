import { Inject, Injectable } from '@nestjs/common';
import { desc, eq, inArray, sql } from 'drizzle-orm';
import { DRIZZLE, type Database } from '../db/db.module';
import {
  idempotencyKeys,
  inboundWebhooks,
  orders,
  outbox,
  processedEvents,
  shipments,
  webhookAttempts,
  webhookDeliveries,
  webhookEndpoints,
  type Order,
} from '../db/schema';
import { EventTypes } from '../events/topics';
import { buildOrderReport, type ReportLine } from '../orders/order-report';
import { CarrierClient } from '../shipping/carrier.client';

export type StepStatus = 'done' | 'active' | 'pending' | 'error' | 'warn';

export interface JourneyStep {
  n: number;
  title: string;
  /** Quem executa o passo (API, Postgres, Relay, Pulsar...). */
  actor: string;
  status: StepStatus;
  at: string | null;
  /** Explicação fixa do passo, para quem está assistindo a demo. */
  explain: string;
  info: Record<string, string | number>;
  json?: { label: string; value: unknown }[];
  /** Passo 11: o relatório em frases numeradas. */
  report?: ReportLine[];
  /** Passo 11: status da transação numa linha (pedido, pagamento, entrega, webhook). */
  headline?: {
    label: string;
    value: string;
    tone: 'ok' | 'warn' | 'bad' | 'info';
  }[];
  /** Passo 11: por que o webhook final não saiu e o que corrigir. */
  fix?: {
    reason: string;
    orderId: string;
    /** Endpoints ativos que não assinam order.completed. */
    endpoints: { id: string; url: string; events: string[] }[];
  };
}

export interface Journey {
  orderId: string;
  status: Order['status'];
  customerEmail: string;
  amount: string;
  startedAt: string;
  finishedAt: string | null;
  steps: JourneyStep[];
}

const iso = (d: Date | null | undefined) => (d ? d.toISOString() : null);
const cents = (v: bigint) => (Number(v) / 100).toFixed(2);

/**
 * Monta, a partir do BANCO, o caminho completo de um pedido em 11 passos:
 * da requisição até o cliente final ser avisado da entrega. Como tudo vem
 * das tabelas (e não de memória), funciona mesmo com várias instâncias.
 */
@Injectable()
export class JourneyService {
  constructor(
    @Inject(DRIZZLE) private readonly db: Database,
    private readonly carrier: CarrierClient,
  ) {}

  async latest(limit = 8): Promise<Journey[]> {
    const rows = await this.db
      .select()
      .from(orders)
      .orderBy(desc(orders.createdAt))
      .limit(limit);
    return Promise.all(rows.map((o) => this.build(o)));
  }

  private async build(o: Order): Promise<Journey> {
    const [idem] = await this.db
      .select()
      .from(idempotencyKeys)
      .where(sql`${idempotencyKeys.responseBody}->>'id' = ${o.id}`);
    const events = await this.db
      .select()
      .from(outbox)
      .where(eq(outbox.aggregateId, o.id));
    const created = events.find((e) => e.eventType === EventTypes.OrderCreated);
    const confirmedEvt = events.find(
      (e) => e.eventType === EventTypes.OrderConfirmed,
    );
    const eventIds = events.map((e) => e.id);
    const processed = eventIds.length
      ? await this.db
          .select()
          .from(processedEvents)
          .where(inArray(processedEvents.eventId, eventIds))
      : [];
    const [shipment] = await this.db
      .select()
      .from(shipments)
      .where(eq(shipments.orderId, o.id));
    const [inbound] = await this.db
      .select()
      .from(inboundWebhooks)
      .where(sql`${inboundWebhooks.payload}->'data'->>'reference' = ${o.id}`)
      .orderBy(inboundWebhooks.receivedAt)
      .limit(1);
    const deliveries = await this.db
      .select({
        id: webhookDeliveries.id,
        eventType: webhookDeliveries.eventType,
        payload: webhookDeliveries.payload,
        status: webhookDeliveries.status,
        attempts: webhookDeliveries.attempts,
        lastStatusCode: webhookDeliveries.lastStatusCode,
        lastError: webhookDeliveries.lastError,
        deliveredAt: webhookDeliveries.deliveredAt,
        url: webhookEndpoints.url,
      })
      .from(webhookDeliveries)
      .innerJoin(
        webhookEndpoints,
        eq(webhookEndpoints.id, webhookDeliveries.endpointId),
      )
      .where(sql`${webhookDeliveries.payload}->'data'->>'orderId' = ${o.id}`);
    const lastAttempts = deliveries.length
      ? await this.db
          .select()
          .from(webhookAttempts)
          .where(
            inArray(
              webhookAttempts.deliveryId,
              deliveries.map((d) => d.id),
            ),
          )
      : [];

    const proc = (eventId?: string) =>
      processed.find((p) => p.eventId === eventId);
    const publishStep = (
      evt: typeof created,
    ): Pick<JourneyStep, 'status' | 'at' | 'info'> => {
      if (!evt) return { status: 'pending', at: null, info: {} };
      const info: Record<string, string | number> = {
        evento: evt.id,
        tópico: evt.topic,
        status: evt.status,
        tentativas: evt.attempts,
      };
      if (evt.lastError) info['último erro'] = evt.lastError;
      if (evt.status === 'published') {
        return { status: 'done', at: iso(evt.publishedAt), info };
      }
      if (evt.status === 'failed') return { status: 'error', at: null, info };
      return {
        status: evt.lastError ? 'warn' : 'pending',
        at: null,
        info,
      };
    };

    const delivered = (type: string) =>
      deliveries.filter((d) => d.eventType === type);
    const customerDelivered = delivered('order.delivered');
    const customerConfirmed = delivered('order.confirmed');
    const describe = (list: typeof deliveries) =>
      list.length
        ? list
            .map(
              (d) =>
                `${d.status}${d.lastStatusCode ? ` (${d.lastStatusCode})` : ''} em ${d.attempts} tentativa(s) → ${d.url}`,
            )
            .join(' · ')
        : 'nenhum endpoint ativo';

    const steps: JourneyStep[] = [
      {
        n: 1,
        title: 'Cliente chama a API',
        actor: 'Cliente → API',
        status: 'done',
        at: iso(o.createdAt),
        explain:
          'POST /orders com o header Idempotency-Key. Se o cliente repetir a chamada (timeout, duplo clique), a mesma chave impede um segundo pedido.',
        info: {
          'Idempotency-Key': idem?.key ?? '—',
          cliente: o.customerEmail,
          valor: `R$ ${cents(o.amountCents)}`,
          resposta: idem?.responseStatus ?? 201,
        },
      },
      {
        n: 2,
        title: 'Transação: pedido + evento na outbox',
        actor: 'API → Postgres',
        status: created ? 'done' : 'pending',
        at: iso(created?.createdAt),
        explain:
          'Numa ÚNICA transação: INSERT em orders (PENDING) e INSERT do evento OrderCreated na tabela outbox. Ou os dois ficam gravados, ou nenhum. Não existe pedido sem evento.',
        info: created
          ? {
              'orders.id': o.id,
              'outbox.id': created.id,
              evento: created.eventType,
            }
          : {},
      },
      {
        n: 3,
        title: 'Relay publica OrderCreated no Pulsar',
        actor: 'Relay → Pulsar',
        ...publishStep(created),
        explain:
          'O relay lê a outbox a cada segundo (FOR UPDATE SKIP LOCKED), publica no tópico order.created e só então marca published. Se o Pulsar estiver fora, tenta de novo com backoff.',
      },
      {
        n: 4,
        title: 'Consumidor confirma o pedido',
        actor: 'Pulsar → Consumidor',
        status: o.confirmedAt ? 'done' : 'pending',
        at: iso(o.confirmedAt),
        explain:
          'O consumidor order-confirmation grava o eventId em processed_events (inbox) e muda o pedido para CONFIRMED na mesma transação. Uma entrega duplicada do broker é descartada.',
        info: proc(created?.id)
          ? {
              processed_events: `${created!.id.slice(0, 8)} / ${proc(created?.id)!.consumer}`,
              pedido: 'CONFIRMED',
            }
          : {},
      },
      {
        n: 5,
        title: 'Próximo evento e aviso ao cliente, na mesma transação',
        actor: 'Consumidor → Postgres',
        status: confirmedEvt ? 'done' : 'pending',
        at: iso(confirmedEvt?.createdAt),
        explain:
          'Junto com a confirmação, grava OrderConfirmed na outbox (para o serviço de envio) e o webhook order.confirmed para o cliente final. Mesma ideia do passo 2, um salto adiante.',
        info: confirmedEvt
          ? {
              'outbox.id': confirmedEvt.id,
              'webhook order.confirmed': describe(customerConfirmed),
            }
          : {},
      },
      {
        n: 6,
        title: 'Relay publica OrderConfirmed no Pulsar',
        actor: 'Relay → Pulsar',
        ...publishStep(confirmedEvt),
        explain:
          'O mesmo relay publica o novo evento no tópico order.confirmed, assinado pelo serviço de envio (subscription shipping).',
      },
      {
        n: 7,
        title: `Serviço de envio chama a API da ${this.carrier.name}`,
        actor: 'Envio → API externa',
        status: !shipment
          ? 'pending'
          : shipment.status === 'REQUESTED'
            ? shipment.lastError
              ? 'error'
              : 'pending'
            : 'done',
        at: iso(shipment?.acceptedAt),
        explain:
          'POST /shipments na API da transportadora, com Idempotency-Key = id do evento. Se ela falhar (5xx, timeout), o evento volta para o broker (NACK) e a chamada é repetida sem criar remessa duplicada. Sucesso: pedido SHIPPED.',
        info: shipment
          ? {
              POST: `${this.carrier.baseUrl}/shipments`,
              tentativas: shipment.attempts,
              resposta: shipment.lastStatusCode ?? '—',
              ...(shipment.externalId ? { remessa: shipment.externalId } : {}),
              ...(shipment.trackingCode
                ? { rastreio: shipment.trackingCode }
                : {}),
              ...(shipment.lastError ? { erro: shipment.lastError } : {}),
            }
          : {},
        json: shipment
          ? [
              { label: 'Requisição enviada', value: shipment.requestBody },
              {
                label: 'Resposta da transportadora',
                value: shipment.lastResponse,
              },
            ]
          : undefined,
      },
      {
        n: 8,
        title: 'Transportadora faz a entrega',
        actor: this.carrier.name,
        status: inbound ? 'done' : 'pending',
        at: inbound
          ? ((inbound.payload as { data?: { deliveredAt?: string } }).data
              ?.deliveredAt ?? null)
          : null,
        explain:
          'Do lado de lá, a remessa sai para entrega. Nossa API não fica esperando: ela vai ser avisada por webhook quando terminar.',
        info: shipment?.acceptedAt
          ? {
              'aceita em': shipment.acceptedAt.toISOString(),
              rastreio: shipment.trackingCode ?? '—',
            }
          : {},
      },
      {
        n: 9,
        title: 'Webhook de entrega chega na nossa API',
        actor: `${this.carrier.name} → API`,
        status: inbound ? 'done' : 'pending',
        at: iso(inbound?.receivedAt),
        explain:
          'POST /webhooks/inbound/carrier com assinatura HMAC (webhook-signature). Validamos a assinatura, gravamos em inbound_webhooks (o id do evento é único, então reenvios não reprocessam) e marcamos o pedido como DELIVERED.',
        info: inbound
          ? {
              'webhook-id': inbound.externalEventId,
              evento: inbound.eventType,
              assinatura: 'válida',
              pedido: o.status,
            }
          : {},
        json: inbound
          ? [
              { label: 'Headers recebidos', value: inbound.headers },
              { label: 'Corpo recebido', value: inbound.payload },
            ]
          : undefined,
      },
      {
        n: 10,
        title: 'Cliente final recebe o webhook order.delivered',
        actor: 'API → Cliente final',
        status: !inbound
          ? 'pending'
          : !customerDelivered.length
            ? 'warn'
            : customerDelivered.every((d) => d.status === 'delivered')
              ? 'done'
              : customerDelivered.some((d) => d.status === 'failed')
                ? 'error'
                : 'pending',
        at: iso(
          customerDelivered.find((d) => d.deliveredAt)?.deliveredAt ?? null,
        ),
        explain:
          'O webhook order.delivered nasceu na transação do passo 9. O dispatcher faz POST assinado no endpoint do cliente, com retry e backoff, até receber 2xx.',
        info: inbound
          ? {
              'order.delivered': describe(customerDelivered),
              'order.confirmed': describe(customerConfirmed),
            }
          : {},
        json: lastAttempts.length
          ? [
              {
                label: 'Última tentativa de webhook ao cliente',
                value: lastAttempts[lastAttempts.length - 1],
              },
            ]
          : undefined,
      },
    ];

    // 11. Relatório final + webhook order.completed com o resultado.
    const report = await buildOrderReport(this.db, o.id);
    const completed = delivered('order.completed');
    const d = report?.duplicates;
    const activeEndpoints = await this.db
      .select({
        id: webhookEndpoints.id,
        url: webhookEndpoints.url,
        events: webhookEndpoints.events,
      })
      .from(webhookEndpoints)
      .where(eq(webhookEndpoints.active, true));
    const subscribes = (events: string[]) =>
      events.includes('order.completed') || events.includes('*');
    const missing = activeEndpoints.filter((e) => !subscribes(e.events));

    const finalWebhook = !inbound
      ? { value: 'sai quando a entrega terminar', tone: 'info' as const }
      : completed.length && completed.every((x) => x.status === 'delivered')
        ? {
            value: `entregue (${completed.map((x) => x.lastStatusCode).join(', ')}) a ${completed.length} endpoint(s)`,
            tone: 'ok' as const,
          }
        : completed.some((x) => x.status === 'failed')
          ? { value: 'falhou: veja a aba Webhooks', tone: 'bad' as const }
          : completed.length
            ? { value: 'na fila / retentando', tone: 'info' as const }
            : {
                value: activeEndpoints.length
                  ? 'não enviado: nenhum endpoint assina order.completed'
                  : 'não enviado: nenhum endpoint cadastrado',
                tone: 'bad' as const,
              };
    const fix =
      inbound && !completed.length
        ? {
            reason: activeEndpoints.length
              ? `${missing.length} endpoint(s) ativo(s) não assina(m) order.completed. Assine o evento (ou "todos os eventos") e reenvie o resultado final.`
              : 'Nenhum endpoint de webhook cadastrado: cadastre um no painel "Webhook do cliente final" e reenvie o resultado final.',
            orderId: o.id,
            endpoints: missing,
          }
        : undefined;
    const sameUrl =
      activeEndpoints.length - new Set(activeEndpoints.map((e) => e.url)).size;
    steps.push({
      n: 11,
      title: 'Relatório final e webhook com o resultado',
      actor: 'API → Cliente final',
      status: !inbound
        ? 'pending'
        : !completed.length
          ? 'warn'
          : completed.every((x) => x.status === 'delivered')
            ? 'done'
            : completed.some((x) => x.status === 'failed')
              ? 'error'
              : 'pending',
      at: iso(completed.find((x) => x.deliveredAt)?.deliveredAt ?? null),
      explain:
        'Resumo do pedido em passos simples: como a requisição começou, o que aconteceu no meio da transação e se algo chegou duplicado. Ao final da entrega, esse resultado vai para o cliente no webhook order.completed, gravado na mesma transação do passo 9.',
      headline: [
        {
          label: 'Pedido',
          value: o.status,
          tone: o.status === 'DELIVERED' ? 'ok' : 'info',
        },
        {
          label: 'Pagamento',
          value: report?.paymentStatus ?? 'PENDING',
          tone: report?.paymentStatus === 'PAID' ? 'ok' : 'info',
        },
        {
          label: 'Entrega',
          value: report?.deliveryStatus ?? 'PENDING',
          tone: report?.deliveryStatus === 'DELIVERED' ? 'ok' : 'info',
        },
        { label: 'Webhook final', ...finalWebhook },
      ],
      info: {
        'requisições duplicadas': d?.requests ?? 0,
        'eventos reentregues pelo broker': d?.brokerRedeliveries ?? 0,
        'webhooks repetidos da transportadora': d?.carrierWebhooks ?? 0,
        ...(report?.paidAt ? { 'pago em': report.paidAt } : {}),
        ...(completed.length ? { 'order.completed': describe(completed) } : {}),
        ...(sameUrl > 0
          ? {
              atenção: `${sameUrl} endpoint(s) com URL repetida: o cliente recebe cada aviso em dobro`,
            }
          : {}),
      },
      fix,
      report: report?.lines,
      json: completed.length
        ? [
            {
              label: 'Webhook order.completed enviado ao cliente',
              value: completed[0].payload,
            },
          ]
        : undefined,
    });

    // O primeiro passo ainda não concluído é o "em andamento".
    const firstOpen = steps.find((s) => s.status !== 'done');
    if (firstOpen && firstOpen.status === 'pending')
      firstOpen.status = 'active';

    const last = steps[steps.length - 1];
    const finished = last.status === 'done' ? last.at : null;
    return {
      orderId: o.id,
      status: o.status,
      customerEmail: o.customerEmail,
      amount: cents(o.amountCents),
      startedAt: o.createdAt.toISOString(),
      finishedAt: finished,
      steps,
    };
  }
}
