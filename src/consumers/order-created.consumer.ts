import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
} from '@nestjs/common';
import { and, eq, sql } from 'drizzle-orm';
import { ChaosService } from '../broker/chaos.service';
import { BrokerMessage, MessageBroker } from '../broker/message-broker';
import { DRIZZLE, type Database } from '../db/db.module';
import { orders, processedEvents } from '../db/schema';
import {
  EventTypes,
  Topics,
  WebhookEvents,
  type OrderConfirmedPayload,
  type OrderConfirmedWebhookData,
  type OrderCreatedPayload,
} from '../events/topics';
import { OutboxService } from '../outbox/outbox.service';
import { messagingLog } from '../messaging/messaging-log';
import { traceStore } from '../tracing/trace-store';
import { WebhooksService } from '../webhooks/webhooks.service';

export const CONSUMER_NAME = 'order-confirmation';

/**
 * Consumidor idempotente (Inbox / Idempotent Consumer).
 *
 * Na MESMA transação:
 *   1. registra (event_id, consumer) em processed_events;
 *   2. aplica o efeito de negócio (confirma o pedido);
 *   3. grava o próximo evento (OrderConfirmed) na outbox e o webhook do cliente.
 * Se for duplicata, a inserção não retorna linha e o efeito é PULADO.
 * Se o efeito falhar, o registro de dedupe sofre rollback junto e o broker
 * reentrega (nack): nada é perdido e nada é aplicado duas vezes.
 */
@Injectable()
export class OrderCreatedConsumer implements OnApplicationBootstrap {
  private readonly logger = new Logger('OrderCreatedConsumer');

  constructor(
    private readonly broker: MessageBroker,
    private readonly chaos: ChaosService,
    @Inject(DRIZZLE) private readonly db: Database,
    private readonly webhooks: WebhooksService,
    private readonly outbox: OutboxService,
  ) {}

  async onApplicationBootstrap() {
    await this.broker.subscribe({
      topic: Topics.OrderCreated,
      subscription: CONSUMER_NAME,
      handler: async (msg) => {
        await this.handle(msg);
      },
    });
  }

  async handle(msg: BrokerMessage): Promise<'processed' | 'duplicate'> {
    const tag = `${msg.eventId.slice(0, 8)}${msg.redeliveryCount ? ` (reentrega #${msg.redeliveryCount})` : ''}`;
    if (!msg.eventId) {
      this.logger.error('Mensagem sem eventId: descartada');
      return 'duplicate';
    }

    const trace = traceStore.find(msg.eventId);
    traceStore.step(
      trace,
      'consumer',
      'info',
      msg.redeliveryCount
        ? `Consumidor recebeu a mensagem (reentrega #${msg.redeliveryCount})`
        : 'Consumidor recebeu a mensagem',
      `Assinatura ${CONSUMER_NAME}`,
    );

    try {
      return await this.db.transaction(async (tx) => {
        const inserted = await tx
          .insert(processedEvents)
          .values({ eventId: msg.eventId, consumer: CONSUMER_NAME })
          .onConflictDoNothing()
          .returning();

        if (inserted.length === 0) {
          // Conta a cópia descartada (entra no relatório final).
          await tx
            .update(processedEvents)
            .set({ duplicates: sql`${processedEvents.duplicates} + 1` })
            .where(
              and(
                eq(processedEvents.eventId, msg.eventId),
                eq(processedEvents.consumer, CONSUMER_NAME),
              ),
            );
          this.logger.warn(
            `Duplicata ignorada: evento ${tag} já foi processado`,
          );
          messagingLog.record(
            msg.eventId,
            msg.topic,
            'duplicate',
            'Consumidor descartou: eventId já processado',
            { consumidor: CONSUMER_NAME, tabela: 'processed_events' },
          );
          traceStore.step(
            trace,
            'consumer',
            'warn',
            'Duplicata ignorada',
            'eventId já está em processed_events: efeito não aplicado de novo',
          );
          return 'duplicate' as const;
        }

        this.chaos.beforeConsume();

        const payload = msg.payload as OrderCreatedPayload;
        const [confirmed] = await tx
          .update(orders)
          .set({ status: 'CONFIRMED', confirmedAt: new Date() })
          .where(
            and(eq(orders.id, payload.orderId), eq(orders.status, 'PENDING')),
          )
          .returning();

        // Webhook para o cliente final na MESMA transação da confirmação:
        // se ela fizer rollback, nenhum aviso sai; se fizer commit, nenhum se perde.
        if (confirmed) {
          // Próximo passo do fluxo: o serviço de envio consome OrderConfirmed e
          // chama a transportadora. Mesma transação = nenhum pedido confirmado
          // fica sem ser enviado.
          const nextEventId = await this.outbox.add(tx, {
            aggregateType: 'order',
            aggregateId: confirmed.id,
            eventType: EventTypes.OrderConfirmed,
            topic: Topics.OrderConfirmed,
            payload: { ...payload } satisfies OrderConfirmedPayload,
          });
          traceStore.link(trace, { eventId: nextEventId });
          const deliveries =
            await this.webhooks.enqueue<OrderConfirmedWebhookData>(
              tx,
              WebhookEvents.OrderConfirmed,
              msg.eventId,
              {
                orderId: confirmed.id,
                customerEmail: confirmed.customerEmail,
                amount: payload.amount,
                amountCents: payload.amountCents,
                status: 'CONFIRMED',
                paymentStatus: 'PAID',
                paidAt: confirmed.confirmedAt!.toISOString(),
                deliveryStatus: 'PENDING',
                confirmedAt: confirmed.confirmedAt!.toISOString(),
                message: `Pedido ${confirmed.id.slice(0, 8)} pago e confirmado com sucesso: transação concluída.`,
              },
            );
          traceStore.step(
            trace,
            'webhook',
            deliveries.length ? 'info' : 'warn',
            deliveries.length
              ? `${deliveries.length} webhook(s) order.confirmed na fila`
              : 'Nenhum endpoint de webhook ativo para order.confirmed',
            deliveries.length
              ? 'Gravado em webhook_deliveries na mesma transação da confirmação'
              : 'Cadastre um endpoint no painel para o cliente receber o aviso',
          );
        }

        this.logger.log(
          `Evento ${tag} processado: pedido ${payload.orderId.slice(0, 8)} CONFIRMED`,
        );
        messagingLog.record(
          msg.eventId,
          msg.topic,
          'processed',
          'Consumidor aplicou o efeito: pedido CONFIRMED',
          { consumidor: CONSUMER_NAME, pedido: payload.orderId },
        );
        traceStore.step(
          trace,
          'consumer',
          'ok',
          'Pedido CONFIRMED',
          'processed_events + UPDATE orders na mesma transação',
        );
        traceStore.setOutcome(trace, 'done');
        return 'processed' as const;
      });
    } catch (err) {
      this.logger.error(
        `Falha ao processar evento ${tag}: ${(err as Error).message}. Nack → reentrega`,
      );
      traceStore.step(
        trace,
        'consumer',
        'error',
        'Falha no consumidor: nack',
        `${(err as Error).message}. O broker vai reentregar`,
      );
      throw err;
    }
  }
}
