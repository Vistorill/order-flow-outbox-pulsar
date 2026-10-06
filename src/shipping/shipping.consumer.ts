import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
} from '@nestjs/common';
import { and, eq, sql } from 'drizzle-orm';
import { BrokerMessage, MessageBroker } from '../broker/message-broker';
import { DRIZZLE, type Database } from '../db/db.module';
import { orders, processedEvents, shipments } from '../db/schema';
import { Topics, type OrderConfirmedPayload } from '../events/topics';
import { messagingLog } from '../messaging/messaging-log';
import { traceStore } from '../tracing/trace-store';
import {
  CarrierClient,
  type CarrierShipment,
  type CreateShipmentRequest,
} from './carrier.client';

export const SHIPPING_CONSUMER = 'shipping';

/**
 * Serviço de envio: consome OrderConfirmed e chama a API da transportadora.
 *
 * Uma chamada HTTP externa não entra numa transação do Postgres, então a
 * segurança vem de três peças:
 *  - Idempotency-Key = eventId na chamada: retry não cria remessa duplicada;
 *  - falha (5xx, timeout) → erro → NACK → o broker reentrega e tentamos de novo;
 *  - sucesso → processed_events + remessa ACCEPTED + pedido SHIPPED numa transação.
 */
@Injectable()
export class ShippingConsumer implements OnApplicationBootstrap {
  private readonly logger = new Logger('ShippingConsumer');

  constructor(
    private readonly broker: MessageBroker,
    @Inject(DRIZZLE) private readonly db: Database,
    private readonly carrier: CarrierClient,
  ) {}

  async onApplicationBootstrap() {
    await this.broker.subscribe({
      topic: Topics.OrderConfirmed,
      subscription: SHIPPING_CONSUMER,
      handler: async (msg) => {
        await this.handle(msg);
      },
    });
  }

  async handle(msg: BrokerMessage): Promise<'shipped' | 'duplicate'> {
    const payload = msg.payload as OrderConfirmedPayload;
    const trace = traceStore.find(payload.orderId);
    const short = payload.orderId.slice(0, 8);

    const [existing] = await this.db
      .select()
      .from(shipments)
      .where(eq(shipments.orderId, payload.orderId));
    if (existing && existing.status !== 'REQUESTED') {
      await this.db
        .update(processedEvents)
        .set({ duplicates: sql`${processedEvents.duplicates} + 1` })
        .where(
          and(
            eq(processedEvents.eventId, msg.eventId),
            eq(processedEvents.consumer, SHIPPING_CONSUMER),
          ),
        );
      this.logger.warn(`Pedido ${short} já foi enviado: duplicata ignorada`);
      messagingLog.record(
        msg.eventId,
        msg.topic,
        'duplicate',
        'Serviço de envio descartou: remessa já criada',
        { pedido: payload.orderId, remessa: existing.externalId ?? '?' },
      );
      return 'duplicate';
    }

    const request: CreateShipmentRequest = {
      reference: payload.orderId,
      recipientEmail: payload.customerEmail,
      declaredValue: payload.amount,
      callbackUrl: this.carrier.callbackUrl,
    };
    const [row] = await this.db
      .insert(shipments)
      .values({
        orderId: payload.orderId,
        carrier: this.carrier.name,
        requestBody: request,
        attempts: 1,
      })
      .onConflictDoUpdate({
        target: shipments.orderId,
        set: { attempts: sql`${shipments.attempts} + 1`, requestBody: request },
      })
      .returning();

    traceStore.step(
      trace,
      'carrier',
      'info',
      `Chamando a API da transportadora (tentativa ${row.attempts})`,
      `POST ${this.carrier.baseUrl}/shipments · Idempotency-Key ${msg.eventId}`,
    );
    const res = await this.carrier.createShipment(request, msg.eventId);

    if (!res.ok) {
      await this.db
        .update(shipments)
        .set({
          lastStatusCode: res.statusCode,
          lastResponse: res.body,
          lastError: res.error,
        })
        .where(eq(shipments.id, row.id));
      traceStore.step(
        trace,
        'carrier',
        'error',
        `Transportadora falhou: ${res.error}`,
        'NACK: o broker reentrega o evento e a chamada é repetida',
      );
      // Lançar = NACK: o broker reentrega e esta chamada é repetida.
      throw new Error(`Transportadora: ${res.error}`);
    }

    const shipment = res.body as CarrierShipment;
    await this.db.transaction(async (tx) => {
      await tx
        .insert(processedEvents)
        .values({ eventId: msg.eventId, consumer: SHIPPING_CONSUMER })
        .onConflictDoNothing();
      await tx
        .update(shipments)
        .set({
          status: 'ACCEPTED',
          externalId: shipment.id,
          trackingCode: shipment.trackingCode,
          acceptedAt: new Date(),
          lastStatusCode: res.statusCode,
          lastResponse: res.body,
          lastError: null,
        })
        .where(eq(shipments.id, row.id));
      await tx
        .update(orders)
        .set({ status: 'SHIPPED', shippedAt: new Date() })
        .where(
          and(eq(orders.id, payload.orderId), eq(orders.status, 'CONFIRMED')),
        );
    });

    this.logger.log(
      `Pedido ${short} enviado à ${this.carrier.name}: remessa ${shipment.id}, rastreio ${shipment.trackingCode} (${res.statusCode}, ${res.durationMs}ms)`,
    );
    messagingLog.record(
      msg.eventId,
      msg.topic,
      'processed',
      'Serviço de envio criou a remessa na transportadora',
      {
        remessa: shipment.id,
        rastreio: shipment.trackingCode,
        http: res.statusCode ?? '?',
      },
    );
    traceStore.step(
      trace,
      'carrier',
      'ok',
      `Transportadora aceitou a remessa (${res.statusCode})`,
      `${shipment.id} · rastreio ${shipment.trackingCode} · pedido SHIPPED`,
    );
    return 'shipped';
  }
}
