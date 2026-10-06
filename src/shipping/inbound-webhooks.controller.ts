import {
  Body,
  Controller,
  HttpCode,
  Inject,
  Logger,
  NotFoundException,
  Post,
  Req,
  UnauthorizedException,
  type RawBodyRequest,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { and, eq, ne, sql } from 'drizzle-orm';
import type { Request } from 'express';
import { DRIZZLE, type Database } from '../db/db.module';
import { inboundWebhooks, orders, shipments } from '../db/schema';
import {
  WebhookEvents,
  type OrderDeliveredWebhookData,
} from '../events/topics';
import { buildOrderReport } from '../orders/order-report';
import { traceStore } from '../tracing/trace-store';
import { verify } from '../webhooks/webhook-signature';
import {
  WebhooksService,
  completedEventId,
} from '../webhooks/webhooks.service';

export const CARRIER_SOURCE = 'carrier';

/** O que a transportadora envia quando entrega. */
export interface CarrierDeliveredEvent {
  id: string;
  type: 'shipment.delivered';
  createdAt: string;
  data: {
    shipmentId: string;
    reference: string;
    trackingCode: string;
    deliveredAt: string;
    receivedBy?: string;
  };
}

/**
 * Webhooks que a transportadora manda para NÓS (fim do fluxo).
 *
 * 1. Confere a assinatura HMAC com o segredo combinado (sem ela, 401).
 * 2. Grava em inbound_webhooks: (origem, id do evento) é único, então um
 *    reenvio da transportadora é reconhecido e respondido 200 sem reprocessar.
 * 3. Na MESMA transação: remessa e pedido DELIVERED + webhook order.delivered
 *    para o cliente final.
 */
@Controller('webhooks/inbound')
export class InboundWebhooksController {
  private readonly logger = new Logger('InboundWebhook');
  private readonly secret: string;

  constructor(
    @Inject(DRIZZLE) private readonly db: Database,
    private readonly webhooks: WebhooksService,
    config: ConfigService,
  ) {
    this.secret = config.getOrThrow<string>('CARRIER_WEBHOOK_SECRET');
  }

  @Post('carrier')
  @HttpCode(200)
  async carrier(
    @Req() req: RawBodyRequest<Request>,
    @Body() body: CarrierDeliveredEvent,
  ) {
    const raw = req.rawBody?.toString('utf8') ?? JSON.stringify(body);
    const headers = {
      'webhook-id': req.header('webhook-id') ?? '',
      'webhook-timestamp': req.header('webhook-timestamp') ?? '',
      'webhook-signature': req.header('webhook-signature') ?? '',
    };
    const check = verify(
      this.secret,
      {
        id: headers['webhook-id'],
        timestamp: headers['webhook-timestamp'],
        signature: headers['webhook-signature'],
      },
      raw,
    );
    const trace = traceStore.find(body?.data?.reference);
    if (!check.ok) {
      this.logger.error(`Webhook da transportadora recusado: ${check.reason}`);
      traceStore.step(
        trace,
        'carrier',
        'error',
        'Webhook da transportadora recusado',
        check.reason,
      );
      throw new UnauthorizedException(check.reason);
    }

    const result = await this.db.transaction(async (tx) => {
      const [inbound] = await tx
        .insert(inboundWebhooks)
        .values({
          source: CARRIER_SOURCE,
          externalEventId: body.id,
          eventType: body.type,
          headers,
          payload: body,
        })
        .onConflictDoNothing()
        .returning();
      if (!inbound) {
        await tx
          .update(inboundWebhooks)
          .set({ duplicates: sql`${inboundWebhooks.duplicates} + 1` })
          .where(
            and(
              eq(inboundWebhooks.source, CARRIER_SOURCE),
              eq(inboundWebhooks.externalEventId, body.id),
            ),
          );
        return { duplicate: true as const };
      }
      if (body.type !== 'shipment.delivered') {
        return { ignored: body.type };
      }

      const deliveredAt = new Date(body.data.deliveredAt);
      const [shipment] = await tx
        .update(shipments)
        .set({ status: 'DELIVERED', deliveredAt })
        .where(
          and(
            eq(shipments.externalId, body.data.shipmentId),
            ne(shipments.status, 'DELIVERED'),
          ),
        )
        .returning();
      if (!shipment) {
        // Outro evento (id diferente) para uma remessa JÁ entregue: o estado
        // de negócio também é idempotente, então nada de novo para o cliente.
        const [known] = await tx
          .select({ id: shipments.id })
          .from(shipments)
          .where(eq(shipments.externalId, body.data.shipmentId));
        if (known) return { alreadyDelivered: true as const };
        throw new NotFoundException(
          `Remessa ${body.data.shipmentId} desconhecida`,
        );
      }
      await tx
        .update(orders)
        .set({ status: 'DELIVERED', deliveredAt })
        .where(
          and(eq(orders.id, shipment.orderId), ne(orders.status, 'DELIVERED')),
        );
      const [order] = await tx
        .select({ confirmedAt: orders.confirmedAt })
        .from(orders)
        .where(eq(orders.id, shipment.orderId));
      const paidAt = order?.confirmedAt ?? null;

      // Último aviso ao cliente final, na mesma transação da entrega.
      const queued = await this.webhooks.enqueue<OrderDeliveredWebhookData>(
        tx,
        WebhookEvents.OrderDelivered,
        inbound.id,
        {
          orderId: shipment.orderId,
          status: 'DELIVERED',
          paymentStatus: paidAt ? 'PAID' : 'PENDING',
          paidAt: paidAt ? paidAt.toISOString() : null,
          deliveryStatus: 'DELIVERED',
          carrier: shipment.carrier,
          trackingCode: shipment.trackingCode,
          deliveredAt: deliveredAt.toISOString(),
          receivedBy: body.data.receivedBy ?? null,
          message: `Pedido ${shipment.orderId.slice(0, 8)} entregue ao destinatário.`,
        },
      );

      // Por último, o resultado final: o relatório numerado do fluxo inteiro
      // (início, meio, duplicidades), lido nesta mesma transação.
      const report = await buildOrderReport(tx, shipment.orderId);
      const finals = report
        ? await this.webhooks.enqueue(
            tx,
            WebhookEvents.OrderCompleted,
            completedEventId(shipment.orderId),
            report,
          )
        : [];
      return {
        orderId: shipment.orderId,
        customerWebhooks: queued.length + finals.length,
      };
    });

    if ('duplicate' in result) {
      this.logger.warn(
        `Webhook ${body.id} da transportadora repetido: já processado, respondendo 200`,
      );
      traceStore.step(
        trace,
        'carrier',
        'warn',
        'Webhook da transportadora repetido',
        'Já estava em inbound_webhooks: respondido 200 sem reprocessar',
      );
      return { received: true, duplicate: true };
    }
    if ('ignored' in result) return { received: true, ignored: result.ignored };
    if ('alreadyDelivered' in result) {
      this.logger.warn(
        `Novo aviso de entrega (${body.id}) para a remessa ${body.data.shipmentId}, que já estava entregue: nada reenviado ao cliente`,
      );
      return { received: true, alreadyDelivered: true };
    }

    this.logger.log(
      `Transportadora confirmou a entrega do pedido ${result.orderId.slice(0, 8)} (assinatura ok): pedido DELIVERED`,
    );
    traceStore.step(
      trace,
      'carrier',
      'ok',
      'Webhook de entrega recebido e validado',
      `Assinatura ok · pedido DELIVERED · ${result.customerWebhooks} webhook(s) para o cliente na fila (order.delivered + order.completed)`,
    );
    return { received: true };
  }
}
