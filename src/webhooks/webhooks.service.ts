import {
  ConflictException,
  Inject,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { and, asc, desc, eq, inArray, sql } from 'drizzle-orm';
import { createHash, randomUUID } from 'node:crypto';
import { DRIZZLE, type Database, type DbOrTx } from '../db/db.module';
import {
  webhookAttempts,
  webhookDeliveries,
  webhookEndpoints,
  type WebhookEndpoint,
} from '../db/schema';
import {
  ALL_WEBHOOK_EVENTS,
  WebhookEvents,
  type WebhookEventType,
} from '../events/topics';
import { buildOrderReport } from '../orders/order-report';
import type { CreateEndpointDto, UpdateEndpointDto } from './dto/webhook.dto';
import { generateSecret, maskSecret } from './webhook-signature';

/** Corpo enviado ao cliente: envelope igual para todos os eventos. */
export interface WebhookEnvelope<T = unknown> {
  /** Id do evento de origem: igual em todos os retries. */
  id: string;
  type: WebhookEventType;
  createdAt: string;
  data: T;
}

/**
 * Id fixo do resultado final de um pedido: o índice único (endpoint, evento)
 * garante que cada endpoint recebe o order.completed uma vez só, mesmo que o
 * reenvio seja pedido várias vezes.
 */
export function completedEventId(orderId: string): string {
  const h = createHash('sha1')
    .update(`${WebhookEvents.OrderCompleted}:${orderId}`)
    .digest('hex');
  const variant = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-5${h.slice(13, 16)}-${variant}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

const publicEndpoint = (e: WebhookEndpoint) => ({
  ...e,
  secret: maskSecret(e.secret),
});

@Injectable()
export class WebhooksService {
  private readonly logger = new Logger('Webhooks');

  constructor(@Inject(DRIZZLE) private readonly db: Database) {}

  /* ------------------------------------------------------------ endpoints */

  /** O segredo completo só é devolvido aqui: o cliente precisa guardá-lo. */
  async createEndpoint(dto: CreateEndpointDto) {
    // Duas linhas para a mesma URL = o cliente recebe cada aviso duas vezes.
    const [existing] = await this.db
      .select()
      .from(webhookEndpoints)
      .where(
        and(
          eq(webhookEndpoints.url, dto.url),
          eq(webhookEndpoints.active, true),
        ),
      );
    if (existing) {
      throw new ConflictException({
        message: `Já existe um endpoint ativo para ${dto.url}: atualize os eventos dele (PATCH) em vez de cadastrar outro`,
        details: { existingEndpointId: existing.id, events: existing.events },
      });
    }
    const [row] = await this.db
      .insert(webhookEndpoints)
      .values({
        url: dto.url,
        description: dto.description,
        events: dto.events,
        secret: generateSecret(),
      })
      .returning();
    this.logger.log(
      `Endpoint de webhook ${row.id.slice(0, 8)} cadastrado: ${row.url} (${row.events.join(', ')})`,
    );
    return row;
  }

  async listEndpoints() {
    const rows = await this.db
      .select()
      .from(webhookEndpoints)
      .orderBy(asc(webhookEndpoints.createdAt));
    return rows.map(publicEndpoint);
  }

  async updateEndpoint(id: string, dto: UpdateEndpointDto) {
    const [row] = await this.db
      .update(webhookEndpoints)
      .set(dto)
      .where(eq(webhookEndpoints.id, id))
      .returning();
    if (!row) throw new NotFoundException(`Endpoint ${id} não encontrado`);
    return publicEndpoint(row);
  }

  async deleteEndpoint(id: string) {
    const [row] = await this.db
      .delete(webhookEndpoints)
      .where(eq(webhookEndpoints.id, id))
      .returning({ id: webhookEndpoints.id });
    if (!row) throw new NotFoundException(`Endpoint ${id} não encontrado`);
    this.logger.warn(`Endpoint de webhook ${id.slice(0, 8)} removido`);
    return { deleted: id };
  }

  /** Usado pelo receptor de demonstração para conferir a assinatura. */
  async secretForDelivery(deliveryId: string): Promise<string | null> {
    const [row] = await this.db
      .select({ secret: webhookEndpoints.secret })
      .from(webhookDeliveries)
      .innerJoin(
        webhookEndpoints,
        eq(webhookEndpoints.id, webhookDeliveries.endpointId),
      )
      .where(eq(webhookDeliveries.id, deliveryId));
    return row?.secret ?? null;
  }

  /* -------------------------------------------------------------- entregas */

  /**
   * Cria uma entrega para cada endpoint ativo que assina o evento.
   * Deve rodar DENTRO da transação que gerou o evento: se ela fizer
   * rollback, nenhum webhook é enviado; se fizer commit, nenhum se perde.
   * O índice único (endpoint, evento) torna a chamada idempotente.
   */
  async enqueue<T>(
    tx: DbOrTx,
    type: WebhookEventType,
    eventId: string,
    data: T,
    onlyEndpointId?: string,
  ): Promise<string[]> {
    const envelope: WebhookEnvelope<T> = {
      id: eventId,
      type,
      createdAt: new Date().toISOString(),
      data,
    };
    const rows = await tx.execute<{ id: string }>(sql`
      INSERT INTO webhook_deliveries (endpoint_id, event_id, event_type, payload)
      SELECT id, ${eventId}::uuid, ${type}, ${JSON.stringify(envelope)}::jsonb
        FROM webhook_endpoints
       WHERE active
         AND (${type} = ANY(events) OR ${ALL_WEBHOOK_EVENTS} = ANY(events))
         ${onlyEndpointId ? sql`AND id = ${onlyEndpointId}::uuid` : sql``}
      ON CONFLICT (endpoint_id, event_id) DO NOTHING
      RETURNING id
    `);
    return [...rows].map((r) => r.id);
  }

  /**
   * Reenvia o resultado final de um pedido já entregue. Útil quando o
   * endpoint passou a assinar order.completed depois da entrega: só quem
   * ainda não recebeu ganha uma entrega nova (id fixo por pedido).
   */
  async resendCompleted(orderId: string) {
    const report = await buildOrderReport(this.db, orderId);
    if (!report)
      throw new NotFoundException(`Pedido ${orderId} não encontrado`);
    if (report.result !== 'DELIVERED') {
      throw new ConflictException(
        `Pedido ${orderId.slice(0, 8)} ainda não terminou (${report.result}): o resultado final sai sozinho na entrega`,
      );
    }
    const queued = await this.enqueue(
      this.db,
      WebhookEvents.OrderCompleted,
      completedEventId(orderId),
      report,
    );
    this.logger.log(
      `Resultado final do pedido ${orderId.slice(0, 8)} reenfileirado para ${queued.length} endpoint(s)`,
    );
    return { orderId, queued: queued.length, deliveryIds: queued };
  }

  /** Envia um evento de teste para um endpoint (botão "Enviar teste"). */
  async sendTest(endpointId: string) {
    const [endpoint] = await this.db
      .select()
      .from(webhookEndpoints)
      .where(eq(webhookEndpoints.id, endpointId));
    if (!endpoint) {
      throw new NotFoundException(`Endpoint ${endpointId} não encontrado`);
    }
    // O teste vai mesmo se o endpoint não assinar webhook.test ou estiver inativo.
    const [row] = await this.db
      .insert(webhookDeliveries)
      .values({
        endpointId,
        eventId: randomUUID(),
        eventType: WebhookEvents.Test,
        payload: {
          id: randomUUID(),
          type: WebhookEvents.Test,
          createdAt: new Date().toISOString(),
          data: {
            message: 'Webhook de teste: sua integração está recebendo avisos.',
          },
        } satisfies WebhookEnvelope,
      })
      .returning();
    return row;
  }

  private deliveries() {
    return this.db
      .select({
        id: webhookDeliveries.id,
        endpointId: webhookDeliveries.endpointId,
        url: webhookEndpoints.url,
        eventId: webhookDeliveries.eventId,
        eventType: webhookDeliveries.eventType,
        payload: webhookDeliveries.payload,
        status: webhookDeliveries.status,
        attempts: webhookDeliveries.attempts,
        nextAttemptAt: webhookDeliveries.nextAttemptAt,
        lastStatusCode: webhookDeliveries.lastStatusCode,
        lastError: webhookDeliveries.lastError,
        createdAt: webhookDeliveries.createdAt,
        deliveredAt: webhookDeliveries.deliveredAt,
      })
      .from(webhookDeliveries)
      .innerJoin(
        webhookEndpoints,
        eq(webhookEndpoints.id, webhookDeliveries.endpointId),
      )
      .$dynamic();
  }

  listDeliveries(limit = 50) {
    return this.deliveries()
      .orderBy(desc(webhookDeliveries.createdAt))
      .limit(limit);
  }

  async listAttempts(deliveryIds: string[]) {
    if (!deliveryIds.length) return [];
    return this.db
      .select()
      .from(webhookAttempts)
      .where(inArray(webhookAttempts.deliveryId, deliveryIds))
      .orderBy(asc(webhookAttempts.createdAt));
  }

  async getDelivery(id: string) {
    const [delivery] = await this.deliveries().where(
      eq(webhookDeliveries.id, id),
    );
    if (!delivery) throw new NotFoundException(`Entrega ${id} não encontrada`);
    return { ...delivery, attemptsLog: await this.listAttempts([id]) };
  }

  /** Devolve uma entrega (falha ou não) para a fila, já. */
  async retry(id: string) {
    const [row] = await this.db
      .update(webhookDeliveries)
      .set({
        status: 'pending',
        nextAttemptAt: new Date(),
        lockedUntil: null,
      })
      .where(eq(webhookDeliveries.id, id))
      .returning();
    if (!row) throw new NotFoundException(`Entrega ${id} não encontrada`);
    this.logger.warn(`Webhook ${id.slice(0, 8)} reenfileirado manualmente`);
    return row;
  }
}
