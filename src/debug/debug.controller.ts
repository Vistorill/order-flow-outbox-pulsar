import {
  Body,
  Controller,
  Get,
  HttpCode,
  Inject,
  Logger,
  NotFoundException,
  Param,
  ParseUUIDPipe,
  Post,
  Put,
  Query,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { desc, eq, sql } from 'drizzle-orm';
import { createZodDto } from 'nestjs-zod';
import { z } from 'zod';
import { ChaosService } from '../broker/chaos.service';
import { MessageBroker } from '../broker/message-broker';
import { toCents } from '../common/money';
import { DRIZZLE, type Database } from '../db/db.module';
import { orders, outbox, processedEvents } from '../db/schema';
import { EventTypes, Topics } from '../events/topics';
import { logBuffer } from '../logging/log-buffer';
import { messagingLog } from '../messaging/messaging-log';
import { traceStore } from '../tracing/trace-store';
import { toView } from '../orders/orders.service';
import { OutboxRelayService } from '../outbox/outbox-relay.service';
import { WebhookDispatcherService } from '../webhooks/webhook-dispatcher.service';
import { WebhooksService } from '../webhooks/webhooks.service';
import { CarrierSimulator } from './carrier-simulator.controller';
import { demoInbox } from './demo-receiver.controller';
import { JourneyService } from './journey.service';

class ChaosDto extends createZodDto(
  z.object({
    failPublish: z.boolean().optional(),
    failConsumer: z.boolean().optional(),
    crashAfterPublish: z.boolean().optional(),
    failWebhook: z.boolean().optional(),
    failCarrierApi: z.boolean().optional(),
  }),
) {}

const limitOf = (q?: string) => Math.min(Math.max(Number(q) || 50, 1), 500);

/**
 * Rotas de inspeção e de injeção de falhas usadas pelo painel (/).
 * Só são registradas com DEBUG_PANEL=true.
 */
@Controller('debug')
export class DebugController {
  private readonly logger = new Logger('Debug');

  constructor(
    @Inject(DRIZZLE) private readonly db: Database,
    private readonly broker: MessageBroker,
    private readonly chaos: ChaosService,
    private readonly relay: OutboxRelayService,
    private readonly config: ConfigService,
    private readonly webhooks: WebhooksService,
    private readonly dispatcher: WebhookDispatcherService,
    private readonly journeys: JourneyService,
    private readonly carrierSim: CarrierSimulator,
  ) {}

  @Get('overview')
  async overview() {
    const statusRows = await this.db
      .select({ status: outbox.status, count: sql<number>`count(*)::int` })
      .from(outbox)
      .groupBy(outbox.status);
    const outboxCounts = { pending: 0, processing: 0, published: 0, failed: 0 };
    for (const r of statusRows) outboxCounts[r.status] = r.count;

    const [oldest] = await this.db
      .select({
        seconds: sql<
          number | null
        >`extract(epoch from now() - min(${outbox.createdAt}))::float`,
      })
      .from(outbox)
      .where(sql`${outbox.status} in ('pending','processing')`);

    const orderRows = await this.db
      .select({ status: orders.status, count: sql<number>`count(*)::int` })
      .from(orders)
      .groupBy(orders.status);
    const orderCounts = { PENDING: 0, CONFIRMED: 0, SHIPPED: 0, DELIVERED: 0 };
    for (const r of orderRows) orderCounts[r.status] = r.count;

    const [processed] = await this.db
      .select({ count: sql<number>`count(*)::int` })
      .from(processedEvents);

    return {
      broker: this.broker.kind,
      brokerHealthy: await this.broker.healthy(),
      chaos: this.chaos.get(),
      outbox: outboxCounts,
      oldestPendingSeconds: oldest?.seconds ?? null,
      orders: orderCounts,
      processedEvents: processed.count,
      relay: {
        intervalMs: this.config.get<number>('OUTBOX_POLL_INTERVAL_MS'),
        batchSize: this.config.get<number>('OUTBOX_BATCH_SIZE'),
        maxAttempts: this.config.get<number>('OUTBOX_MAX_ATTEMPTS'),
        leaseSeconds: this.config.get<number>('OUTBOX_LEASE_SECONDS'),
      },
    };
  }

  @Get('outbox')
  listOutbox(@Query('limit') limit?: string) {
    return this.db
      .select()
      .from(outbox)
      .orderBy(desc(outbox.createdAt))
      .limit(limitOf(limit));
  }

  @Get('orders')
  async listOrders(@Query('limit') limit?: string) {
    const rows = await this.db
      .select()
      .from(orders)
      .orderBy(desc(orders.createdAt))
      .limit(limitOf(limit));
    return rows.map(toView);
  }

  @Get('processed-events')
  listProcessed(@Query('limit') limit?: string) {
    return this.db
      .select()
      .from(processedEvents)
      .orderBy(desc(processedEvents.processedAt))
      .limit(limitOf(limit));
  }

  /** No Pulsar, consulta o admin REST pelo backend (evita CORS no navegador). */
  @Get('topics')
  async topics() {
    try {
      return {
        ok: true,
        broker: this.broker.kind,
        topics: await this.broker.stats(),
      };
    } catch (err) {
      return {
        ok: false,
        broker: this.broker.kind,
        error: (err as Error).message,
        topics: [],
      };
    }
  }

  @Get('logs')
  logs(@Query('after') after?: string) {
    return logBuffer.since(Number(after) || 0);
  }

  /** Linha do tempo das últimas requisições POST /orders. */
  @Get('traces')
  traces() {
    return traceStore.list();
  }

  /** Caminho de cada mensagem pelo broker (aba "Mensageria"). */
  @Get('messaging')
  messaging() {
    return messagingLog.list();
  }

  /** Detalhes do Pulsar lidos do admin REST (aba "Pulsar"). */
  @Get('pulsar')
  async pulsar() {
    try {
      const details = await this.broker.inspect();
      return { ok: true, broker: this.broker.kind, details };
    } catch (err) {
      return {
        ok: false,
        broker: this.broker.kind,
        error: (err as Error).message,
      };
    }
  }

  /** Tudo da aba "Webhooks": endpoints, entregas com tentativas e o que o cliente recebeu. */
  @Get('webhooks')
  async webhooksOverview() {
    const [endpoints, deliveries] = await Promise.all([
      this.webhooks.listEndpoints(),
      this.webhooks.listDeliveries(30),
    ]);
    const attempts = await this.webhooks.listAttempts(
      deliveries.map((d) => d.id),
    );
    return {
      endpoints,
      deliveries: deliveries.map((d) => ({
        ...d,
        attemptsLog: attempts.filter((a) => a.deliveryId === d.id),
      })),
      received: demoInbox.list(),
      config: {
        maxAttempts: this.config.get<number>('WEBHOOK_MAX_ATTEMPTS'),
        timeoutMs: this.config.get<number>('WEBHOOK_TIMEOUT_MS'),
      },
    };
  }

  /** Fluxo ponta a ponta (11 passos) dos últimos pedidos, montado a partir do banco. */
  @Get('journeys')
  journeysList(@Query('limit') limit?: string) {
    return this.journeys.latest(Math.min(Math.max(Number(limit) || 8, 1), 30));
  }

  /** O que a transportadora simulada tem do lado dela. */
  @Get('carrier')
  carrier() {
    return this.carrierSim.list();
  }

  @Post('webhooks/run')
  @HttpCode(200)
  runWebhooks() {
    return this.dispatcher.tick();
  }

  @Get('chaos')
  getChaos() {
    return this.chaos.get();
  }

  @Put('chaos')
  setChaos(@Body() dto: ChaosDto) {
    return this.chaos.set(dto);
  }

  /** Força um ciclo do relay agora (sem esperar o intervalo). */
  @Post('relay/run')
  @HttpCode(200)
  runRelay() {
    return this.relay.tick();
  }

  /** Devolve um evento FAILED (dead letter) para a fila. */
  @Post('outbox/:id/retry')
  @HttpCode(200)
  async retry(@Param('id', new ParseUUIDPipe()) id: string) {
    const [row] = await this.db
      .update(outbox)
      .set({
        status: 'pending',
        attempts: 0,
        availableAt: new Date(),
        lockedUntil: null,
      })
      .where(eq(outbox.id, id))
      .returning();
    if (!row) throw new NotFoundException(`Evento ${id} não encontrado`);
    this.logger.warn(`Evento ${id.slice(0, 8)} reenfileirado manualmente`);
    return row;
  }

  /**
   * Republica um evento já publicado, com o MESMO eventId.
   * Simula entrega duplicada do broker: o consumidor deve ignorar.
   */
  @Post('outbox/:id/replay')
  @HttpCode(200)
  async replay(@Param('id', new ParseUUIDPipe()) id: string) {
    const [row] = await this.db.select().from(outbox).where(eq(outbox.id, id));
    if (!row) throw new NotFoundException(`Evento ${id} não encontrado`);
    await this.broker.publish(row.topic, row.payload, {
      eventId: row.id,
      key: row.aggregateId,
    });
    this.logger.warn(
      `Evento ${id.slice(0, 8)} republicado manualmente (duplicata simulada)`,
    );
    return { replayed: row.id };
  }

  /**
   * Prova de atomicidade: grava pedido + evento e falha ANTES do commit.
   * Nenhum dos dois pode existir depois.
   */
  @Post('atomicity-test')
  @HttpCode(200)
  async atomicityTest() {
    const count = async () => {
      const [o] = await this.db
        .select({ n: sql<number>`count(*)::int` })
        .from(orders);
      const [e] = await this.db
        .select({ n: sql<number>`count(*)::int` })
        .from(outbox);
      return { orders: o.n, outbox: e.n };
    };
    const before = await count();
    let error = '';
    try {
      await this.db.transaction(async (tx) => {
        const [order] = await tx
          .insert(orders)
          .values({
            customerEmail: 'rollback@teste.com',
            amountCents: toCents('1.00'),
          })
          .returning();
        await tx.insert(outbox).values({
          aggregateType: 'order',
          aggregateId: order.id,
          eventType: EventTypes.OrderCreated,
          topic: Topics.OrderCreated,
          payload: { orderId: order.id },
        });
        throw new Error(
          'Falha simulada depois dos dois INSERTs, antes do COMMIT',
        );
      });
    } catch (err) {
      error = (err as Error).message;
    }
    const after = await count();
    const atomic =
      before.orders === after.orders && before.outbox === after.outbox;
    this.logger.log(
      `Teste de atomicidade: ${atomic ? 'OK, rollback desfez pedido e evento' : 'FALHOU'}`,
    );
    return { atomic, error, before, after };
  }

  /** Limpa todas as tabelas e os logs (útil antes de uma demo). */
  @Post('reset')
  @HttpCode(200)
  async reset() {
    // Descarta mensagens em trânsito ANTES de limpar o banco: senão uma
    // reentrega agendada chega depois e processa um evento de pedido apagado.
    this.broker.reset();
    await this.db.execute(
      sql`TRUNCATE orders, outbox, processed_events, idempotency_keys, webhook_deliveries, webhook_attempts, shipments, inbound_webhooks`,
    );
    logBuffer.clear();
    demoInbox.clear();
    this.carrierSim.reset();
    traceStore.clear();
    messagingLog.clear();
    this.chaos.set({
      failPublish: false,
      failConsumer: false,
      crashAfterPublish: false,
      failWebhook: false,
      failCarrierApi: false,
    });
    this.logger.warn('Banco e logs limpos (endpoints de webhook mantidos)');
    return { ok: true };
  }
}
