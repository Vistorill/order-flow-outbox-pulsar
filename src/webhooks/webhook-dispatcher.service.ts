import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { SchedulerRegistry } from '@nestjs/schedule';
import { eq, sql } from 'drizzle-orm';
import { DRIZZLE, type Database } from '../db/db.module';
import { webhookAttempts, webhookDeliveries } from '../db/schema';
import { backoffMs } from '../outbox/backoff';
import { traceStore } from '../tracing/trace-store';
import { sign } from './webhook-signature';
import type { WebhookEnvelope } from './webhooks.service';

type ClaimedDelivery = {
  id: string;
  event_type: string;
  payload: WebhookEnvelope;
  attempts: number;
  url: string;
  secret: string;
};

export interface DispatchRunResult {
  claimed: number;
  delivered: number;
  failed: number;
  dead: number;
}

const USER_AGENT = 'nestjs-outbox-webhooks/1.0';
const MAX_RESPONSE_CHARS = 2000;

/**
 * Envia os webhooks da tabela webhook_deliveries, no mesmo esquema do relay:
 *
 *  1. CLAIM   → reserva um lote com FOR UPDATE SKIP LOCKED (vários workers
 *               podem rodar juntos sem enviar o mesmo aviso duas vezes).
 *  2. POST    → corpo JSON assinado com HMAC (headers Standard Webhooks).
 *  3. MARK    → 2xx = delivered; senão volta a pending com backoff, ou
 *               failed depois de WEBHOOK_MAX_ATTEMPTS. Toda tentativa fica
 *               registrada em webhook_attempts.
 *
 * Entrega at-least-once: o cliente deve deduplicar pelo header webhook-id.
 */
@Injectable()
export class WebhookDispatcherService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger('WebhookDispatcher');
  private running = false;
  private stopping = false;

  private readonly intervalMs: number;
  private readonly batchSize: number;
  private readonly maxAttempts: number;
  private readonly timeoutMs: number;

  constructor(
    @Inject(DRIZZLE) private readonly db: Database,
    private readonly scheduler: SchedulerRegistry,
    config: ConfigService,
  ) {
    this.intervalMs = config.get<number>('WEBHOOK_POLL_INTERVAL_MS', 1000);
    this.batchSize = config.get<number>('WEBHOOK_BATCH_SIZE', 20);
    this.maxAttempts = config.get<number>('WEBHOOK_MAX_ATTEMPTS', 6);
    this.timeoutMs = config.get<number>('WEBHOOK_TIMEOUT_MS', 5000);
  }

  onApplicationBootstrap() {
    if (process.env.WEBHOOK_DISPATCHER_AUTOSTART === 'false') return;
    const handle = setInterval(() => void this.tick(), this.intervalMs);
    this.scheduler.addInterval('webhook-dispatcher', handle);
    this.logger.log(
      `Webhooks ativos: a cada ${this.intervalMs}ms, timeout ${this.timeoutMs}ms, máx. ${this.maxAttempts} tentativas`,
    );
  }

  async onApplicationShutdown() {
    this.stopping = true;
    if (this.scheduler.doesExist('interval', 'webhook-dispatcher')) {
      this.scheduler.deleteInterval('webhook-dispatcher');
    }
    const deadline = Date.now() + this.timeoutMs + 1000;
    while (this.running && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 50));
    }
  }

  /** Executa ciclos até esvaziar a fila disponível. Público para testes e para o painel. */
  async tick(): Promise<DispatchRunResult> {
    const total: DispatchRunResult = {
      claimed: 0,
      delivered: 0,
      failed: 0,
      dead: 0,
    };
    if (this.running || this.stopping) return total;
    this.running = true;
    try {
      let batch: ClaimedDelivery[];
      do {
        batch = await this.claim();
        total.claimed += batch.length;
        // Em paralelo: um cliente lento não segura os avisos dos outros.
        const results = await Promise.all(batch.map((d) => this.deliver(d)));
        for (const r of results) total[r]++;
      } while (batch.length === this.batchSize && !this.stopping);
    } catch (err) {
      this.logger.error(
        `Falha no ciclo de webhooks: ${(err as Error).message}`,
        (err as Error).stack,
      );
    } finally {
      this.running = false;
    }
    return total;
  }

  private async claim(): Promise<ClaimedDelivery[]> {
    // Lease maior que o timeout: só expira se o processo morrer no meio do envio.
    const leaseSeconds = Math.ceil(this.timeoutMs / 1000) + 30;
    const rows = await this.db.execute<ClaimedDelivery>(sql`
      UPDATE webhook_deliveries d
         SET status = 'delivering',
             attempts = d.attempts + 1,
             locked_until = now() + make_interval(secs => ${leaseSeconds})
        FROM webhook_endpoints e
       WHERE e.id = d.endpoint_id
         AND d.id IN (
           SELECT id FROM webhook_deliveries
            WHERE (status = 'pending' AND next_attempt_at <= now())
               OR (status = 'delivering' AND locked_until < now())
            ORDER BY created_at
            LIMIT ${this.batchSize}
            FOR UPDATE SKIP LOCKED
         )
      RETURNING d.id, d.event_type, d.payload, d.attempts, e.url, e.secret
    `);
    return [...rows];
  }

  private async deliver(
    d: ClaimedDelivery,
  ): Promise<'delivered' | 'failed' | 'dead'> {
    const body = JSON.stringify(d.payload);
    const timestamp = Math.floor(Date.now() / 1000);
    const headers: Record<string, string> = {
      'content-type': 'application/json',
      'user-agent': USER_AGENT,
      'webhook-id': d.id,
      'webhook-timestamp': String(timestamp),
      'webhook-signature': sign(d.secret, d.id, timestamp, body),
      'webhook-event': d.event_type,
      'webhook-attempt': String(d.attempts),
    };
    const trace = traceStore.find(orderIdOf(d.payload));
    traceStore.step(
      trace,
      'webhook',
      'info',
      `Enviando webhook ${d.event_type} (tentativa ${d.attempts})`,
      `POST ${d.url}`,
    );

    const started = Date.now();
    let statusCode: number | null = null;
    let responseBody: string | null = null;
    let error: string | null = null;
    try {
      const res = await fetch(d.url, {
        method: 'POST',
        headers,
        body,
        redirect: 'manual', // redirecionar um aviso assinado para outro host é arriscado
        signal: AbortSignal.timeout(this.timeoutMs),
      });
      statusCode = res.status;
      responseBody = (await res.text()).slice(0, MAX_RESPONSE_CHARS);
      if (!res.ok) error = `HTTP ${res.status}`;
    } catch (err) {
      const e = err as Error;
      error =
        e.name === 'TimeoutError'
          ? `Sem resposta em ${this.timeoutMs}ms (timeout)`
          : `${e.message}${e.cause instanceof Error ? `: ${e.cause.message}` : ''}`;
    }
    const durationMs = Date.now() - started;

    await this.db.insert(webhookAttempts).values({
      deliveryId: d.id,
      attempt: d.attempts,
      requestHeaders: headers,
      statusCode,
      responseBody,
      error,
      durationMs,
    });

    if (!error) {
      await this.db
        .update(webhookDeliveries)
        .set({
          status: 'delivered',
          deliveredAt: new Date(),
          lockedUntil: null,
          lastStatusCode: statusCode,
          lastError: null,
        })
        .where(eq(webhookDeliveries.id, d.id));
      this.logger.log(
        `Webhook ${d.event_type} entregue a ${d.url} (${statusCode}, ${durationMs}ms, tentativa ${d.attempts})`,
      );
      traceStore.step(
        trace,
        'webhook',
        'ok',
        `Cliente confirmou o webhook (${statusCode})`,
        `${durationMs}ms · webhook-id ${d.id}`,
      );
      return 'delivered';
    }

    const dead = d.attempts >= this.maxAttempts;
    const delay = backoffMs(d.attempts);
    await this.db
      .update(webhookDeliveries)
      .set({
        status: dead ? 'failed' : 'pending',
        nextAttemptAt: new Date(Date.now() + delay),
        lockedUntil: null,
        lastStatusCode: statusCode,
        lastError: error,
      })
      .where(eq(webhookDeliveries.id, d.id));

    if (dead) {
      this.logger.error(
        `Webhook ${d.id.slice(0, 8)} para ${d.url} FALHOU após ${d.attempts} tentativas: ${error}`,
      );
      traceStore.step(
        trace,
        'webhook',
        'error',
        `Webhook desistiu após ${d.attempts} tentativas`,
        error,
      );
      return 'dead';
    }
    this.logger.warn(
      `Webhook ${d.id.slice(0, 8)} para ${d.url} falhou (tentativa ${d.attempts}/${this.maxAttempts}): ${error}. Nova tentativa em ${(delay / 1000).toFixed(1)}s`,
    );
    traceStore.step(
      trace,
      'webhook',
      'warn',
      `Webhook falhou (tentativa ${d.attempts}/${this.maxAttempts})`,
      `${error}. Nova tentativa em ${(delay / 1000).toFixed(1)}s`,
    );
    return 'failed';
  }
}

function orderIdOf(payload: WebhookEnvelope): string | undefined {
  const data = payload?.data as { orderId?: string } | undefined;
  return data?.orderId;
}
