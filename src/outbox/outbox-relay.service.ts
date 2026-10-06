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
import { ChaosService } from '../broker/chaos.service';
import { MessageBroker } from '../broker/message-broker';
import { DRIZZLE, type Database } from '../db/db.module';
import { outbox } from '../db/schema';
import { messagingLog } from '../messaging/messaging-log';
import { traceStore } from '../tracing/trace-store';
import { backoffMs } from './backoff';

type ClaimedRow = {
  id: string;
  aggregate_id: string;
  event_type: string;
  topic: string;
  payload: unknown;
  attempts: number;
  created_at: string | Date;
};

export interface RelayRunResult {
  claimed: number;
  published: number;
  failed: number;
  dead: number;
}

/**
 * Message relay do Transactional Outbox, em três passos:
 *
 *  1. CLAIM   (transação curta) → reserva um lote com FOR UPDATE SKIP LOCKED,
 *             marca `processing`, incrementa `attempts` e define um lease
 *             (`locked_until`).
 *  2. PUBLISH (fora de transação) → envia ao broker.
 *  3. MARK    (transação curta) → `published`, ou volta a `pending` com
 *             backoff, ou `failed` (dead letter) após OUTBOX_MAX_ATTEMPTS.
 *
 * Se o processo morrer entre 2 e 3, o lease expira e outro worker reprocessa:
 * a entrega é at-least-once e o consumidor deduplica pelo eventId.
 */
@Injectable()
export class OutboxRelayService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger('OutboxRelay');
  private running = false;
  private stopping = false;

  private readonly batchSize: number;
  private readonly maxAttempts: number;
  private readonly leaseSeconds: number;
  private readonly intervalMs: number;

  constructor(
    @Inject(DRIZZLE) private readonly db: Database,
    private readonly broker: MessageBroker,
    private readonly chaos: ChaosService,
    private readonly scheduler: SchedulerRegistry,
    config: ConfigService,
  ) {
    this.batchSize = config.get<number>('OUTBOX_BATCH_SIZE', 50);
    this.maxAttempts = config.get<number>('OUTBOX_MAX_ATTEMPTS', 8);
    this.leaseSeconds = config.get<number>('OUTBOX_LEASE_SECONDS', 15);
    this.intervalMs = config.get<number>('OUTBOX_POLL_INTERVAL_MS', 1000);
  }

  /** O intervalo vem do ConfigService (no original era lido antes do .env carregar). */
  onApplicationBootstrap() {
    if (process.env.OUTBOX_RELAY_AUTOSTART === 'false') return;
    const handle = setInterval(() => void this.tick(), this.intervalMs);
    this.scheduler.addInterval('outbox-relay', handle);
    this.logger.log(
      `Relay ativo: a cada ${this.intervalMs}ms, lote ${this.batchSize}, máx. ${this.maxAttempts} tentativas`,
    );
  }

  async onApplicationShutdown() {
    this.stopping = true;
    if (this.scheduler.doesExist('interval', 'outbox-relay')) {
      this.scheduler.deleteInterval('outbox-relay');
    }
    // Espera o lote atual terminar para não deixar eventos presos em `processing`.
    const deadline = Date.now() + 5000;
    while (this.running && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 50));
    }
  }

  /** Executa ciclos até esvaziar a fila disponível. Público para testes e para o painel. */
  async tick(): Promise<RelayRunResult> {
    const total: RelayRunResult = {
      claimed: 0,
      published: 0,
      failed: 0,
      dead: 0,
    };
    if (this.running || this.stopping) return total;
    this.running = true;
    try {
      let batch: ClaimedRow[];
      do {
        batch = await this.claim();
        total.claimed += batch.length;
        // Sequencial dentro do lote: preserva a ordem de criação.
        for (const row of batch) {
          const r = await this.publishOne(row);
          total[r]++;
        }
      } while (batch.length === this.batchSize && !this.stopping);
    } catch (err) {
      this.logger.error(
        `Falha no ciclo do relay: ${(err as Error).message}`,
        (err as Error).stack,
      );
    } finally {
      this.running = false;
    }
    return total;
  }

  private async claim(): Promise<ClaimedRow[]> {
    const rows = await this.db.execute<ClaimedRow>(sql`
      UPDATE outbox
         SET status = 'processing',
             attempts = attempts + 1,
             locked_until = now() + make_interval(secs => ${this.leaseSeconds})
       WHERE id IN (
         SELECT id FROM outbox
          WHERE (status = 'pending' AND available_at <= now())
             OR (status = 'processing' AND locked_until < now())
          ORDER BY created_at
          LIMIT ${this.batchSize}
          FOR UPDATE SKIP LOCKED
       )
      RETURNING id, aggregate_id, event_type, topic, payload, attempts, created_at
    `);
    // RETURNING não garante ordem: reordena pela criação.
    return [...rows].sort(
      (a, b) =>
        new Date(a.created_at).getTime() - new Date(b.created_at).getTime(),
    );
  }

  private async publishOne(
    row: ClaimedRow,
  ): Promise<'published' | 'failed' | 'dead'> {
    messagingLog.record(
      row.id,
      row.topic,
      'claimed',
      `Relay leu a outbox e reservou o evento (tentativa ${row.attempts})`,
      {
        evento: row.event_type,
        pedido: row.aggregate_id,
        'status na outbox': 'processing',
        lease: `${this.leaseSeconds}s`,
      },
      row.aggregate_id,
    );
    const trace = traceStore.find(row.id);
    traceStore.step(
      trace,
      'relay',
      'info',
      `Relay reservou o evento (tentativa ${row.attempts})`,
      `FOR UPDATE SKIP LOCKED → status processing, lease de ${this.leaseSeconds}s`,
    );
    try {
      this.chaos.beforePublish();
      await this.broker.publish(row.topic, row.payload, {
        eventId: row.id,
        key: row.aggregate_id,
      });
      this.logger.log(
        `Publicado ${row.event_type} → ${row.topic} (evento ${short(row.id)}, tentativa ${row.attempts})`,
      );
      traceStore.step(
        trace,
        'relay',
        'ok',
        `Publicado no broker em ${row.topic}`,
        `${this.broker.kind}, chave ${row.aggregate_id}`,
      );
    } catch (err) {
      return this.markFailure(row, err);
    }

    if (this.chaos.shouldCrashAfterPublish()) {
      // Simula o processo morrendo entre PUBLISH e MARK: a linha fica em
      // `processing` até o lease expirar, e então é publicada de novo.
      // O consumidor recebe duplicata e a descarta pelo eventId.
      this.logger.warn(
        `Queda simulada após publicar ${short(row.id)}: será reenviado quando o lease de ${this.leaseSeconds}s expirar`,
      );
      traceStore.step(
        trace,
        'relay',
        'warn',
        'Queda simulada antes de marcar como publicado',
        `O evento continua processing e sai de novo quando o lease de ${this.leaseSeconds}s expirar`,
      );
      return 'failed';
    }

    await this.db
      .update(outbox)
      .set({
        status: 'published',
        publishedAt: new Date(),
        lockedUntil: null,
        lastError: null,
      })
      .where(eq(outbox.id, row.id));
    traceStore.step(trace, 'relay', 'ok', 'Outbox marcada como published');
    return 'published';
  }

  private async markFailure(row: ClaimedRow, err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    const dead = row.attempts >= this.maxAttempts;
    const delay = backoffMs(row.attempts);
    await this.db
      .update(outbox)
      .set({
        status: dead ? 'failed' : 'pending',
        availableAt: new Date(Date.now() + delay),
        lockedUntil: null,
        lastError: message,
      })
      .where(eq(outbox.id, row.id));

    const trace = traceStore.find(row.id);
    if (dead) {
      traceStore.step(
        trace,
        'relay',
        'error',
        `Evento movido para FAILED após ${row.attempts} tentativas`,
        message,
      );
      traceStore.setOutcome(trace, 'error');
      this.logger.error(
        `Evento ${short(row.id)} movido para FAILED após ${row.attempts} tentativas: ${message}`,
      );
      return 'dead' as const;
    }
    traceStore.step(
      trace,
      'relay',
      'warn',
      `Falha ao publicar (tentativa ${row.attempts}/${this.maxAttempts})`,
      `${message}. Nova tentativa em ${(delay / 1000).toFixed(1)}s`,
    );
    this.logger.warn(
      `Falha ao publicar ${short(row.id)} (tentativa ${row.attempts}/${this.maxAttempts}): ${message}. Nova tentativa em ${(delay / 1000).toFixed(1)}s`,
    );
    return 'failed' as const;
  }
}

const short = (id: string) => id.slice(0, 8);
