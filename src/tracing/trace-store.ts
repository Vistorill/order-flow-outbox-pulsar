import { AsyncLocalStorage } from 'node:async_hooks';

export type TraceStage =
  | 'api'
  | 'idempotency'
  | 'transaction'
  | 'relay'
  | 'consumer'
  | 'carrier'
  | 'webhook';
export type StepStatus = 'ok' | 'info' | 'warn' | 'error';
export type TraceOutcome =
  | 'running' // requisição ainda sem resposta
  | 'waiting' // 201 enviado, evento ainda não confirmado pelo consumidor
  | 'done' // pedido confirmado pelo consumidor
  | 'duplicate'
  | 'error';

export interface TraceStep {
  at: string;
  /** Milissegundos desde o início da requisição. */
  offsetMs: number;
  stage: TraceStage;
  status: StepStatus;
  title: string;
  detail?: string;
}

export interface Trace {
  id: number;
  /** Muda a cada passo novo: o painel só redesenha o que mudou. */
  version: number;
  startedAt: string;
  method: string;
  path: string;
  idempotencyKey?: string;
  request?: unknown;
  orderId?: string;
  eventId?: string;
  httpStatus?: number;
  responseMs?: number;
  response?: unknown;
  outcome: TraceOutcome;
  steps: TraceStep[];
}

/**
 * Linha do tempo de cada POST /orders, da requisição até o consumidor.
 * Dentro da requisição o trace atual vem do AsyncLocalStorage; depois do
 * commit, relay e consumidor o encontram pelo id do evento ou do pedido.
 * Singleton de processo, como o LogBuffer: é só para o painel.
 */
export class TraceStore {
  private readonly traces: Trace[] = [];
  private readonly byKey = new Map<string, Trace>();
  private readonly als = new AsyncLocalStorage<Trace>();
  private seq = 0;

  constructor(private readonly capacity = 40) {}

  start(method: string, path: string, idempotencyKey?: string): Trace {
    const trace: Trace = {
      id: ++this.seq,
      version: 0,
      startedAt: new Date().toISOString(),
      method,
      path,
      idempotencyKey,
      outcome: 'running',
      steps: [],
    };
    this.traces.push(trace);
    if (this.traces.length > this.capacity) {
      const old = this.traces.shift()!;
      if (old.orderId) this.byKey.delete(old.orderId);
      if (old.eventId) this.byKey.delete(old.eventId);
    }
    return trace;
  }

  run<T>(trace: Trace, fn: () => T): T {
    return this.als.run(trace, fn);
  }

  current(): Trace | undefined {
    return this.als.getStore();
  }

  /** Liga o trace ao pedido e ao evento para relay e consumidor acharem. */
  link(trace: Trace | undefined, ids: { orderId?: string; eventId?: string }) {
    if (!trace) return;
    if (ids.orderId) {
      trace.orderId = ids.orderId;
      this.byKey.set(ids.orderId, trace);
    }
    if (ids.eventId) {
      // O primeiro evento (OrderCreated) segue como o "evento" da requisição.
      trace.eventId ??= ids.eventId;
      this.byKey.set(ids.eventId, trace);
    }
  }

  find(id: string | undefined): Trace | undefined {
    return id ? this.byKey.get(id) : undefined;
  }

  step(
    trace: Trace | undefined,
    stage: TraceStage,
    status: StepStatus,
    title: string,
    detail?: string,
  ) {
    if (!trace) return;
    trace.steps.push({
      at: new Date().toISOString(),
      offsetMs: Date.now() - new Date(trace.startedAt).getTime(),
      stage,
      status,
      title,
      detail,
    });
    trace.version++;
  }

  setOutcome(trace: Trace | undefined, outcome: TraceOutcome) {
    if (!trace) return;
    trace.outcome = outcome;
    trace.version++;
  }

  /** Mais recentes primeiro. */
  list(): Trace[] {
    return [...this.traces].reverse();
  }

  clear() {
    this.traces.length = 0;
    this.byKey.clear();
  }
}

export const traceStore = new TraceStore();
