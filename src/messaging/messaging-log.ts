export type MessagingStepKind =
  | 'claimed' // relay reservou a linha da outbox
  | 'produce' // producer.send() chamado
  | 'stored' // broker confirmou: mensagem persistida
  | 'send-failed'
  | 'delivered' // broker entregou à assinatura
  | 'processed' // consumidor aplicou o efeito
  | 'duplicate' // consumidor descartou (eventId já processado)
  | 'ack'
  | 'nack';

export interface MessagingStep {
  at: string;
  kind: MessagingStepKind;
  title: string;
  /** Pares rótulo → valor mostrados no painel (messageId, ledger, bytes...). */
  info?: Record<string, string | number>;
}

export interface MessageTrail {
  eventId: string;
  topic: string;
  key?: string;
  firstAt: string;
  lastAt: string;
  /** Muda a cada passo novo: o painel só redesenha o que mudou. */
  version: number;
  steps: MessagingStep[];
}

/**
 * Caminho de cada mensagem pelo broker, do relay ao ack, agrupado por eventId.
 * Alimentado pelo relay, pelos brokers e pelo consumidor; lido pela aba
 * "Mensageria" do painel. Singleton de processo, como o LogBuffer.
 */
export class MessagingLog {
  private readonly trails = new Map<string, MessageTrail>();

  constructor(private readonly capacity = 60) {}

  record(
    eventId: string,
    topic: string,
    kind: MessagingStepKind,
    title: string,
    info?: MessagingStep['info'],
    key?: string,
  ) {
    if (!eventId) return;
    const at = new Date().toISOString();
    let trail = this.trails.get(eventId);
    if (!trail) {
      trail = {
        eventId,
        topic,
        key,
        firstAt: at,
        lastAt: at,
        version: 0,
        steps: [],
      };
      this.trails.set(eventId, trail);
      if (this.trails.size > this.capacity) {
        // Map preserva a ordem de inserção: o primeiro é o mais antigo.
        const oldest = this.trails.keys().next().value as string;
        this.trails.delete(oldest);
      }
    }
    if (key && !trail.key) trail.key = key;
    trail.steps.push({ at, kind, title, info });
    trail.lastAt = at;
    trail.version++;
  }

  /** Mais recentes primeiro. */
  list(): MessageTrail[] {
    return [...this.trails.values()].reverse();
  }

  clear() {
    this.trails.clear();
  }
}

export const messagingLog = new MessagingLog();
