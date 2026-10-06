import { Logger, OnApplicationShutdown } from '@nestjs/common';
import {
  BrokerMessage,
  MessageBroker,
  PublishOptions,
  SubscribeOptions,
  TopicStats,
} from './message-broker';
import { messagingLog } from '../messaging/messaging-log';

interface Subscription {
  name: string;
  handler: SubscribeOptions['handler'];
  queue: BrokerMessage[];
  busy: boolean;
  acked: number;
  redeliveries: number;
}

interface TopicState {
  published: number;
  subs: Map<string, Subscription>;
}

/**
 * Broker em memória com semântica parecida com uma assinatura do Pulsar:
 * - cada assinatura recebe cópia de cada mensagem;
 * - entrega sequencial (preserva ordem);
 * - erro no handler = nack → reentrega após `redeliveryDelayMs`.
 * Mensagens publicadas antes de existir assinatura ficam retidas (como um
 * tópico persistente com assinatura criada depois em "Earliest").
 */
export class InMemoryBroker
  extends MessageBroker
  implements OnApplicationShutdown
{
  readonly kind = 'memory' as const;
  private readonly logger = new Logger('InMemoryBroker');
  private readonly topics = new Map<string, TopicState>();
  private readonly retained = new Map<string, BrokerMessage[]>();
  private readonly timers = new Set<NodeJS.Timeout>();
  private closed = false;

  constructor(private readonly redeliveryDelayMs = 1000) {
    super();
  }

  private topic(name: string): TopicState {
    let t = this.topics.get(name);
    if (!t) {
      t = { published: 0, subs: new Map() };
      this.topics.set(name, t);
    }
    return t;
  }

  publish(topic: string, payload: unknown, opts: PublishOptions) {
    const t = this.topic(topic);
    t.published++;
    // Serializa e desserializa para simular o fio (e pegar payload não-serializável).
    const msg: BrokerMessage = {
      topic,
      eventId: opts.eventId,
      key: opts.key,
      payload: JSON.parse(JSON.stringify(payload)),
      redeliveryCount: 0,
    };
    messagingLog.record(
      opts.eventId,
      topic,
      'stored',
      'Broker em memória recebeu a mensagem',
      { assinaturas: t.subs.size, bytes: JSON.stringify(payload).length },
      opts.key,
    );
    if (t.subs.size === 0) {
      const list = this.retained.get(topic) ?? [];
      list.push(msg);
      this.retained.set(topic, list);
    }
    for (const sub of t.subs.values()) this.enqueue(sub, { ...msg });
    return Promise.resolve();
  }

  subscribe({ topic, subscription, handler }: SubscribeOptions) {
    const t = this.topic(topic);
    const sub: Subscription = {
      name: subscription,
      handler,
      queue: [],
      busy: false,
      acked: 0,
      redeliveries: 0,
    };
    t.subs.set(subscription, sub);
    for (const msg of this.retained.get(topic) ?? [])
      this.enqueue(sub, { ...msg });
    this.retained.delete(topic);
    return Promise.resolve();
  }

  private enqueue(sub: Subscription, msg: BrokerMessage) {
    sub.queue.push(msg);
    void this.drain(sub);
  }

  private async drain(sub: Subscription) {
    if (sub.busy || this.closed) return;
    sub.busy = true;
    try {
      while (sub.queue.length && !this.closed) {
        const msg = sub.queue.shift()!;
        messagingLog.record(
          msg.eventId,
          msg.topic,
          'delivered',
          msg.redeliveryCount
            ? `Entregue à assinatura (reentrega #${msg.redeliveryCount})`
            : 'Entregue à assinatura',
          { assinatura: sub.name },
        );
        try {
          await sub.handler(msg);
          sub.acked++;
          messagingLog.record(msg.eventId, msg.topic, 'ack', 'ACK');
        } catch (err) {
          messagingLog.record(
            msg.eventId,
            msg.topic,
            'nack',
            `NACK: reentrega em ${this.redeliveryDelayMs}ms`,
            { erro: (err as Error)?.message ?? String(err) },
          );
          sub.redeliveries++;
          const timer = setTimeout(() => {
            this.timers.delete(timer);
            this.enqueue(sub, {
              ...msg,
              redeliveryCount: msg.redeliveryCount + 1,
            });
          }, this.redeliveryDelayMs);
          this.timers.add(timer);
        }
      }
    } finally {
      sub.busy = false;
    }
  }

  stats(): Promise<TopicStats[]> {
    return Promise.resolve(
      [...this.topics.entries()].map(([topic, t]) => ({
        topic,
        published: t.published,
        subscriptions: [...t.subs.values()].map((s) => ({
          name: s.name,
          backlog: s.queue.length + (s.busy ? 1 : 0),
          acked: s.acked,
          redeliveries: s.redeliveries,
        })),
      })),
    );
  }

  healthy() {
    return Promise.resolve(!this.closed);
  }

  /** Zera contadores e descarta mensagens em fila e reentregas agendadas. */
  reset() {
    for (const t of this.timers) clearTimeout(t);
    this.timers.clear();
    this.retained.clear();
    for (const t of this.topics.values()) {
      t.published = 0;
      for (const s of t.subs.values()) {
        s.queue.length = 0;
        s.acked = 0;
        s.redeliveries = 0;
      }
    }
  }

  /** Usado pelos testes para esperar o consumo terminar. */
  async idle(timeoutMs = 5000) {
    const start = Date.now();
    const busy = () =>
      this.timers.size > 0 ||
      [...this.topics.values()].some((t) =>
        [...t.subs.values()].some((s) => s.busy || s.queue.length > 0),
      );
    while (busy()) {
      if (Date.now() - start > timeoutMs)
        throw new Error('broker não ficou ocioso');
      await new Promise((r) => setTimeout(r, 20));
    }
  }

  onApplicationShutdown() {
    this.closed = true;
    for (const t of this.timers) clearTimeout(t);
    this.timers.clear();
    this.logger.log('Broker em memória encerrado');
  }
}
