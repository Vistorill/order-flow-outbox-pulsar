import { Logger, OnApplicationShutdown } from '@nestjs/common';
import type * as PulsarTypes from 'pulsar-client';
import {
  MessageBroker,
  PublishOptions,
  SubscribeOptions,
  TopicStats,
} from './message-broker';
import { messagingLog } from '../messaging/messaging-log';

const NAMESPACE = 'public/default';
const MAX_REDELIVER = 10;

interface PulsarTopicStats {
  msgInCounter?: number;
  msgOutCounter?: number;
  bytesInCounter?: number;
  msgRateIn?: number;
  msgRateOut?: number;
  storageSize?: number;
  publishers?: {
    producerName?: string;
    address?: string;
    connectedSince?: string;
    msgRateIn?: number;
    clientVersion?: string;
  }[];
  subscriptions?: Record<
    string,
    {
      type?: string;
      msgBacklog?: number;
      msgOutCounter?: number;
      unackedMessages?: number;
      msgRateOut?: number;
      lastAckedTimestamp?: number;
      consumers?: {
        consumerName?: string;
        address?: string;
        availablePermits?: number;
        unackedMessages?: number;
        msgOutCounter?: number;
        connectedSince?: string;
      }[];
    }
  >;
}

interface PulsarInternalStats {
  entriesAddedCounter?: number;
  numberOfEntries?: number;
  totalSize?: number;
  currentLedgerEntries?: number;
  lastConfirmedEntry?: string;
  state?: string;
  ledgers?: { ledgerId: number; entries: number; size: number }[];
  cursors?: Record<
    string,
    {
      markDeletePosition?: string;
      readPosition?: string;
      messagesConsumedCounter?: number;
      active?: boolean;
    }
  >;
}

/** "(17,25,-1,-1)" → ledger 17, entry 25: onde o BookKeeper gravou a mensagem. */
function describeMessageId(id: string) {
  const [ledger, entry] = id.replace(/[()]/g, '').split(/[,:]/);
  return { messageId: id, ledger: ledger ?? '?', entry: entry ?? '?' };
}

export class PulsarBroker
  extends MessageBroker
  implements OnApplicationShutdown
{
  readonly kind = 'pulsar' as const;
  private readonly logger = new Logger('PulsarBroker');
  private readonly producers = new Map<string, Promise<PulsarTypes.Producer>>();
  private readonly consumers: PulsarTypes.Consumer[] = [];
  private readonly redeliveries = new Map<string, number>();
  private readonly client: PulsarTypes.Client;

  constructor(
    private readonly serviceUrl: string,
    private readonly adminUrl: string,
  ) {
    super();
    // Import tardio: com BROKER=memory o binário nativo nem é carregado.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const Pulsar = require('pulsar-client') as typeof PulsarTypes;
    this.client = new Pulsar.Client({
      serviceUrl,
      operationTimeoutSeconds: 10,
    });
  }

  /**
   * Cache de producer por tópico. Se a criação falhar (broker fora do ar),
   * a Promise rejeitada é removida do cache: no original ela ficava presa
   * e o tópico nunca mais publicava até reiniciar a aplicação.
   */
  private producer(topic: string) {
    let p = this.producers.get(topic);
    if (!p) {
      p = this.client
        .createProducer({ topic, sendTimeoutMs: 5000, batchingEnabled: false })
        .catch((err: unknown) => {
          this.producers.delete(topic);
          throw err;
        });
      this.producers.set(topic, p);
    }
    return p;
  }

  async publish(topic: string, payload: unknown, opts: PublishOptions) {
    const data = Buffer.from(JSON.stringify(payload));
    const started = Date.now();
    try {
      const producer = await this.producer(topic);
      messagingLog.record(
        opts.eventId,
        topic,
        'produce',
        'Producer enviou a mensagem ao broker',
        {
          producer: producer.getProducerName(),
          tópico: `persistent://${NAMESPACE}/${topic}`,
          partitionKey: opts.key,
          'propriedade eventId': opts.eventId,
          bytes: data.length,
        },
        opts.key,
      );
      const id = await producer.send({
        data,
        properties: { eventId: opts.eventId },
        partitionKey: opts.key,
        orderingKey: opts.key,
      });
      messagingLog.record(
        opts.eventId,
        topic,
        'stored',
        'Broker gravou no BookKeeper e confirmou (send receipt)',
        {
          ...describeMessageId(id.toString()),
          latência: `${Date.now() - started}ms`,
        },
      );
    } catch (err) {
      messagingLog.record(
        opts.eventId,
        topic,
        'send-failed',
        'Envio falhou: o broker não confirmou',
        { erro: (err as Error).message, após: `${Date.now() - started}ms` },
        opts.key,
      );
      throw err;
    }
  }

  async subscribe({ topic, subscription, handler }: SubscribeOptions) {
    const consumer = await this.client.subscribe({
      topic,
      subscription,
      // Key_Shared: paralelismo entre chaves, ordem dentro da mesma chave.
      subscriptionType: 'KeyShared',
      subscriptionInitialPosition: 'Earliest',
      nAckRedeliverTimeoutMs: 1000,
      deadLetterPolicy: {
        deadLetterTopic: `${topic}.${subscription}.DLQ`,
        maxRedeliverCount: MAX_REDELIVER,
      },
      listener: (message, c) => {
        const eventId = message.getProperties().eventId ?? '';
        const msgId = message.getMessageId().toString();
        const redeliveryCount = message.getRedeliveryCount();
        messagingLog.record(
          eventId,
          topic,
          'delivered',
          redeliveryCount
            ? `Broker reentregou à assinatura (reentrega #${redeliveryCount})`
            : 'Broker entregou à assinatura',
          {
            assinatura: `${subscription} (Key_Shared)`,
            ...describeMessageId(msgId),
            'publicada há': `${Date.now() - message.getPublishTimestamp()}ms`,
            'producer de origem': message.getProducerName(),
          },
        );
        void handler({
          topic,
          eventId,
          key: message.getPartitionKey(),
          payload: JSON.parse(message.getData().toString()) as unknown,
          redeliveryCount,
        })
          .then(async () => {
            await c.acknowledge(message);
            messagingLog.record(
              eventId,
              topic,
              'ack',
              'ACK: cursor da assinatura avança e a mensagem sai do backlog',
              { messageId: msgId },
            );
          })
          .catch((err: unknown) => {
            this.redeliveries.set(
              subscription,
              (this.redeliveries.get(subscription) ?? 0) + 1,
            );
            c.negativeAcknowledge(message);
            messagingLog.record(
              eventId,
              topic,
              'nack',
              'NACK: o broker reentrega em ~1s',
              {
                erro: (err as Error)?.message ?? String(err),
                tentativa: `${redeliveryCount + 1} de ${MAX_REDELIVER}`,
                'depois do limite': `${topic}.${subscription}.DLQ`,
              },
            );
          });
      },
    });
    this.consumers.push(consumer);
    this.logger.log(`Assinado ${topic} (${subscription}, KeyShared)`);
  }

  /** Lista TODOS os tópicos do namespace: tópicos "perdidos" aparecem aqui. */
  async stats(): Promise<TopicStats[]> {
    const base = `${this.adminUrl}/admin/v2/persistent/${NAMESPACE}`;
    const names = await this.getJson<string[]>(base);
    const result: TopicStats[] = [];
    for (const full of names) {
      const short = full.split('/').pop()!;
      const s = await this.getJson<PulsarTopicStats>(
        `${base}/${encodeURIComponent(short)}/stats`,
      );
      result.push({
        topic: short,
        published: s.msgInCounter ?? 0,
        subscriptions: Object.entries(s.subscriptions ?? {}).map(
          ([name, sub]) => ({
            name,
            backlog: sub.msgBacklog ?? 0,
            acked: sub.msgOutCounter ?? 0,
            redeliveries: this.redeliveries.get(name) ?? 0,
          }),
        ),
      });
    }
    return result;
  }

  /** Visão completa de cada tópico para a aba "Pulsar" do painel. */
  async inspect() {
    const base = `${this.adminUrl}/admin/v2/persistent/${NAMESPACE}`;
    const names = await this.getJson<string[]>(base);
    const topics: unknown[] = [];
    for (const full of names) {
      const short = full.split('/').pop()!;
      const enc = encodeURIComponent(short);
      const [s, internal] = await Promise.all([
        this.getJson<PulsarTopicStats>(`${base}/${enc}/stats`),
        this.getJson<PulsarInternalStats>(`${base}/${enc}/internalStats`),
      ]);
      topics.push({
        name: full,
        short,
        msgInCounter: s.msgInCounter ?? 0,
        msgOutCounter: s.msgOutCounter ?? 0,
        bytesInCounter: s.bytesInCounter ?? 0,
        msgRateIn: s.msgRateIn ?? 0,
        msgRateOut: s.msgRateOut ?? 0,
        storageSize: s.storageSize ?? 0,
        publishers: (s.publishers ?? []).map((p) => ({
          name: p.producerName,
          address: p.address,
          connectedSince: p.connectedSince,
          client: p.clientVersion,
        })),
        subscriptions: Object.entries(s.subscriptions ?? {}).map(
          ([name, sub]) => ({
            name,
            type: sub.type,
            backlog: sub.msgBacklog ?? 0,
            unacked: sub.unackedMessages ?? 0,
            delivered: sub.msgOutCounter ?? 0,
            redeliveries: this.redeliveries.get(name) ?? 0,
            lastAckedAt: sub.lastAckedTimestamp || null,
            cursor: internal.cursors?.[name]
              ? {
                  markDeletePosition:
                    internal.cursors[name].markDeletePosition ?? null,
                  readPosition: internal.cursors[name].readPosition ?? null,
                  consumed: internal.cursors[name].messagesConsumedCounter ?? 0,
                }
              : null,
            consumers: (sub.consumers ?? []).map((c) => ({
              name: c.consumerName,
              address: c.address,
              permits: c.availablePermits,
              unacked: c.unackedMessages,
              delivered: c.msgOutCounter,
              connectedSince: c.connectedSince,
            })),
          }),
        ),
        storage: {
          entries: internal.numberOfEntries ?? 0,
          lastConfirmedEntry: internal.lastConfirmedEntry,
          state: internal.state,
          ledgers: internal.ledgers ?? [],
        },
      });
    }
    return {
      serviceUrl: this.serviceUrl,
      adminUrl: this.adminUrl,
      namespace: NAMESPACE,
      maxRedeliver: MAX_REDELIVER,
      nackRedeliverMs: 1000,
      topics,
    };
  }

  async healthy() {
    try {
      const res = await fetch(`${this.adminUrl}/admin/v2/brokers/health`, {
        signal: AbortSignal.timeout(2000),
      });
      return res.ok;
    } catch {
      return false;
    }
  }

  reset() {
    // Contadores do Pulsar são do broker e não podem ser zerados daqui.
    this.redeliveries.clear();
  }

  private async getJson<T>(url: string): Promise<T> {
    const res = await fetch(url, { signal: AbortSignal.timeout(3000) });
    if (!res.ok) throw new Error(`Pulsar admin ${res.status} em ${url}`);
    return (await res.json()) as T;
  }

  async onApplicationShutdown() {
    for (const p of this.producers.values()) {
      try {
        const producer = await p;
        await producer.flush();
        await producer.close();
      } catch {
        /* producer nunca foi criado */
      }
    }
    for (const c of this.consumers) await c.close().catch(() => undefined);
    await this.client.close();
    this.logger.log('Conexões com o Pulsar encerradas');
  }
}
