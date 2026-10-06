/**
 * Abstração do broker. A aplicação depende desta classe, não do Pulsar:
 * - PulsarBroker: uso real (docker-compose).
 * - InMemoryBroker: testes e demo sem infraestrutura, com a mesma semântica
 *   de ack/nack e reentrega.
 */
export interface PublishOptions {
  eventId: string;
  /** Chave de ordenação: mesma chave → mesma ordem de entrega. */
  key: string;
}

export interface BrokerMessage {
  topic: string;
  eventId: string;
  key: string;
  payload: unknown;
  /** Quantas vezes esta mensagem já foi reentregue (0 na primeira entrega). */
  redeliveryCount: number;
}

/** Resolver = ack. Lançar erro = nack → o broker reentrega depois. */
export type MessageHandler = (msg: BrokerMessage) => Promise<void>;

export interface SubscribeOptions {
  topic: string;
  subscription: string;
  handler: MessageHandler;
}

export interface TopicStats {
  topic: string;
  /** Mensagens publicadas no tópico. */
  published: number;
  subscriptions: {
    name: string;
    /** Mensagens ainda não confirmadas (fila). */
    backlog: number;
    /** Mensagens confirmadas. */
    acked: number;
    redeliveries: number;
  }[];
}

export abstract class MessageBroker {
  abstract readonly kind: 'pulsar' | 'memory';
  abstract publish(
    topic: string,
    payload: unknown,
    opts: PublishOptions,
  ): Promise<void>;
  abstract subscribe(opts: SubscribeOptions): Promise<void>;
  abstract stats(): Promise<TopicStats[]>;
  abstract healthy(): Promise<boolean>;
  /** Detalhes internos do broker para a aba "Pulsar" do painel (null se não houver). */
  inspect(): Promise<unknown> {
    return Promise.resolve(null);
  }
  /** Usado pelo "Limpar tudo" do painel: zera contadores locais. */
  reset(): void {}
}
