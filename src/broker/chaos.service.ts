import { Injectable, Logger } from '@nestjs/common';

export interface ChaosFlags {
  /** O relay falha ao publicar (simula broker fora do ar). */
  failPublish: boolean;
  /** O consumidor lança erro (simula bug ou dependência fora do ar) → nack. */
  failConsumer: boolean;
  /** O relay publica mas "cai" antes de marcar published → reentrega. */
  crashAfterPublish: boolean;
  /** O receptor de webhook de demonstração responde 503 (cliente fora do ar). */
  failWebhook: boolean;
  /** A API da transportadora simulada responde 503. */
  failCarrierApi: boolean;
}

/**
 * Injeção de falhas para a demo. Só pode ser alterada pelas rotas /debug,
 * que existem apenas com DEBUG_PANEL=true.
 */
@Injectable()
export class ChaosService {
  private readonly logger = new Logger('Chaos');
  private flags: ChaosFlags = {
    failPublish: false,
    failConsumer: false,
    crashAfterPublish: false,
    failWebhook: false,
    failCarrierApi: false,
  };

  get(): ChaosFlags {
    return { ...this.flags };
  }

  set(patch: Partial<ChaosFlags>): ChaosFlags {
    this.flags = { ...this.flags, ...patch };
    this.logger.warn(`Flags de caos: ${JSON.stringify(this.flags)}`);
    return this.get();
  }

  beforePublish() {
    if (this.flags.failPublish) {
      throw new Error('Broker indisponível (falha simulada)');
    }
  }

  shouldCrashAfterPublish(): boolean {
    return this.flags.crashAfterPublish;
  }

  beforeConsume() {
    if (this.flags.failConsumer) {
      throw new Error('Consumidor falhou (falha simulada)');
    }
  }
}
