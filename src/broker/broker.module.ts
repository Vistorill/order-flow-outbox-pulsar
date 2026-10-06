import { Global, Logger, Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ChaosService } from './chaos.service';
import { InMemoryBroker } from './in-memory.broker';
import { MessageBroker } from './message-broker';
import { PulsarBroker } from './pulsar.broker';

@Global()
@Module({
  providers: [
    ChaosService,
    {
      provide: MessageBroker,
      inject: [ConfigService],
      useFactory: (config: ConfigService): MessageBroker => {
        const kind = config.get<'pulsar' | 'memory'>('BROKER', 'pulsar');
        new Logger('BrokerModule').log(`Broker selecionado: ${kind}`);
        if (kind === 'memory') return new InMemoryBroker();
        return new PulsarBroker(
          config.getOrThrow<string>('PULSAR_URL'),
          config.getOrThrow<string>('PULSAR_ADMIN_URL'),
        );
      },
    },
  ],
  exports: [MessageBroker, ChaosService],
})
export class BrokerModule {}
