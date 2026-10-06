import {
  MiddlewareConsumer,
  Module,
  NestModule,
  RequestMethod,
} from '@nestjs/common';
import { ShippingModule } from '../shipping/shipping.module';
import { TraceMiddleware } from '../tracing/trace.middleware';
import {
  CarrierSimulator,
  CarrierSimulatorController,
} from './carrier-simulator.controller';
import { DebugController } from './debug.controller';
import { DemoReceiverController } from './demo-receiver.controller';
import { JourneyService } from './journey.service';

@Module({
  imports: [ShippingModule],
  controllers: [
    DebugController,
    DemoReceiverController,
    CarrierSimulatorController,
  ],
  providers: [JourneyService, CarrierSimulator],
})
export class DebugModule implements NestModule {
  /** Linha do tempo de cada pedido, mostrada no painel em "Requisições em tempo real". */
  configure(consumer: MiddlewareConsumer) {
    consumer
      .apply(TraceMiddleware)
      .forRoutes({ path: 'orders', method: RequestMethod.POST });
  }
}
