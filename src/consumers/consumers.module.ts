import { Module } from '@nestjs/common';
import { OrderCreatedConsumer } from './order-created.consumer';

@Module({
  providers: [OrderCreatedConsumer],
  exports: [OrderCreatedConsumer],
})
export class ConsumersModule {}
