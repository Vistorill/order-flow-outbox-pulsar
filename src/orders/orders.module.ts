import { Module } from '@nestjs/common';
import { IdempotencyService } from '../idempotency/idempotency.service';
import { OrdersController } from './orders.controller';
import { OrdersService } from './orders.service';

@Module({
  controllers: [OrdersController],
  providers: [OrdersService, IdempotencyService],
})
export class OrdersModule {}
