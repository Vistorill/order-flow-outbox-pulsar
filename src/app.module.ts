import { Module } from '@nestjs/common';
import { ConditionalModule, ConfigModule } from '@nestjs/config';
import { APP_FILTER, APP_PIPE } from '@nestjs/core';
import { ScheduleModule } from '@nestjs/schedule';
import { ZodValidationPipe } from 'nestjs-zod';
import { BrokerModule } from './broker/broker.module';
import { AllExceptionsFilter } from './common/http-exception.filter';
import { validateEnv } from './config/env';
import { ConsumersModule } from './consumers/consumers.module';
import { DbModule } from './db/db.module';
import { DebugModule } from './debug/debug.module';
import { HealthController } from './health/health.controller';
import { OrdersModule } from './orders/orders.module';
import { OutboxModule } from './outbox/outbox.module';
import { ShippingModule } from './shipping/shipping.module';
import { WebhooksModule } from './webhooks/webhooks.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true, validate: validateEnv }),
    ScheduleModule.forRoot(),
    DbModule,
    BrokerModule,
    OutboxModule,
    WebhooksModule,
    OrdersModule,
    ConsumersModule,
    ShippingModule,
    // Avaliado depois do .env carregar (no original, o env era lido em tempo de import).
    ConditionalModule.registerWhen(
      DebugModule,
      (env) => env.DEBUG_PANEL !== 'false',
    ),
  ],
  controllers: [HealthController],
  providers: [
    { provide: APP_PIPE, useClass: ZodValidationPipe },
    { provide: APP_FILTER, useClass: AllExceptionsFilter },
  ],
})
export class AppModule {}
