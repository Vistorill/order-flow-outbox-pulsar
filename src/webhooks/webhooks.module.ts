import { Global, Module } from '@nestjs/common';
import { WebhookDispatcherService } from './webhook-dispatcher.service';
import { WebhooksController } from './webhooks.controller';
import { WebhooksService } from './webhooks.service';

/** Global: o consumidor cria as entregas dentro da transação dele. */
@Global()
@Module({
  controllers: [WebhooksController],
  providers: [WebhooksService, WebhookDispatcherService],
  exports: [WebhooksService, WebhookDispatcherService],
})
export class WebhooksModule {}
