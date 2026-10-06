import { Module } from '@nestjs/common';
import { CarrierClient } from './carrier.client';
import { InboundWebhooksController } from './inbound-webhooks.controller';
import { ShippingConsumer } from './shipping.consumer';

/** Integração com a transportadora: envio da remessa e webhook de entrega. */
@Module({
  controllers: [InboundWebhooksController],
  providers: [CarrierClient, ShippingConsumer],
  exports: [CarrierClient],
})
export class ShippingModule {}
