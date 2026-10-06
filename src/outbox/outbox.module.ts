import { Global, Module } from '@nestjs/common';
import { OutboxCleanupService } from './outbox-cleanup.service';
import { OutboxRelayService } from './outbox-relay.service';
import { OutboxService } from './outbox.service';

@Global()
@Module({
  providers: [OutboxService, OutboxRelayService, OutboxCleanupService],
  exports: [OutboxService, OutboxRelayService],
})
export class OutboxModule {}
