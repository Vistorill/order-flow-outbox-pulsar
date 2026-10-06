import { Injectable } from '@nestjs/common';
import type { Tx } from '../db/db.module';
import { outbox } from '../db/schema';
import type { Topic } from '../events/topics';

export interface OutboxMessage {
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  topic: Topic;
  payload: Record<string, unknown>;
}

/**
 * Único jeito de registrar um evento: exige uma transação aberta (`tx`),
 * então o evento sempre nasce junto com a mudança de estado que o originou.
 */
@Injectable()
export class OutboxService {
  async add(tx: Tx, msg: OutboxMessage) {
    const [row] = await tx
      .insert(outbox)
      .values(msg)
      .returning({ id: outbox.id });
    return row.id;
  }
}
