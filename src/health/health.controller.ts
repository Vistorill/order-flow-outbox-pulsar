import {
  Controller,
  Get,
  Inject,
  ServiceUnavailableException,
} from '@nestjs/common';
import { sql } from 'drizzle-orm';
import { MessageBroker } from '../broker/message-broker';
import { DRIZZLE, type Database } from '../db/db.module';

@Controller('health')
export class HealthController {
  constructor(
    @Inject(DRIZZLE) private readonly db: Database,
    private readonly broker: MessageBroker,
  ) {}

  @Get()
  async check() {
    const database = await this.db
      .execute(sql`select 1`)
      .then(() => true)
      .catch(() => false);
    const broker = await this.broker.healthy();
    const body = {
      status: database && broker ? 'ok' : 'degraded',
      database,
      broker,
    };
    if (!database) throw new ServiceUnavailableException(body);
    return body;
  }
}
