import { Inject, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import { and, eq, lt, sql } from 'drizzle-orm';
import { DRIZZLE, type Database } from '../db/db.module';
import { outbox } from '../db/schema';

/** Expurgo diário de eventos já publicados (a tabela não cresce sem limite). */
@Injectable()
export class OutboxCleanupService {
  private readonly logger = new Logger('OutboxCleanup');

  constructor(
    @Inject(DRIZZLE) private readonly db: Database,
    private readonly config: ConfigService,
  ) {}

  @Cron('0 3 * * *')
  async purge() {
    const days = this.config.get<number>('OUTBOX_RETENTION_DAYS', 7);
    const removed = await this.db
      .delete(outbox)
      .where(
        and(
          eq(outbox.status, 'published'),
          lt(outbox.publishedAt, sql`now() - make_interval(days => ${days})`),
        ),
      )
      .returning({ id: outbox.id });
    this.logger.log(
      `Expurgo: ${removed.length} eventos publicados há mais de ${days} dias`,
    );
    return removed.length;
  }
}
