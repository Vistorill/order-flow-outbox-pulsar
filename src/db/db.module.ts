import {
  Global,
  Inject,
  Injectable,
  Module,
  OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import * as schema from './schema';

export const DRIZZLE = Symbol('DRIZZLE');
export const PG_CLIENT = Symbol('PG_CLIENT');

export function createDb(client: postgres.Sql) {
  return drizzle(client, { schema });
}

export type Database = ReturnType<typeof createDb>;
/** Tipo do `tx` recebido em `db.transaction(async (tx) => ...)`. */
export type Tx = Parameters<Parameters<Database['transaction']>[0]>[0];
/** Aceita tanto o db quanto uma transação aberta. */
export type DbOrTx = Database | Tx;

@Injectable()
class PgShutdown implements OnApplicationShutdown {
  constructor(@Inject(PG_CLIENT) private readonly client: postgres.Sql) {}
  async onApplicationShutdown() {
    await this.client.end({ timeout: 5 });
  }
}

@Global()
@Module({
  providers: [
    {
      provide: PG_CLIENT,
      inject: [ConfigService],
      useFactory: (config: ConfigService) =>
        postgres(config.getOrThrow<string>('DATABASE_URL'), {
          max: 10,
          onnotice: () => undefined,
        }),
    },
    {
      provide: DRIZZLE,
      inject: [PG_CLIENT],
      useFactory: (client: postgres.Sql) => createDb(client),
    },
    PgShutdown,
  ],
  exports: [DRIZZLE, PG_CLIENT],
})
export class DbModule {}
