import { drizzle } from 'drizzle-orm/postgres-js';
import { migrate } from 'drizzle-orm/postgres-js/migrator';
import postgres from 'postgres';
import { join } from 'node:path';

/** Aplica as migrações no banco de teste antes de rodar a suíte. */
export default async function globalSetup() {
  const url =
    process.env.DATABASE_URL_TEST ??
    'postgres://outbox:outbox@localhost:5445/outbox_test';
  process.env.DATABASE_URL_TEST = url;
  const client = postgres(url, { max: 1, onnotice: () => undefined });
  await migrate(drizzle(client), {
    migrationsFolder: join(__dirname, '..', 'drizzle'),
  });
  await client.end();
}
