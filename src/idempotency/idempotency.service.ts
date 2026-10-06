import {
  ConflictException,
  Injectable,
  UnprocessableEntityException,
} from '@nestjs/common';
import { createHash } from 'node:crypto';
import { eq, sql } from 'drizzle-orm';
import type { Tx } from '../db/db.module';
import { idempotencyKeys } from '../db/schema';

export type IdempotencyBegin =
  { state: 'NEW' } | { state: 'REPLAY'; status: number; body: unknown };

/**
 * Idempotency-Key no estilo Stripe.
 * Deve ser chamado DENTRO da mesma transação que cria o recurso:
 * - requisição nova → grava IN_PROGRESS (a PK trava requisições concorrentes
 *   com a mesma chave até o commit);
 * - mesma chave + mesmo corpo, já concluída → devolve a resposta salva (replay);
 * - mesma chave + corpo diferente → 422.
 */
@Injectable()
export class IdempotencyService {
  static hash(body: unknown): string {
    return createHash('sha256').update(stableStringify(body)).digest('hex');
  }

  async begin(
    tx: Tx,
    key: string,
    requestHash: string,
  ): Promise<IdempotencyBegin> {
    const inserted = await tx
      .insert(idempotencyKeys)
      .values({ key, requestHash, status: 'IN_PROGRESS' })
      .onConflictDoNothing()
      .returning({ key: idempotencyKeys.key });
    if (inserted.length) return { state: 'NEW' };

    const [existing] = await tx
      .select()
      .from(idempotencyKeys)
      .where(eq(idempotencyKeys.key, key));

    if (existing.requestHash !== requestHash) {
      throw new UnprocessableEntityException(
        'Idempotency-Key já usada com outro corpo de requisição',
      );
    }
    if (existing.status === 'IN_PROGRESS') {
      throw new ConflictException(
        'Requisição com esta Idempotency-Key em andamento',
      );
    }
    return {
      state: 'REPLAY',
      status: existing.responseStatus ?? 200,
      body: existing.responseBody,
    };
  }

  /** Registra um reenvio recusado (409): aparece no relatório final do pedido. */
  async recordDuplicate(tx: Tx, key: string) {
    await tx
      .update(idempotencyKeys)
      .set({
        duplicateRequests: sql`${idempotencyKeys.duplicateRequests} + 1`,
        lastDuplicateAt: new Date(),
      })
      .where(eq(idempotencyKeys.key, key));
  }

  async complete(tx: Tx, key: string, status: number, body: unknown) {
    await tx
      .update(idempotencyKeys)
      .set({ status: 'COMPLETED', responseStatus: status, responseBody: body })
      .where(eq(idempotencyKeys.key, key));
  }
}

/** JSON com chaves ordenadas: {a,b} e {b,a} geram o mesmo hash. */
function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  const obj = value as Record<string, unknown>;
  return `{${Object.keys(obj)
    .sort()
    .map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`)
    .join(',')}}`;
}
