import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';

/**
 * Assinatura no formato Standard Webhooks (https://www.standardwebhooks.com):
 *
 *   conteúdo assinado = `${webhook-id}.${webhook-timestamp}.${corpo}`
 *   webhook-signature = `v1,${base64(HMAC-SHA256(segredo, conteúdo))}`
 *
 * O timestamp entra na assinatura para o cliente recusar reenvios antigos
 * (replay attack), e o id para ele deduplicar retries.
 */
export const SIGNATURE_VERSION = 'v1';
/** Avisos com timestamp mais velho (ou mais novo) que isso são recusados. */
export const TIMESTAMP_TOLERANCE_SECONDS = 5 * 60;

export function generateSecret(): string {
  return `whsec_${randomBytes(24).toString('base64')}`;
}

export function sign(
  secret: string,
  id: string,
  timestamp: number,
  body: string,
): string {
  const mac = createHmac('sha256', secret)
    .update(`${id}.${timestamp}.${body}`)
    .digest('base64');
  return `${SIGNATURE_VERSION},${mac}`;
}

export type VerifyResult = { ok: true } | { ok: false; reason: string };

/** O que o cliente final faz ao receber: confere assinatura e janela de tempo. */
export function verify(
  secret: string,
  headers: { id?: string; timestamp?: string; signature?: string },
  body: string,
  nowSeconds = Math.floor(Date.now() / 1000),
): VerifyResult {
  const { id, timestamp, signature } = headers;
  if (!id || !timestamp || !signature) {
    return {
      ok: false,
      reason:
        'Headers webhook-id, webhook-timestamp e webhook-signature são obrigatórios',
    };
  }
  const ts = Number(timestamp);
  if (!Number.isInteger(ts))
    return { ok: false, reason: 'webhook-timestamp inválido' };
  if (Math.abs(nowSeconds - ts) > TIMESTAMP_TOLERANCE_SECONDS) {
    return {
      ok: false,
      reason: 'webhook-timestamp fora da janela de 5 minutos',
    };
  }
  const expected = Buffer.from(sign(secret, id, ts, body));
  // O header pode trazer várias assinaturas separadas por espaço (troca de segredo).
  const match = signature.split(' ').some((candidate) => {
    const got = Buffer.from(candidate);
    return got.length === expected.length && timingSafeEqual(got, expected);
  });
  return match ? { ok: true } : { ok: false, reason: 'Assinatura não confere' };
}

/** Mostra só o começo do segredo nas listagens. */
export function maskSecret(secret: string): string {
  return `${secret.slice(0, 10)}…`;
}
