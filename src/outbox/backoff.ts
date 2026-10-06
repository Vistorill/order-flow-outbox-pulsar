/**
 * Backoff exponencial com jitter: 1s, 2s, 4s, 8s... até `maxMs`, mais até 1s aleatório.
 * O jitter evita que muitos eventos que falharam juntos sejam retentados juntos.
 */
export function backoffMs(
  attempt: number,
  {
    baseMs = 1000,
    maxMs = 300_000,
    jitterMs = 1000,
    random = Math.random,
  } = {},
): number {
  const exp = Math.min(baseMs * 2 ** Math.max(0, attempt - 1), maxMs);
  return exp + Math.floor(random() * jitterMs);
}
