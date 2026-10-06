/**
 * Conversão de valores monetários sem ponto flutuante.
 * Entrada da API é string decimal ("199.90"); internamente usamos centavos (bigint).
 */
const DECIMAL = /^\d{1,13}(\.\d{1,2})?$/;

export function isValidDecimal(value: string): boolean {
  return DECIMAL.test(value);
}

export function toCents(value: string): bigint {
  if (!isValidDecimal(value)) {
    throw new Error(`Valor monetário inválido: "${value}"`);
  }
  const [int, frac = ''] = value.split('.');
  return BigInt(int) * 100n + BigInt(frac.padEnd(2, '0'));
}

export function fromCents(cents: bigint): string {
  const sign = cents < 0n ? '-' : '';
  const abs = cents < 0n ? -cents : cents;
  return `${sign}${abs / 100n}.${String(abs % 100n).padStart(2, '0')}`;
}
