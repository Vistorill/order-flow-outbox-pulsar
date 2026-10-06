import { fromCents, isValidDecimal, toCents } from './money';

describe('money', () => {
  it('converte decimal em string para centavos sem ponto flutuante', () => {
    expect(toCents('199.90')).toBe(19990n);
    expect(toCents('0.1')).toBe(10n);
    expect(toCents('7')).toBe(700n);
    expect(toCents('0.10') + toCents('0.20')).toBe(30n); // em float: 0.30000000000000004
  });

  it('formata centavos de volta para decimal', () => {
    expect(fromCents(19990n)).toBe('199.90');
    expect(fromCents(5n)).toBe('0.05');
    expect(fromCents(-150n)).toBe('-1.50');
  });

  it('rejeita formatos inválidos', () => {
    for (const v of ['1.234', 'abc', '1,50', '', '-1.00', '.5']) {
      expect(isValidDecimal(v)).toBe(false);
      expect(() => toCents(v)).toThrow();
    }
  });
});
