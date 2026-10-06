import { backoffMs } from './backoff';

describe('backoffMs', () => {
  const noJitter = { random: () => 0 };

  it('dobra a cada tentativa', () => {
    expect([1, 2, 3, 4, 5].map((a) => backoffMs(a, noJitter))).toEqual([
      1000, 2000, 4000, 8000, 16000,
    ]);
  });

  it('respeita o teto', () => {
    expect(backoffMs(30, noJitter)).toBe(300_000);
  });

  it('adiciona jitter de até 1s', () => {
    expect(backoffMs(1, { random: () => 0.999 })).toBe(1999);
  });
});
