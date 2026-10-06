import { generateSecret, sign, verify } from './webhook-signature';

describe('assinatura de webhook', () => {
  const secret = generateSecret();
  const body = JSON.stringify({ type: 'order.confirmed', data: { a: 1 } });
  const now = 1_800_000_000;
  const headers = (signature: string, timestamp = now) => ({
    id: 'msg_1',
    timestamp: String(timestamp),
    signature,
  });

  it('aceita a assinatura gerada com o mesmo segredo', () => {
    const sig = sign(secret, 'msg_1', now, body);
    expect(sig).toMatch(/^v1,/);
    expect(verify(secret, headers(sig), body, now)).toEqual({ ok: true });
  });

  it('recusa corpo alterado', () => {
    const sig = sign(secret, 'msg_1', now, body);
    expect(verify(secret, headers(sig), body + ' ', now).ok).toBe(false);
  });

  it('recusa outro segredo', () => {
    const sig = sign(generateSecret(), 'msg_1', now, body);
    expect(verify(secret, headers(sig), body, now).ok).toBe(false);
  });

  it('recusa timestamp fora da janela (replay)', () => {
    const old = now - 600;
    const sig = sign(secret, 'msg_1', old, body);
    expect(verify(secret, headers(sig, old), body, now)).toEqual({
      ok: false,
      reason: expect.stringMatching(/janela/),
    });
  });

  it('aceita quando uma das várias assinaturas confere (troca de segredo)', () => {
    const sig = `v1,invalida ${sign(secret, 'msg_1', now, body)}`;
    expect(verify(secret, headers(sig), body, now).ok).toBe(true);
  });
});
