#!/usr/bin/env node
/**
 * Testes de falha AO VIVO, pelos mesmos interruptores do painel ("Injetar falhas").
 *
 * Para cada falha: liga, cria um pedido, confere o comportamento descrito no
 * painel, desliga e confere a recuperação. As falhas são sempre desligadas no
 * final, mesmo se um teste quebrar.
 *
 *   pnpm run test:chaos                         # contra http://localhost:3000
 *   pnpm run test:chaos -- --url http://localhost:3996
 *   pnpm run test:chaos -- --only broker,webhook
 *
 * Rode com UMA instância do app apontando para o banco: outra instância
 * (sem a falha ligada) processaria os pedidos e mascararia o teste.
 */

const args = process.argv.slice(2);
const arg = (name, def) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : def;
};
const BASE = arg('url', process.env.CHAOS_URL ?? 'http://localhost:3000').replace(/\/$/, '');
const ONLY = arg('only', '')?.split(',').filter(Boolean) ?? [];

const FLAGS = ['failPublish', 'failConsumer', 'crashAfterPublish', 'failCarrierApi', 'failWebhook'];
const color = (c, s) => (process.stdout.isTTY ? `\x1b[${c}m${s}\x1b[0m` : s);
const ok = (s) => color('32', s);
const bad = (s) => color('31', s);
const dim = (s) => color('2', s);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(method, path, body, headers = {}) {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: body !== undefined ? { 'content-type': 'application/json', ...headers } : headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  return { status: res.status, data };
}

const chaos = (flags) => api('PUT', '/debug/chaos', flags);
const allOff = () => chaos(Object.fromEntries(FLAGS.map((f) => [f, false])));

/** Espera `fn` devolver algo verdadeiro (polling). */
async function waitFor(label, fn, timeoutMs) {
  const start = Date.now();
  let last;
  while (Date.now() - start < timeoutMs) {
    last = await fn();
    if (last) return last;
    await sleep(400);
  }
  throw new Error(`Tempo esgotado (${Math.round(timeoutMs / 1000)}s) esperando: ${label}`);
}

async function createOrder(amount = '42.00') {
  const r = await api('POST', '/orders', { customerEmail: 'chaos@teste.com', amount }, {
    'Idempotency-Key': crypto.randomUUID(),
  });
  if (r.status !== 201) throw new Error(`POST /orders respondeu ${r.status}: ${JSON.stringify(r.data)}`);
  return r.data.id;
}

const outboxOf = async (orderId, type) => {
  const r = await api('GET', '/debug/outbox?limit=200');
  return r.data.find((e) => e.aggregateId === orderId && e.eventType === type);
};
const order = async (id) => (await api('GET', `/orders/${id}`)).data;
const report = async (id) => (await api('GET', `/orders/${id}/report`)).data;
const journey = async (id) =>
  (await api('GET', '/debug/journeys?limit=30')).data.find((j) => j.orderId === id);
const webhooksOf = async (id) => {
  const r = await api('GET', '/debug/webhooks');
  return {
    deliveries: r.data.deliveries.filter((d) => d.payload?.data?.orderId === id),
    received: r.data.received.filter((x) => x.body?.data?.orderId === id),
    config: r.data.config,
  };
};

/* ------------------------------------------------------------------ cenários */

let cfg;
const results = [];

async function scenario(key, title, fn) {
  if (ONLY.length && !ONLY.includes(key)) return;
  const steps = [];
  const step = (text) => {
    steps.push(text);
    console.log(`   ${ok('✓')} ${steps.length}. ${text}`);
  };
  console.log(`\n${color('1', title)}`);
  const started = Date.now();
  try {
    await allOff();
    await fn(step);
    results.push({ title, ok: true, ms: Date.now() - started, steps: steps.length });
  } catch (err) {
    console.log(`   ${bad('✗')} ${steps.length + 1}. ${err.message}`);
    results.push({ title, ok: false, ms: Date.now() - started, error: err.message });
  } finally {
    await allOff();
  }
}

async function main() {
  console.log(`Testes de falha contra ${BASE}`);
  const health = await api('GET', '/health').catch(() => null);
  if (!health || health.status !== 200) {
    console.error(bad(`API não respondeu em ${BASE}/health. Suba o app (pnpm run start:dev) e tente de novo.`));
    process.exit(2);
  }
  const overview = (await api('GET', '/debug/overview')).data;
  const wh = await api('GET', '/debug/webhooks');
  cfg = { ...overview.relay, webhookMaxAttempts: wh.data.config.maxAttempts };
  console.log(dim(`Broker: ${overview.broker} · lease ${cfg.leaseSeconds}s · relay a cada ${cfg.intervalMs}ms · webhook máx. ${cfg.webhookMaxAttempts} tentativas`));

  // Endpoint do cliente de demonstração recebendo todos os eventos.
  const demoUrl = `${BASE}/demo-receiver`;
  const eps = (await api('GET', '/webhooks/endpoints')).data;
  const demo = eps.find((e) => e.active && e.url === demoUrl);
  if (!demo) await api('POST', '/webhooks/endpoints', { url: demoUrl, description: 'Cliente final (testes de falha)', events: ['*'] });
  else if (!demo.events.includes('*')) await api('PATCH', `/webhooks/endpoints/${demo.id}`, { events: ['*'] });

  const T = (s) => s * 1000;

  await scenario('happy', '0. Caminho feliz (referência, sem falha)', async (step) => {
    const id = await createOrder();
    step(`Pedido ${id.slice(0, 8)} criado: POST respondeu PENDING`);
    const j = await waitFor('os 11 passos concluídos', async () => {
      const x = await journey(id);
      return x?.steps.every((s) => s.status === 'done') && x;
    }, T(40));
    step(`Fluxo completo em ${((new Date(j.finishedAt) - new Date(j.startedAt)) / 1000).toFixed(1)}s: 11/11 passos`);
    const o = await order(id);
    if (o.paymentStatus !== 'PAID' || o.deliveryStatus !== 'DELIVERED') throw new Error(`status inesperado: ${o.paymentStatus}/${o.deliveryStatus}`);
    step('Consulta: paymentStatus PAID e deliveryStatus DELIVERED');
  });

  await scenario('broker', '1. Broker fora do ar', async (step) => {
    await chaos({ failPublish: true });
    step('Falha ligada: o relay não consegue publicar');
    const id = await createOrder();
    const e = await waitFor('2 tentativas com erro na outbox', async () => {
      const x = await outboxOf(id, 'OrderCreated');
      return x && x.attempts >= 2 && x.lastError ? x : null;
    }, T(20));
    step(`Evento ficou na outbox: ${e.attempts} tentativas, status ${e.status}, erro "${e.lastError}"`);
    if (e.status !== 'pending' || new Date(e.availableAt) <= new Date(Date.now() - 500)) throw new Error('esperava backoff (available_at no futuro)');
    step(`Backoff: próxima tentativa agendada para ${new Date(e.availableAt).toLocaleTimeString('pt-BR')}`);
    if ((await order(id)).paymentStatus !== 'PENDING') throw new Error('pedido não deveria estar pago com o broker fora');
    step('Pedido continua PENDING: nada foi confirmado sem o evento');
    await chaos({ failPublish: false });
    step('Falha desligada');
    await waitFor('pedido PAID após a recuperação', async () => (await order(id)).paymentStatus === 'PAID', T(60));
    const after = await outboxOf(id, 'OrderCreated');
    step(`Recuperou: evento publicado na tentativa ${after.attempts} e pedido PAID`);
  });

  await scenario('consumer', '2. Consumidor quebrado', async (step) => {
    await chaos({ failConsumer: true });
    step('Falha ligada: o consumidor lança erro');
    const id = await createOrder();
    const e = await waitFor('evento publicado', async () => {
      const x = await outboxOf(id, 'OrderCreated');
      return x?.status === 'published' ? x : null;
    }, T(20));
    step('Relay publicou normalmente (o problema é do lado do consumidor)');
    const nacks = await waitFor('NACKs registrados', async () => {
      const m = (await api('GET', '/debug/messaging')).data.find((t) => t.eventId === e.id);
      const n = m?.steps.filter((s) => s.kind === 'nack').length ?? 0;
      return n >= 2 ? n : null;
    }, T(20));
    step(`O broker reentregou: ${nacks} NACKs até agora`);
    if ((await order(id)).paymentStatus !== 'PENDING') throw new Error('pedido não deveria estar pago com o consumidor quebrado');
    step('Pedido continua PENDING: o rollback desfez cada tentativa');
    await chaos({ failConsumer: false });
    step('Falha desligada');
    await waitFor('pedido PAID', async () => (await order(id)).paymentStatus === 'PAID', T(30));
    const r = await report(id);
    step(`Recuperou: pedido PAID, efeito aplicado uma vez (duplicidades descartadas: ${r.duplicates.brokerRedeliveries})`);
  });

  await scenario('crash', '3. Queda após publicar', async (step) => {
    await chaos({ crashAfterPublish: true });
    step('Falha ligada: o relay publica e "morre" antes de marcar');
    const id = await createOrder();
    const e = await waitFor('evento publicado e preso em processing', async () => {
      const x = await outboxOf(id, 'OrderCreated');
      return x?.status === 'processing' && (await order(id)).paymentStatus === 'PAID' ? x : null;
    }, T(20));
    step(`Mensagem chegou (pedido PAID), mas a outbox ficou "processing" com lease até ${new Date(e.lockedUntil).toLocaleTimeString('pt-BR')}`);
    await chaos({ crashAfterPublish: false });
    step('Falha desligada');
    const after = await waitFor('lease expirar e o evento sair de novo', async () => {
      const x = await outboxOf(id, 'OrderCreated');
      return x?.status === 'published' ? x : null;
    }, T(cfg.leaseSeconds + 30));
    step(`Lease expirou: evento publicado de novo (tentativa ${after.attempts})`);
    const r = await waitFor('consumidor descartar a duplicata', async () => {
      const x = await report(id);
      return x.duplicates.brokerRedeliveries >= 1 ? x : null;
    }, T(15));
    step(`Consumidor descartou ${r.duplicates.brokerRedeliveries} cópia(s): pedido confirmado uma única vez`);
  });

  await scenario('carrier', '4. API da transportadora fora do ar', async (step) => {
    await chaos({ failCarrierApi: true });
    step('Falha ligada: a transportadora responde 503');
    const id = await createOrder();
    const s7 = await waitFor('2 tentativas de chamar a transportadora', async () => {
      const st = (await journey(id))?.steps[6];
      return st?.info?.tentativas >= 2 ? st : null;
    }, T(30));
    step(`Serviço de envio recebeu ${s7.info.resposta} e devolveu o evento (NACK): ${s7.info.tentativas} tentativas`);
    if ((await order(id)).deliveryStatus !== 'PENDING') throw new Error('não deveria ter remessa ainda');
    step('Pedido PAID mas sem remessa: deliveryStatus PENDING');
    await chaos({ failCarrierApi: false });
    step('Falha desligada');
    await waitFor('pedido entregue', async () => (await order(id)).deliveryStatus === 'DELIVERED', T(40));
    const sims = (await api('GET', '/debug/carrier')).data.filter((x) => x.request?.reference === id);
    if (sims.length !== 1) throw new Error(`esperava 1 remessa na transportadora, há ${sims.length}`);
    const calls = (await journey(id)).steps[6].info.tentativas;
    step(`Recuperou: ${calls} chamadas com a mesma Idempotency-Key (as anteriores receberam 503) e 1 remessa só (${sims[0].id}); pedido DELIVERED`);
  });

  await scenario('webhook', '5. Cliente do webhook fora do ar', async (step) => {
    await chaos({ failWebhook: true });
    step('Falha ligada: o receptor do cliente responde 503');
    const id = await createOrder();
    const max = cfg.webhookMaxAttempts;
    const failed = await waitFor(`order.confirmed esgotar ${max} tentativas`, async () => {
      const w = await webhooksOf(id);
      return w.deliveries.find((d) => d.eventType === 'order.confirmed' && d.status === 'failed');
    }, T(10 + 2 ** max + 2 * max));
    step(`Webhook order.confirmed retentado com backoff e marcado como falhou após ${failed.attempts} tentativas (último ${failed.lastStatusCode})`);
    const got503 = (await webhooksOf(id)).received.filter((x) => x.status === 503 && x.signatureValid).length;
    step(`O cliente recebeu ${got503} tentativas assinadas (todas respondidas 503)`);
    await chaos({ failWebhook: false });
    step('Falha desligada');
    const pending = (await webhooksOf(id)).deliveries.filter((d) => d.status === 'failed');
    for (const d of pending) await api('POST', `/webhooks/deliveries/${d.id}/retry`);
    step(`Reenvio manual de ${pending.length} webhook(s) que ficaram como falhou`);
    const final = await waitFor('todos os webhooks do pedido entregues', async () => {
      const w = await webhooksOf(id);
      const types = new Set(w.deliveries.map((d) => d.eventType));
      return types.has('order.completed') && w.deliveries.every((d) => d.status === 'delivered') ? w : null;
    }, T(60));
    const okReceived = final.received.filter((x) => x.status === 200 && x.signatureValid).map((x) => x.event);
    step(`Recuperou: cliente confirmou com 200 e assinatura válida (${[...new Set(okReceived)].join(', ')})`);
  });

  // Relatório
  console.log(`\n${color('1', 'Resultado')}`);
  for (const r of results) {
    console.log(`  ${r.ok ? ok('PASSOU') : bad('FALHOU')}  ${r.title} ${dim(`(${(r.ms / 1000).toFixed(1)}s)`)}${r.ok ? '' : `\n          ${bad(r.error)}`}`);
  }
  const failedCount = results.filter((r) => !r.ok).length;
  console.log(`\n${failedCount ? bad(`${failedCount} de ${results.length} falharam`) : ok(`${results.length} de ${results.length} passaram`)}. Todas as falhas foram desligadas.`);
  process.exit(failedCount ? 1 : 0);
}

process.on('SIGINT', async () => {
  await allOff().catch(() => undefined);
  console.log('\nInterrompido: falhas desligadas.');
  process.exit(130);
});

main().catch(async (err) => {
  await allOff().catch(() => undefined);
  console.error(bad(err.stack ?? String(err)));
  process.exit(1);
});
