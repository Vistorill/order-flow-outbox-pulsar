/**
 * Testes de integração contra um Postgres REAL (banco outbox_test) com o
 * broker em memória. Cada teste prova uma garantia do padrão.
 *
 * Rodar: pnpm test:e2e   (precisa do docker compose up -d)
 */
import { INestApplication } from '@nestjs/common';
import { SchedulerRegistry } from '@nestjs/schedule';
import { ConfigService } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import { eq, sql } from 'drizzle-orm';
import request from 'supertest';
import { randomUUID } from 'node:crypto';
import { createServer, type IncomingHttpHeaders, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { AppModule } from '../src/app.module';
import { ChaosService } from '../src/broker/chaos.service';
import { InMemoryBroker } from '../src/broker/in-memory.broker';
import { MessageBroker } from '../src/broker/message-broker';
import { AllExceptionsFilter } from '../src/common/http-exception.filter';
import { DRIZZLE, type Database } from '../src/db/db.module';
import {
  orders,
  outbox,
  processedEvents,
  shipments,
  webhookDeliveries,
} from '../src/db/schema';
import { EventTypes } from '../src/events/topics';
import { OutboxRelayService } from '../src/outbox/outbox-relay.service';
import { WebhookDispatcherService } from '../src/webhooks/webhook-dispatcher.service';
import { sign, verify } from '../src/webhooks/webhook-signature';

(BigInt.prototype as unknown as { toJSON: () => string }).toJSON = function (
  this: bigint,
) {
  return this.toString();
};

describe('Outbox / Inbox (e2e)', () => {
  let app: INestApplication;
  let db: Database;
  let relay: OutboxRelayService;
  let broker: InMemoryBroker;
  let chaos: ChaosService;

  /** API da transportadora falsa (porta fixa em test/setup-env.ts). */
  let carrier: Server;
  let carrierStatus = 202;
  const carrierCalls: { key: string; body: { reference: string } }[] = [];

  beforeAll(async () => {
    carrier = createServer((req, res) => {
      let raw = '';
      req.on('data', (c: Buffer) => (raw += c.toString()));
      req.on('end', () => {
        const key = req.headers['idempotency-key'] as string;
        carrierCalls.push({ key, body: JSON.parse(raw) });
        if (carrierStatus >= 300) return res.writeHead(carrierStatus).end();
        res.writeHead(202, { 'content-type': 'application/json' }).end(
          JSON.stringify({
            id: `shp_${key.slice(0, 8)}`, // mesma chave → mesma remessa
            trackingCode: 'BR123SP',
            status: 'ACCEPTED',
          }),
        );
      });
    });
    await new Promise<void>((r) => carrier.listen(4599, '127.0.0.1', r));

    const moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleRef.createNestApplication({ logger: false, rawBody: true });
    app.useGlobalFilters(new AllExceptionsFilter());
    app.enableShutdownHooks();
    await app.init();

    // Proteção: nunca truncar o banco de desenvolvimento.
    expect(app.get(ConfigService).get('DATABASE_URL')).toMatch(/_test/);

    db = app.get(DRIZZLE);
    relay = app.get(OutboxRelayService);
    broker = app.get(MessageBroker);
    chaos = app.get(ChaosService);
  });

  beforeEach(async () => {
    // Primeiro deixa o consumidor terminar o que sobrou do teste anterior,
    // senão o TRUNCATE disputa lock com a transação dele (deadlock).
    chaos.set({
      failPublish: false,
      failConsumer: false,
      crashAfterPublish: false,
    });
    carrierStatus = 202;
    await broker.idle(10_000);
    carrierCalls.length = 0;
    await db.execute(
      sql`TRUNCATE orders, outbox, processed_events, idempotency_keys, webhook_endpoints, inbound_webhooks CASCADE`,
    );
  });

  afterAll(async () => {
    await app.close();
    await new Promise((r) => carrier.close(r));
  });

  const createOrder = (
    key = randomUUID(),
    body = { customerEmail: 'a@b.com', amount: '10.50' },
  ) =>
    request(app.getHttpServer())
      .post('/orders')
      .set('Idempotency-Key', key)
      .send(body);

  const count = async (
    table: typeof orders | typeof outbox | typeof processedEvents,
  ) => {
    const [r] = await db.select({ n: sql<number>`count(*)::int` }).from(table);
    return r.n;
  };

  /** O evento OrderCreated (a outbox também recebe OrderConfirmed depois). */
  const createdEvent = async () => {
    const [e] = await db
      .select()
      .from(outbox)
      .where(eq(outbox.eventType, EventTypes.OrderCreated));
    return e;
  };

  /** Confirmações aplicadas pelo consumidor de pedidos (sem contar o envio). */
  const confirmations = async () => {
    const [r] = await db
      .select({ n: sql<number>`count(*)::int` })
      .from(processedEvents)
      .where(eq(processedEvents.consumer, 'order-confirmation'));
    return r.n;
  };

  it('grava pedido e evento juntos; o relay publica e o consumidor confirma', async () => {
    const res = await createOrder().expect(201);
    expect(res.body.amount).toBe('10.50');
    expect(res.body.amountCents).toBe('1050');
    // A confirmação é assíncrona: o POST nunca responde PAID.
    expect(res.body.paymentStatus).toBe('PENDING');
    expect(res.body.paidAt).toBeNull();

    const event = await createdEvent();
    expect(event.status).toBe('pending');
    expect(event.topic).toBe('order.created');
    expect(event.aggregateId).toBe(res.body.id);

    const run = await relay.tick();
    expect(run.published).toBe(1);
    await broker.idle();

    const [order] = await db
      .select()
      .from(orders)
      .where(eq(orders.id, res.body.id));
    expect(order.status).toBe('CONFIRMED');
    expect(await count(processedEvents)).toBe(1);

    // Depois do consumidor, a consulta mostra o pagamento.
    const view = await request(app.getHttpServer())
      .get(`/orders/${res.body.id}`)
      .expect(200);
    expect(view.body.paymentStatus).toBe('PAID');
    expect(view.body.paidAt).toBe(order.confirmedAt!.toISOString());
  });

  it('rollback desfaz pedido e evento (atomicidade)', async () => {
    const res = await request(app.getHttpServer())
      .post('/debug/atomicity-test')
      .expect(200);
    expect(res.body.atomic).toBe(true);
    expect(await count(orders)).toBe(0);
    expect(await count(outbox)).toBe(0);
  });

  describe('Idempotency-Key', () => {
    it('requisições simultâneas com a mesma chave criam UM pedido', async () => {
      const key = randomUUID();
      const responses = await Promise.all([
        createOrder(key),
        createOrder(key),
        createOrder(key),
      ]);
      const created = responses.filter((r) => r.status === 201);
      const duplicates = responses.filter((r) => r.status === 409);
      expect(created).toHaveLength(1);
      expect(duplicates).toHaveLength(2);
      for (const d of duplicates) {
        expect(d.body.message).toMatch(/Transação duplicada/);
        expect(d.body.details.originalOrderId).toBe(created[0].body.id);
      }
      expect(await count(orders)).toBe(1);
      expect(await count(outbox)).toBe(1);
    });

    it('mesma chave com outro corpo → 422', async () => {
      const key = randomUUID();
      await createOrder(key).expect(201);
      const res = await createOrder(key, {
        customerEmail: 'a@b.com',
        amount: '99.00',
      }).expect(422);
      expect(res.body.message).toMatch(/outro corpo/);
    });

    it('sem a chave → 400', async () => {
      await request(app.getHttpServer())
        .post('/orders')
        .send({ customerEmail: 'a@b.com', amount: '1.00' })
        .expect(400);
    });
  });

  it('validação devolve 400 com os campos inválidos', async () => {
    const res = await createOrder(randomUUID(), {
      customerEmail: 'x',
      amount: 10.5 as unknown as string,
    }).expect(400);
    const fields = (res.body.details as { field: string }[])
      .map((d) => d.field)
      .sort();
    expect(fields).toEqual(['amount', 'customerEmail']);
  });

  it('broker fora do ar: retenta com backoff e vai para FAILED após o máximo', async () => {
    await createOrder().expect(201);
    chaos.set({ failPublish: true });

    for (let i = 1; i <= 3; i++) {
      await relay.tick();
      const e = await createdEvent();
      expect(e.attempts).toBe(i);
      expect(e.lastError).toMatch(/Broker indisponível/);
      if (i < 3) {
        expect(e.status).toBe('pending');
        expect(e.availableAt.getTime()).toBeGreaterThan(Date.now()); // backoff
        await db.update(outbox).set({ availableAt: new Date() }); // "avança o relógio"
      } else {
        expect(e.status).toBe('failed'); // dead letter
      }
    }

    // FAILED não é mais coletado pelo relay
    chaos.set({ failPublish: false });
    expect((await relay.tick()).claimed).toBe(0);

    // ...até ser reenfileirado manualmente
    const e = await createdEvent();
    await request(app.getHttpServer())
      .post(`/debug/outbox/${e.id}/retry`)
      .expect(200);
    expect((await relay.tick()).published).toBe(1);
  });

  it('entrega duplicada é ignorada pelo consumidor (inbox)', async () => {
    await createOrder().expect(201);
    await relay.tick();
    await broker.idle();
    const e = await createdEvent();

    await request(app.getHttpServer())
      .post(`/debug/outbox/${e.id}/replay`)
      .expect(200);
    await request(app.getHttpServer())
      .post(`/debug/outbox/${e.id}/replay`)
      .expect(200);
    await broker.idle();

    expect(await count(processedEvents)).toBe(1);
    const stats = (await broker.stats()).find(
      (t) => t.topic === 'order.created',
    )!;
    expect(stats.subscriptions[0].acked).toBeGreaterThanOrEqual(3); // 3 entregas, 1 efeito
  });

  it('queda após publicar: lease expira, evento sai de novo, efeito continua único', async () => {
    await createOrder().expect(201);
    chaos.set({ crashAfterPublish: true });
    await relay.tick();
    await broker.idle();

    let e = await createdEvent();
    expect(e.status).toBe('processing'); // "morreu" antes de marcar
    chaos.set({ crashAfterPublish: false });

    // Lease ainda válido: o OrderCreated não é pego de novo
    // (o ciclo só leva o OrderConfirmed que o consumidor gravou).
    await relay.tick();
    expect((await createdEvent()).status).toBe('processing');
    // Lease expirado: outro ciclo recupera e publica de novo.
    await db
      .update(outbox)
      .set({ lockedUntil: new Date(Date.now() - 1000) })
      .where(eq(outbox.id, e.id));
    expect((await relay.tick()).published).toBe(1);
    await broker.idle();

    e = await createdEvent();
    expect(e.status).toBe('published');
    expect(e.attempts).toBe(2);
    expect(await confirmations()).toBe(1);
  });

  it('consumidor com erro: nack, reentrega e efeito aplicado uma vez', async () => {
    const res = await createOrder().expect(201);
    chaos.set({ failConsumer: true });
    await relay.tick();
    await new Promise((r) => setTimeout(r, 300));

    // Falhou: o rollback desfez também o registro de dedupe.
    expect(await count(processedEvents)).toBe(0);
    let [order] = await db
      .select()
      .from(orders)
      .where(eq(orders.id, res.body.id));
    expect(order.status).toBe('PENDING');

    chaos.set({ failConsumer: false });
    await broker.idle(); // espera a reentrega
    [order] = await db.select().from(orders).where(eq(orders.id, res.body.id));
    expect(order.status).toBe('CONFIRMED');
    expect(await count(processedEvents)).toBe(1);
  });

  it('dois relays concorrentes publicam cada evento exatamente uma vez (SKIP LOCKED)', async () => {
    await Promise.all(
      Array.from({ length: 40 }, () => createOrder().expect(201)),
    );

    const published: string[] = [];
    const spyBroker = {
      kind: 'memory',
      publish: async (_t: string, _p: unknown, o: { eventId: string }) => {
        await new Promise((r) => setTimeout(r, 2)); // simula latência de rede
        published.push(o.eventId);
      },
    } as unknown as MessageBroker;
    const config = app.get(ConfigService);
    const scheduler = new SchedulerRegistry();
    const relayA = new OutboxRelayService(
      db,
      spyBroker,
      chaos,
      scheduler,
      config,
    );
    const relayB = new OutboxRelayService(
      db,
      spyBroker,
      chaos,
      scheduler,
      config,
    );

    await Promise.all([
      relayA.tick(),
      relayB.tick(),
      relayA.tick(),
      relayB.tick(),
    ]);

    expect(published).toHaveLength(40);
    expect(new Set(published).size).toBe(40);
    const [r] = await db
      .select({ n: sql<number>`count(*)::int` })
      .from(outbox)
      .where(eq(outbox.status, 'published'));
    expect(r.n).toBe(40);
  });

  describe('Webhooks para o cliente final', () => {
    /** Servidor HTTP que faz o papel do cliente: guarda o que recebeu. */
    type Received = { headers: IncomingHttpHeaders; raw: string };
    let server: Server;
    let url: string;
    let received: Received[];
    let replyStatus: number;

    beforeEach(async () => {
      received = [];
      replyStatus = 200;
      server = createServer((req, res) => {
        let raw = '';
        req.on('data', (c: Buffer) => (raw += c.toString()));
        req.on('end', () => {
          received.push({ headers: req.headers, raw });
          res.writeHead(replyStatus).end('ok');
        });
      });
      await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
      url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/hook`;
    });

    afterEach(() => new Promise((r) => server.close(r)));

    const dispatcher = () => app.get(WebhookDispatcherService);
    const createEndpoint = async () => {
      const res = await request(app.getHttpServer())
        .post('/webhooks/endpoints')
        .send({ url })
        .expect(201);
      expect(res.body.secret).toMatch(/^whsec_/);
      return res.body as { id: string; secret: string };
    };
    const confirmOrder = async () => {
      const res = await createOrder().expect(201);
      await relay.tick();
      await broker.idle();
      return res.body.id as string;
    };

    it('pedido confirmado gera um webhook assinado, entregue uma única vez', async () => {
      const endpoint = await createEndpoint();
      const orderId = await confirmOrder();

      // Nasceu na transação do consumidor, ainda não enviado.
      let [d] = await db.select().from(webhookDeliveries);
      expect(d.status).toBe('pending');

      expect((await dispatcher().tick()).delivered).toBe(1);
      expect(received).toHaveLength(1);
      const { headers, raw } = received[0];
      expect(
        verify(
          endpoint.secret,
          {
            id: headers['webhook-id'] as string,
            timestamp: headers['webhook-timestamp'] as string,
            signature: headers['webhook-signature'] as string,
          },
          raw,
        ),
      ).toEqual({ ok: true });
      const body = JSON.parse(raw);
      expect(body.type).toBe('order.confirmed');
      expect(body.data).toMatchObject({
        orderId,
        status: 'CONFIRMED',
        paymentStatus: 'PAID',
      });
      expect(body.data.paidAt).toBe(body.data.confirmedAt);

      [d] = await db.select().from(webhookDeliveries);
      expect(d.status).toBe('delivered');
      const detail = await request(app.getHttpServer())
        .get(`/webhooks/deliveries/${d.id}`)
        .expect(200);
      expect(detail.body.attemptsLog).toHaveLength(1);
      expect(detail.body.attemptsLog[0].statusCode).toBe(200);

      // Evento entregue de novo pelo broker: o consumidor descarta, nenhum webhook novo.
      const e = await createdEvent();
      await request(app.getHttpServer())
        .post(`/debug/outbox/${e.id}/replay`)
        .expect(200);
      await broker.idle();
      await dispatcher().tick();
      expect(await db.select().from(webhookDeliveries)).toHaveLength(1);
      expect(received).toHaveLength(1);
    });

    it('cliente fora do ar: retenta com backoff, desiste no máximo e aceita reenvio manual', async () => {
      await createEndpoint();
      await confirmOrder();
      replyStatus = 500;

      expect((await dispatcher().tick()).failed).toBe(1);
      let [d] = await db.select().from(webhookDeliveries);
      expect(d.status).toBe('pending');
      expect(d.lastStatusCode).toBe(500);
      expect(d.nextAttemptAt.getTime()).toBeGreaterThan(Date.now()); // backoff

      await db.update(webhookDeliveries).set({ nextAttemptAt: new Date() });
      expect((await dispatcher().tick()).dead).toBe(1); // WEBHOOK_MAX_ATTEMPTS=2
      [d] = await db.select().from(webhookDeliveries);
      expect(d.status).toBe('failed');

      replyStatus = 200;
      await request(app.getHttpServer())
        .post(`/webhooks/deliveries/${d.id}/retry`)
        .expect(202);
      expect((await dispatcher().tick()).delivered).toBe(1);
      expect(received).toHaveLength(3);
      // Mesmo webhook-id em todas as tentativas: o cliente deduplica por ele.
      expect(new Set(received.map((r) => r.headers['webhook-id'])).size).toBe(
        1,
      );
    });

    /** Webhook de entrega como a transportadora mandaria. */
    const carrierWebhook = (
      shipmentId: string,
      orderId: string,
      secret = 'whsec_carrier_demo_secret_123',
    ) => {
      const event = {
        id: `evt_${randomUUID()}`,
        type: 'shipment.delivered',
        createdAt: new Date().toISOString(),
        data: {
          shipmentId,
          reference: orderId,
          trackingCode: 'BR123SP',
          deliveredAt: new Date().toISOString(),
          receivedBy: 'Portaria',
        },
      };
      const body = JSON.stringify(event);
      const ts = Math.floor(Date.now() / 1000);
      return () =>
        request(app.getHttpServer())
          .post('/webhooks/inbound/carrier')
          .set('content-type', 'application/json')
          .set('webhook-id', event.id)
          .set('webhook-timestamp', String(ts))
          .set('webhook-signature', sign(secret, event.id, ts, body))
          .send(body);
    };

    it('fluxo completo: transação → outbox → transportadora → webhook de entrega → cliente', async () => {
      await createEndpoint();
      const orderId = await confirmOrder();
      await relay.tick(); // publica OrderConfirmed
      await broker.idle(); // serviço de envio chama a transportadora

      const [confirmedEvt] = await db
        .select()
        .from(outbox)
        .where(eq(outbox.eventType, EventTypes.OrderConfirmed));
      expect(carrierCalls).toHaveLength(1);
      expect(carrierCalls[0].key).toBe(confirmedEvt.id); // Idempotency-Key = eventId
      expect(carrierCalls[0].body.reference).toBe(orderId);
      let [order] = await db.select().from(orders);
      expect(order.status).toBe('SHIPPED');
      const [shipment] = await db.select().from(shipments);
      expect(shipment.status).toBe('ACCEPTED');

      // A transportadora avisa a entrega.
      const send = carrierWebhook(shipment.externalId!, orderId);
      await send().expect(200, { received: true });
      [order] = await db.select().from(orders);
      expect(order.status).toBe('DELIVERED');

      // Reenvio do mesmo webhook: 200, sem reprocessar.
      await send().expect(200, { received: true, duplicate: true });

      await dispatcher().tick();
      const types = received.map((r) => JSON.parse(r.raw).type).sort();
      expect(types).toEqual([
        'order.completed',
        'order.confirmed',
        'order.delivered',
      ]);
    });

    it('relatório final: narra o fluxo, conta as duplicidades e vai no webhook order.completed', async () => {
      await createEndpoint();
      const key = randomUUID();
      const res = await createOrder(key).expect(201);
      await createOrder(key).expect(409); // requisição duplicada
      const orderId = res.body.id as string;

      await relay.tick();
      await broker.idle();
      // Broker entrega o mesmo OrderCreated de novo.
      const e = await createdEvent();
      await request(app.getHttpServer())
        .post(`/debug/outbox/${e.id}/replay`)
        .expect(200);
      await broker.idle();
      await relay.tick(); // OrderConfirmed → transportadora
      await broker.idle();

      const [shipment] = await db.select().from(shipments);
      const deliveredHook = carrierWebhook(shipment.externalId!, orderId);
      await deliveredHook().expect(200);
      await dispatcher().tick();

      const completed = received
        .map((r) => JSON.parse(r.raw))
        .find((b) => b.type === 'order.completed');
      expect(completed.data).toMatchObject({
        orderId,
        result: 'DELIVERED',
        paymentStatus: 'PAID',
        idempotencyKey: key,
        duplicates: {
          requests: 1,
          brokerRedeliveries: 1,
          carrierWebhooks: 0,
          total: 2,
        },
      });
      const lines = completed.data.lines as { n: number; text: string }[];
      expect(lines.map((l) => l.n)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
      expect(lines[0].text).toMatch(/enviou POST \/orders/);
      expect(lines[2].text).toMatch(/chegou de novo 1 vez.*409/);
      expect(lines[4].text).toMatch(/PAID.*mais 1 vez.*descartadas/);
      expect(lines[7].text).toMatch(/Resultado final: pedido DELIVERED e PAID/);
      expect(completed.data.summary).toMatch(
        /2 duplicidades, todas neutralizadas/,
      );

      // O painel (passo 11) segue contando o que chegar depois:
      // o mesmo aviso reenviado...
      await deliveredHook().expect(200, { received: true, duplicate: true });
      // ...e um aviso NOVO para a remessa já entregue não notifica de novo.
      await carrierWebhook(shipment.externalId!, orderId)().expect(200, {
        received: true,
        alreadyDelivered: true,
      });
      const journeys = await request(app.getHttpServer())
        .get('/debug/journeys')
        .expect(200);
      const step11 = journeys.body[0].steps[10];
      expect(step11.n).toBe(11);
      expect(step11.status).toBe('done');
      expect(step11.report.length).toBe(8);
      expect(step11.info['webhooks repetidos da transportadora']).toBe(1);
    });

    it('status consistente: PAID e DELIVERED na consulta, no relatório e em todos os webhooks', async () => {
      await createEndpoint();
      const orderId = await confirmOrder();
      await relay.tick();
      await broker.idle();
      const [shipment] = await db.select().from(shipments);
      await carrierWebhook(shipment.externalId!, orderId)().expect(200);
      await dispatcher().tick();

      const view = await request(app.getHttpServer())
        .get(`/orders/${orderId}`)
        .expect(200);
      expect(view.body).toMatchObject({
        status: 'DELIVERED',
        paymentStatus: 'PAID',
        deliveryStatus: 'DELIVERED',
      });
      const report = await request(app.getHttpServer())
        .get(`/orders/${orderId}/report`)
        .expect(200);
      expect(report.body).toMatchObject({
        result: 'DELIVERED',
        paymentStatus: 'PAID',
        deliveryStatus: 'DELIVERED',
      });

      const bodies = received.map((r) => JSON.parse(r.raw));
      for (const b of bodies) expect(b.data.paymentStatus).toBe('PAID');
      expect(
        bodies.find((b) => b.type === 'order.delivered').data.deliveryStatus,
      ).toBe('DELIVERED');
    });

    it('mesma URL não pode ser cadastrada duas vezes (aviso em dobro)', async () => {
      const first = await createEndpoint();
      const res = await request(app.getHttpServer())
        .post('/webhooks/endpoints')
        .send({ url })
        .expect(409);
      expect(res.body.details.existingEndpointId).toBe(first.id);
    });

    it('endpoint que não assinava order.completed: assina e reenvia o resultado final uma vez', async () => {
      const res = await request(app.getHttpServer())
        .post('/webhooks/endpoints')
        .send({ url, events: ['order.confirmed', 'order.delivered'] })
        .expect(201);
      const orderId = await confirmOrder();
      await relay.tick();
      await broker.idle();
      const [shipment] = await db.select().from(shipments);
      await carrierWebhook(shipment.externalId!, orderId)().expect(200);
      await dispatcher().tick();
      expect(
        received.some((r) => JSON.parse(r.raw).type === 'order.completed'),
      ).toBe(false);

      // O passo 11 explica o motivo e lista o endpoint a corrigir.
      const before = await request(app.getHttpServer()).get('/debug/journeys');
      const step11 = before.body[0].steps[10] as {
        status: string;
        fix: { endpoints: { id: string }[] };
      };
      expect(step11.status).toBe('warn');
      expect(step11.fix.endpoints.map((e) => e.id)).toEqual([res.body.id]);

      await request(app.getHttpServer())
        .patch(`/webhooks/endpoints/${res.body.id}`)
        .send({ events: ['*'] })
        .expect(200);
      const resend = () =>
        request(app.getHttpServer())
          .post(`/webhooks/orders/${orderId}/completed`)
          .expect(202);
      expect((await resend()).body.queued).toBe(1);
      expect((await resend()).body.queued).toBe(0); // id fixo: não duplica
      await dispatcher().tick();
      const final = received
        .map((r) => JSON.parse(r.raw))
        .filter((b) => b.type === 'order.completed');
      expect(final).toHaveLength(1);
      expect(final[0].data).toMatchObject({
        result: 'DELIVERED',
        paymentStatus: 'PAID',
      });
    });

    it('webhook da transportadora com assinatura errada → 401', async () => {
      const orderId = await confirmOrder();
      await relay.tick();
      await broker.idle();
      const [shipment] = await db.select().from(shipments);
      await carrierWebhook(
        shipment.externalId!,
        orderId,
        'whsec_outro_segredo_qualquer',
      )().expect(401);
      const [order] = await db.select().from(orders);
      expect(order.status).toBe('SHIPPED');
    });

    it('transportadora fora do ar: NACK, nova chamada com a mesma Idempotency-Key', async () => {
      await confirmOrder();
      carrierStatus = 503;
      await relay.tick();
      await new Promise((r) => setTimeout(r, 300));
      let [shipment] = await db.select().from(shipments);
      expect(shipment.status).toBe('REQUESTED');
      expect(shipment.lastStatusCode).toBe(503);

      carrierStatus = 202;
      await broker.idle(); // reentrega após o NACK
      [shipment] = await db.select().from(shipments);
      expect(shipment.status).toBe('ACCEPTED');
      expect(shipment.attempts).toBeGreaterThanOrEqual(2);
      expect(new Set(carrierCalls.map((c) => c.key)).size).toBe(1);
    });

    it('sem endpoint cadastrado não cria entrega', async () => {
      await confirmOrder();
      expect(await db.select().from(webhookDeliveries)).toHaveLength(0);
    });
  });

  it('health responde ok', async () => {
    const res = await request(app.getHttpServer()).get('/health').expect(200);
    expect(res.body).toEqual({ status: 'ok', database: true, broker: true });
  });
});
