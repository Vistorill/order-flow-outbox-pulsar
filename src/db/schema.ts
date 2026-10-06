import { sql } from 'drizzle-orm';
import {
  pgTable,
  uuid,
  text,
  bigint,
  integer,
  timestamp,
  jsonb,
  pgEnum,
  primaryKey,
  index,
  uniqueIndex,
  check,
  boolean,
} from 'drizzle-orm/pg-core';

const ts = (name: string) => timestamp(name, { withTimezone: true });

/* ---------------------------------------------------------------- orders */

export const orderStatus = pgEnum('order_status', [
  'PENDING',
  'CONFIRMED',
  'SHIPPED', // enviado à transportadora
  'DELIVERED', // transportadora avisou a entrega por webhook
]);

export const orders = pgTable(
  'orders',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    customerEmail: text('customer_email').notNull(),
    // Dinheiro sempre em centavos inteiros: nada de float.
    amountCents: bigint('amount_cents', { mode: 'bigint' }).notNull(),
    status: orderStatus('status').notNull().default('PENDING'),
    createdAt: ts('created_at').notNull().defaultNow(),
    confirmedAt: ts('confirmed_at'),
    shippedAt: ts('shipped_at'),
    deliveredAt: ts('delivered_at'),
  },
  // Defesa em profundidade: a regra também vale no banco.
  (t) => [check('ck_orders_amount_positive', sql`${t.amountCents} > 0`)],
);

/* ---------------------------------------------------------------- outbox */

export const outboxStatus = pgEnum('outbox_status', [
  'pending',
  'processing',
  'published',
  'failed',
]);

export const outbox = pgTable(
  'outbox',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    aggregateType: text('aggregate_type').notNull(),
    // Chave de ordenação: eventos do mesmo agregado mantêm a ordem no broker.
    aggregateId: text('aggregate_id').notNull(),
    eventType: text('event_type').notNull(),
    topic: text('topic').notNull(),
    payload: jsonb('payload').notNull(),
    status: outboxStatus('status').notNull().default('pending'),
    attempts: integer('attempts').notNull().default(0),
    availableAt: ts('available_at').notNull().defaultNow(),
    lockedUntil: ts('locked_until'),
    lastError: text('last_error'),
    createdAt: ts('created_at').notNull().defaultNow(),
    publishedAt: ts('published_at'),
  },
  (t) => [
    index('ix_outbox_pick').on(t.status, t.availableAt),
    check('ck_outbox_attempts', sql`${t.attempts} >= 0`),
  ],
);

/* ------------------------------------------------- inbox / dedupe consumer */

export const processedEvents = pgTable(
  'processed_events',
  {
    eventId: uuid('event_id').notNull(),
    consumer: text('consumer').notNull(),
    processedAt: ts('processed_at').notNull().defaultNow(),
    // Quantas cópias do mesmo evento o broker entregou depois (descartadas).
    duplicates: integer('duplicates').notNull().default(0),
  },
  (t) => [primaryKey({ columns: [t.eventId, t.consumer] })],
);

/* ------------------------------------------------------- idempotency keys */

export const idempotencyKeys = pgTable('idempotency_keys', {
  key: text('key').primaryKey(),
  requestHash: text('request_hash').notNull(),
  status: text('status', { enum: ['IN_PROGRESS', 'COMPLETED'] }).notNull(),
  responseStatus: integer('response_status'),
  responseBody: jsonb('response_body'),
  createdAt: ts('created_at').notNull().defaultNow(),
  // Reenvios da mesma requisição recusados com 409 (entram no relatório final).
  duplicateRequests: integer('duplicate_requests').notNull().default(0),
  lastDuplicateAt: ts('last_duplicate_at'),
});

/* --------------------------------------------------------------- webhooks */

/** Para onde o cliente final quer receber os avisos, e de quais eventos. */
export const webhookEndpoints = pgTable('webhook_endpoints', {
  id: uuid('id').primaryKey().defaultRandom(),
  url: text('url').notNull(),
  description: text('description'),
  // Chave do HMAC: o cliente usa para provar que o aviso veio de nós.
  secret: text('secret').notNull(),
  events: text('events')
    .array()
    .notNull()
    .default(sql`'{order.confirmed}'`),
  active: boolean('active').notNull().default(true),
  createdAt: ts('created_at').notNull().defaultNow(),
});

export const webhookDeliveryStatus = pgEnum('webhook_delivery_status', [
  'pending',
  'delivering',
  'delivered',
  'failed',
]);

/**
 * Uma entrega = um evento para um endpoint. Nasce na mesma transação que
 * gerou o evento (outbox de webhooks) e o dispatcher a envia com retry.
 */
export const webhookDeliveries = pgTable(
  'webhook_deliveries',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    endpointId: uuid('endpoint_id')
      .notNull()
      .references(() => webhookEndpoints.id, { onDelete: 'cascade' }),
    // Evento de origem: com o endpoint, impede entregar o mesmo evento duas vezes.
    eventId: uuid('event_id').notNull(),
    eventType: text('event_type').notNull(),
    payload: jsonb('payload').notNull(),
    status: webhookDeliveryStatus('status').notNull().default('pending'),
    attempts: integer('attempts').notNull().default(0),
    nextAttemptAt: ts('next_attempt_at').notNull().defaultNow(),
    lockedUntil: ts('locked_until'),
    lastStatusCode: integer('last_status_code'),
    lastError: text('last_error'),
    createdAt: ts('created_at').notNull().defaultNow(),
    deliveredAt: ts('delivered_at'),
  },
  (t) => [
    uniqueIndex('ux_webhook_deliveries_endpoint_event').on(
      t.endpointId,
      t.eventId,
    ),
    index('ix_webhook_deliveries_pick').on(t.status, t.nextAttemptAt),
  ],
);

/** Histórico de cada tentativa HTTP: o que foi enviado e o que voltou. */
export const webhookAttempts = pgTable(
  'webhook_attempts',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    deliveryId: uuid('delivery_id')
      .notNull()
      .references(() => webhookDeliveries.id, { onDelete: 'cascade' }),
    attempt: integer('attempt').notNull(),
    requestHeaders: jsonb('request_headers').notNull(),
    statusCode: integer('status_code'),
    responseBody: text('response_body'),
    error: text('error'),
    durationMs: integer('duration_ms').notNull(),
    createdAt: ts('created_at').notNull().defaultNow(),
  },
  (t) => [index('ix_webhook_attempts_delivery').on(t.deliveryId)],
);

/* ------------------------------------------------- integração transportadora */

export const shipmentStatus = pgEnum('shipment_status', [
  'REQUESTED', // tentando chamar a API da transportadora
  'ACCEPTED', // transportadora aceitou (202) e devolveu o id dela
  'DELIVERED', // webhook de entrega recebido
]);

/** Uma remessa por pedido: o que pedimos à API externa e o que ela respondeu. */
export const shipments = pgTable('shipments', {
  id: uuid('id').primaryKey().defaultRandom(),
  orderId: uuid('order_id')
    .notNull()
    .unique()
    .references(() => orders.id, { onDelete: 'cascade' }),
  carrier: text('carrier').notNull(),
  status: shipmentStatus('status').notNull().default('REQUESTED'),
  externalId: text('external_id'),
  trackingCode: text('tracking_code'),
  attempts: integer('attempts').notNull().default(0),
  requestBody: jsonb('request_body'),
  lastStatusCode: integer('last_status_code'),
  lastResponse: jsonb('last_response'),
  lastError: text('last_error'),
  requestedAt: ts('requested_at').notNull().defaultNow(),
  acceptedAt: ts('accepted_at'),
  deliveredAt: ts('delivered_at'),
});

/**
 * Webhooks que RECEBEMOS de parceiros. O par (origem, id do evento deles)
 * é único: o parceiro pode reenviar à vontade que processamos uma vez só.
 */
export const inboundWebhooks = pgTable(
  'inbound_webhooks',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    source: text('source').notNull(),
    externalEventId: text('external_event_id').notNull(),
    eventType: text('event_type').notNull(),
    headers: jsonb('headers').notNull(),
    payload: jsonb('payload').notNull(),
    receivedAt: ts('received_at').notNull().defaultNow(),
    // Reenvios do mesmo evento pelo parceiro (respondidos 200, ignorados).
    duplicates: integer('duplicates').notNull().default(0),
  },
  (t) => [
    uniqueIndex('ux_inbound_webhooks_source_event').on(
      t.source,
      t.externalEventId,
    ),
  ],
);

export type Order = typeof orders.$inferSelect;
export type Shipment = typeof shipments.$inferSelect;
export type WebhookEndpoint = typeof webhookEndpoints.$inferSelect;
export type WebhookDelivery = typeof webhookDeliveries.$inferSelect;
export type WebhookAttempt = typeof webhookAttempts.$inferSelect;
export type OutboxEvent = typeof outbox.$inferSelect;
export type NewOutboxEvent = typeof outbox.$inferInsert;
