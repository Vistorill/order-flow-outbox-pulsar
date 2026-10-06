/**
 * Roda ANTES de qualquer import dos testes. O ConfigModule.forRoot() lê o
 * ambiente no momento do import do AppModule, então as variáveis de teste
 * precisam existir antes disso (o .env não sobrescreve variáveis já definidas).
 */
process.env.NODE_ENV = 'test';
process.env.DATABASE_URL =
  process.env.DATABASE_URL_TEST ??
  'postgres://outbox:outbox@localhost:5445/outbox_test';
process.env.BROKER = 'memory';
process.env.OUTBOX_MAX_ATTEMPTS = '3';
process.env.OUTBOX_RELAY_AUTOSTART = 'false'; // os testes chamam tick() manualmente
process.env.WEBHOOK_DISPATCHER_AUTOSTART = 'false';
process.env.WEBHOOK_MAX_ATTEMPTS = '2';
process.env.CARRIER_API_URL = 'http://127.0.0.1:4599'; // transportadora falsa do teste
process.env.DEBUG_PANEL = 'true';
