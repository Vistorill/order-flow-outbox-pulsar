-- Executado só na primeira subida do volume: cria o banco usado por `pnpm test:e2e`.
CREATE DATABASE outbox_test OWNER outbox;
