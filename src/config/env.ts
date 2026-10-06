import { z } from 'zod';

/**
 * Todas as variáveis de ambiente são validadas na subida (fail fast).
 * O ConfigModule chama `validateEnv` depois de carregar o `.env`, então
 * nenhum valor é lido de `process.env` em tempo de import.
 */
const envSchema = z.object({
  NODE_ENV: z
    .enum(['development', 'test', 'production'])
    .default('development'),
  PORT: z.coerce.number().int().positive().default(3000),

  DATABASE_URL: z.string().min(1),

  // 'pulsar' em uso real; 'memory' roda sem broker externo (testes / demo offline)
  BROKER: z.enum(['pulsar', 'memory']).default('pulsar'),
  PULSAR_URL: z.string().default('pulsar://localhost:6650'),
  PULSAR_ADMIN_URL: z.string().default('http://localhost:8080'),

  OUTBOX_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(1000),
  OUTBOX_BATCH_SIZE: z.coerce.number().int().positive().default(50),
  OUTBOX_MAX_ATTEMPTS: z.coerce.number().int().positive().default(8),
  OUTBOX_LEASE_SECONDS: z.coerce.number().int().positive().default(15),
  OUTBOX_RETENTION_DAYS: z.coerce.number().int().positive().default(7),

  WEBHOOK_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(1000),
  WEBHOOK_BATCH_SIZE: z.coerce.number().int().positive().default(20),
  WEBHOOK_MAX_ATTEMPTS: z.coerce.number().int().positive().default(6),
  WEBHOOK_TIMEOUT_MS: z.coerce.number().int().positive().default(5000),

  // URL pública desta API: a transportadora chama de volta em /webhooks/inbound/carrier.
  // Padrão: http://localhost:PORT
  APP_PUBLIC_URL: z.url().optional(),
  // API da transportadora. Padrão: o simulador em http://localhost:PORT/partner
  CARRIER_API_URL: z.url().optional(),
  CARRIER_NAME: z.string().default('Transportadora Simulada'),
  // Segredo combinado com a transportadora para validar os webhooks dela.
  CARRIER_WEBHOOK_SECRET: z
    .string()
    .min(16)
    .default('whsec_carrier_demo_secret_123'),
  CARRIER_TIMEOUT_MS: z.coerce.number().int().positive().default(5000),
  // Só o simulador: quanto tempo a "entrega" leva até o webhook voltar.
  CARRIER_DELIVERY_DELAY_MS: z.coerce
    .number()
    .int()
    .nonnegative()
    .default(4000),

  // Painel e rotas /debug. Nunca habilitar em produção.
  DEBUG_PANEL: z
    .enum(['true', 'false'])
    .default('true')
    .transform((v) => v === 'true'),
});

export type Env = z.infer<typeof envSchema>;

export function validateEnv(raw: Record<string, unknown>): Env {
  const parsed = envSchema.safeParse(raw);
  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((i) => `  - ${i.path.join('.')}: ${i.message}`)
      .join('\n');
    throw new Error(`Variáveis de ambiente inválidas:\n${issues}`);
  }
  return parsed.data;
}
