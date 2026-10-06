import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { join } from 'node:path';
import { AppModule } from './app.module';
import { BufferedLogger } from './logging/log-buffer';

// JSON não sabe serializar bigint: centavos viram string na resposta.
(BigInt.prototype as unknown as { toJSON: () => string }).toJSON = function (
  this: bigint,
) {
  return this.toString();
};

async function bootstrap() {
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    logger: new BufferedLogger(),
    // Corpo cru em req.rawBody: o receptor de webhook confere a assinatura sobre ele.
    rawBody: true,
  });

  // Fecha relay, consumidores, producers e pool do Postgres no SIGTERM/SIGINT.
  app.enableShutdownHooks();

  const config = app.get(ConfigService);
  const port = config.get<number>('PORT', 3000);

  if (config.get<boolean>('DEBUG_PANEL')) {
    app.useStaticAssets(join(__dirname, '..', 'public'));
    Logger.log(`Painel: http://localhost:${port}/`, 'Bootstrap');
  }

  await app.listen(port);
  Logger.log(`API ouvindo na porta ${port}`, 'Bootstrap');
}

void bootstrap();
