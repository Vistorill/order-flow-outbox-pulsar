import {
  Controller,
  HttpCode,
  Logger,
  Post,
  Req,
  ServiceUnavailableException,
  UnauthorizedException,
  type RawBodyRequest,
} from '@nestjs/common';
import type { Request } from 'express';
import { ChaosService } from '../broker/chaos.service';
import { verify } from '../webhooks/webhook-signature';
import { WebhooksService } from '../webhooks/webhooks.service';

export interface ReceivedWebhook {
  seq: number;
  at: string;
  webhookId: string;
  event: string;
  attempt: string;
  signatureValid: boolean;
  /** Mesmo webhook-id já recebido antes: o cliente não deve processar de novo. */
  duplicate: boolean;
  status: number;
  reason?: string;
  headers: Record<string, string>;
  body: unknown;
}

/** Caixa de entrada do "cliente final" de demonstração, lida pelo painel. */
export class DemoInbox {
  private readonly items: ReceivedWebhook[] = [];
  private readonly seen = new Set<string>();
  private seq = 0;

  add(item: Omit<ReceivedWebhook, 'seq' | 'at' | 'duplicate'>) {
    const duplicate = item.signatureValid && this.seen.has(item.webhookId);
    if (item.signatureValid && item.status < 300) this.seen.add(item.webhookId);
    this.items.push({
      ...item,
      duplicate,
      seq: ++this.seq,
      at: new Date().toISOString(),
    });
    if (this.items.length > 50) this.items.shift();
  }

  list() {
    return [...this.items].reverse();
  }

  clear() {
    this.items.length = 0;
    this.seen.clear();
  }
}

export const demoInbox = new DemoInbox();

/**
 * Faz o papel do servidor do CLIENTE FINAL para a demo: recebe o webhook,
 * confere a assinatura e responde 200. Cadastre a URL
 * http://localhost:3000/demo-receiver como endpoint para ver o ciclo completo.
 *
 * Num cliente real o segredo fica guardado no servidor dele desde o cadastro;
 * aqui ele é buscado pelo webhook-id só porque os dois lados moram no mesmo app.
 */
@Controller('demo-receiver')
export class DemoReceiverController {
  private readonly logger = new Logger('ClienteFinal');

  constructor(
    private readonly webhooks: WebhooksService,
    private readonly chaos: ChaosService,
  ) {}

  @Post()
  @HttpCode(200)
  async receive(@Req() req: RawBodyRequest<Request>) {
    const header = (name: string) => req.header(name) ?? '';
    const headers = Object.fromEntries(
      [
        'content-type',
        'user-agent',
        'webhook-id',
        'webhook-timestamp',
        'webhook-signature',
        'webhook-event',
        'webhook-attempt',
      ].map((h) => [h, header(h)]),
    );
    const raw = req.rawBody?.toString('utf8') ?? JSON.stringify(req.body);
    const base = {
      webhookId: header('webhook-id'),
      event: header('webhook-event'),
      attempt: header('webhook-attempt'),
      headers,
      body: req.body as unknown,
    };

    // A assinatura é conferida sobre o corpo CRU, byte a byte, nunca sobre o JSON re-serializado.
    const secret = await this.webhooks.secretForDelivery(base.webhookId);
    const check = secret
      ? verify(
          secret,
          {
            id: base.webhookId,
            timestamp: header('webhook-timestamp'),
            signature: header('webhook-signature'),
          },
          raw,
        )
      : { ok: false as const, reason: 'webhook-id desconhecido' };

    if (!check.ok) {
      demoInbox.add({
        ...base,
        signatureValid: false,
        status: 401,
        reason: check.reason,
      });
      this.logger.error(`Webhook recusado: ${check.reason}`);
      throw new UnauthorizedException(check.reason);
    }

    if (this.chaos.get().failWebhook) {
      demoInbox.add({
        ...base,
        signatureValid: true,
        status: 503,
        reason: 'Cliente fora do ar (falha simulada)',
      });
      throw new ServiceUnavailableException(
        'Cliente fora do ar (falha simulada)',
      );
    }

    demoInbox.add({ ...base, signatureValid: true, status: 200 });
    const data = (req.body as { data?: { message?: string } })?.data;
    this.logger.log(
      `Webhook ${base.event} recebido e validado (assinatura ok): ${data?.message ?? ''}`,
    );
    return { received: true };
  }
}
