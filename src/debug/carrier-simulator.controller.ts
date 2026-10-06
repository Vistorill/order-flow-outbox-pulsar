import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  Injectable,
  Logger,
  OnApplicationShutdown,
  Post,
  ServiceUnavailableException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { randomBytes, randomUUID } from 'node:crypto';
import { ChaosService } from '../broker/chaos.service';
import type { CreateShipmentRequest } from '../shipping/carrier.client';
import type { CarrierDeliveredEvent } from '../shipping/inbound-webhooks.controller';
import { sign } from '../webhooks/webhook-signature';

export interface SimCallback {
  attempt: number;
  at: string;
  statusCode: number | null;
  error: string | null;
  durationMs: number;
}

export interface SimShipment {
  id: string;
  trackingCode: string;
  status: 'ACCEPTED' | 'IN_TRANSIT' | 'DELIVERED';
  idempotencyKey: string;
  request: CreateShipmentRequest;
  /** Quantas vezes nossa API pediu esta remessa (retries com a mesma chave). */
  requests: number;
  createdAt: string;
  deliveredAt: string | null;
  webhook: CarrierDeliveredEvent | null;
  callbacks: SimCallback[];
}

const MAX_CALLBACKS = 5;

/**
 * SIMULA a API de uma transportadora, como se fosse outro sistema:
 *  - POST /partner/shipments: cria a remessa (idempotente pela Idempotency-Key) e responde 202;
 *  - depois de CARRIER_DELIVERY_DELAY_MS, "entrega" e manda o webhook
 *    shipment.delivered assinado para o callbackUrl, com retry se nossa API falhar.
 * Só existe com DEBUG_PANEL=true. Em produção, CARRIER_API_URL aponta para a real.
 */
@Injectable()
export class CarrierSimulator implements OnApplicationShutdown {
  private readonly logger = new Logger('Transportadora');
  private readonly shipments = new Map<string, SimShipment>();
  private readonly byKey = new Map<string, string>();
  private readonly timers = new Set<NodeJS.Timeout>();
  private readonly secret: string;
  private readonly delayMs: number;

  constructor(
    private readonly chaos: ChaosService,
    config: ConfigService,
  ) {
    this.secret = config.getOrThrow<string>('CARRIER_WEBHOOK_SECRET');
    this.delayMs = config.get<number>('CARRIER_DELIVERY_DELAY_MS', 4000);
  }

  create(key: string | undefined, body: CreateShipmentRequest) {
    if (this.chaos.get().failCarrierApi) {
      this.logger.error(
        'API da transportadora fora do ar (falha simulada): 503',
      );
      throw new ServiceUnavailableException(
        'Transportadora indisponível (falha simulada)',
      );
    }
    if (!key) throw new BadRequestException('Idempotency-Key obrigatória');
    if (!body?.reference || !body?.callbackUrl) {
      throw new BadRequestException('reference e callbackUrl são obrigatórios');
    }

    const known = this.byKey.get(key);
    if (known) {
      const s = this.shipments.get(known)!;
      s.requests++;
      this.logger.warn(
        `Pedido de remessa repetido (mesma Idempotency-Key): devolvendo ${s.id}, nada novo criado`,
      );
      return this.view(s);
    }

    const s: SimShipment = {
      id: `shp_${randomBytes(6).toString('hex')}`,
      trackingCode: `BR${Math.floor(1e8 + Math.random() * 9e8)}SP`,
      status: 'ACCEPTED',
      idempotencyKey: key,
      request: body,
      requests: 1,
      createdAt: new Date().toISOString(),
      deliveredAt: null,
      webhook: null,
      callbacks: [],
    };
    this.shipments.set(s.id, s);
    this.byKey.set(key, s.id);
    if (this.shipments.size > 100) {
      const oldest = this.shipments.keys().next().value as string;
      this.byKey.delete(this.shipments.get(oldest)!.idempotencyKey);
      this.shipments.delete(oldest);
    }
    this.logger.log(
      `Remessa ${s.id} aceita para o pedido ${body.reference.slice(0, 8)} (rastreio ${s.trackingCode}). Entrega em ${(this.delayMs / 1000).toFixed(0)}s`,
    );

    this.later(Math.round(this.delayMs / 3), () => {
      s.status = 'IN_TRANSIT';
    });
    this.later(this.delayMs, () => this.deliver(s));
    return this.view(s);
  }

  /** Painel: o "lado da transportadora". */
  list() {
    return [...this.shipments.values()].reverse().slice(0, 30);
  }

  private view(s: SimShipment) {
    return {
      id: s.id,
      trackingCode: s.trackingCode,
      status: s.status,
      reference: s.request.reference,
      estimatedDeliveryAt: new Date(
        new Date(s.createdAt).getTime() + this.delayMs,
      ).toISOString(),
    };
  }

  private deliver(s: SimShipment) {
    s.status = 'DELIVERED';
    s.deliveredAt = new Date().toISOString();
    s.webhook = {
      id: `evt_${randomUUID()}`,
      type: 'shipment.delivered',
      createdAt: s.deliveredAt,
      data: {
        shipmentId: s.id,
        reference: s.request.reference,
        trackingCode: s.trackingCode,
        deliveredAt: s.deliveredAt,
        receivedBy: 'Portaria',
      },
    };
    this.logger.log(
      `Remessa ${s.id} entregue. Avisando a loja em ${s.request.callbackUrl}`,
    );
    void this.callback(s);
  }

  /** Webhook de volta, assinado, com retry e backoff como faria um parceiro real. */
  private async callback(s: SimShipment) {
    const attempt = s.callbacks.length + 1;
    const body = JSON.stringify(s.webhook);
    const msgId = s.webhook!.id;
    const ts = Math.floor(Date.now() / 1000);
    const started = Date.now();
    let statusCode: number | null = null;
    let error: string | null = null;
    try {
      const res = await fetch(s.request.callbackUrl, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'webhook-id': msgId,
          'webhook-timestamp': String(ts),
          'webhook-signature': sign(this.secret, msgId, ts, body),
        },
        body,
        signal: AbortSignal.timeout(5000),
      });
      statusCode = res.status;
      if (!res.ok) error = `HTTP ${res.status}`;
    } catch (err) {
      error = (err as Error).message;
    }
    s.callbacks.push({
      attempt,
      at: new Date().toISOString(),
      statusCode,
      error,
      durationMs: Date.now() - started,
    });
    if (!error) return;
    if (attempt >= MAX_CALLBACKS) {
      this.logger.error(
        `Desistiu de avisar a entrega de ${s.id} após ${attempt} tentativas: ${error}`,
      );
      return;
    }
    const delay = 1000 * 2 ** (attempt - 1);
    this.logger.warn(
      `Aviso de entrega de ${s.id} falhou (${error}). Nova tentativa em ${delay / 1000}s`,
    );
    this.later(delay, () => void this.callback(s));
  }

  private later(ms: number, fn: () => void) {
    const t = setTimeout(() => {
      this.timers.delete(t);
      fn();
    }, ms);
    this.timers.add(t);
  }

  reset() {
    for (const t of this.timers) clearTimeout(t);
    this.timers.clear();
    this.shipments.clear();
    this.byKey.clear();
  }

  onApplicationShutdown() {
    for (const t of this.timers) clearTimeout(t);
    this.timers.clear();
  }
}

@Controller('partner')
export class CarrierSimulatorController {
  constructor(private readonly sim: CarrierSimulator) {}

  @Post('shipments')
  @HttpCode(202)
  create(
    @Headers('idempotency-key') key: string | undefined,
    @Body() body: CreateShipmentRequest,
  ) {
    return this.sim.create(key, body);
  }

  @Get('shipments')
  list() {
    return this.sim.list();
  }
}
