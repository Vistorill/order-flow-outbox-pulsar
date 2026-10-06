import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export interface CreateShipmentRequest {
  /** Nossa referência: a transportadora devolve no webhook de entrega. */
  reference: string;
  recipientEmail: string;
  declaredValue: string;
  /** Para onde a transportadora manda o webhook quando entregar. */
  callbackUrl: string;
}

export interface CarrierShipment {
  id: string;
  trackingCode: string;
  status: string;
  estimatedDeliveryAt?: string;
}

export interface CarrierCallResult {
  ok: boolean;
  url: string;
  statusCode: number | null;
  body: unknown;
  error: string | null;
  durationMs: number;
}

/** Cliente HTTP da API da transportadora (a "outra API" do fluxo). */
@Injectable()
export class CarrierClient {
  readonly name: string;
  readonly baseUrl: string;
  readonly callbackUrl: string;
  private readonly timeoutMs: number;

  constructor(config: ConfigService) {
    const port = config.get<number>('PORT', 3000);
    const publicUrl =
      config.get<string>('APP_PUBLIC_URL') ?? `http://localhost:${port}`;
    this.name = config.get<string>('CARRIER_NAME', 'Transportadora Simulada');
    this.baseUrl =
      config.get<string>('CARRIER_API_URL') ??
      `http://localhost:${port}/partner`;
    this.callbackUrl = `${publicUrl}/webhooks/inbound/carrier`;
    this.timeoutMs = config.get<number>('CARRIER_TIMEOUT_MS', 5000);
  }

  /**
   * POST /shipments com Idempotency-Key: se o consumidor reprocessar o mesmo
   * evento (retry, reentrega), a transportadora devolve a MESMA remessa em
   * vez de criar outra. É isso que torna seguro repetir uma chamada externa.
   */
  async createShipment(
    request: CreateShipmentRequest,
    idempotencyKey: string,
  ): Promise<CarrierCallResult> {
    const url = `${this.baseUrl}/shipments`;
    const started = Date.now();
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'idempotency-key': idempotencyKey,
        },
        body: JSON.stringify(request),
        signal: AbortSignal.timeout(this.timeoutMs),
      });
      const text = await res.text();
      let body: unknown = text;
      try {
        body = text ? JSON.parse(text) : null;
      } catch {
        /* resposta não-JSON: guarda o texto */
      }
      return {
        ok: res.ok,
        url,
        statusCode: res.status,
        body,
        error: res.ok ? null : `HTTP ${res.status}`,
        durationMs: Date.now() - started,
      };
    } catch (err) {
      const e = err as Error;
      return {
        ok: false,
        url,
        statusCode: null,
        body: null,
        error:
          e.name === 'TimeoutError'
            ? `Sem resposta em ${this.timeoutMs}ms (timeout)`
            : `${e.message}${e.cause instanceof Error ? `: ${e.cause.message}` : ''}`,
        durationMs: Date.now() - started,
      };
    }
  }
}
