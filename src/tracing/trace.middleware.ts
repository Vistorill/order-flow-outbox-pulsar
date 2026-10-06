import { Injectable, NestMiddleware } from '@nestjs/common';
import type { NextFunction, Request, Response } from 'express';
import { traceStore } from './trace-store';

/**
 * Abre um trace por requisição e o deixa no AsyncLocalStorage até a resposta.
 * Middleware do Nest (não `app.use`) porque roda depois do body parser:
 * antes dele o contexto assíncrono se perde na leitura do corpo.
 */
@Injectable()
export class TraceMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction) {
    const key = req.header('idempotency-key')?.trim() || undefined;
    const trace = traceStore.start(req.method, req.originalUrl, key);
    trace.request = req.body as unknown;
    traceStore.step(
      trace,
      'api',
      'info',
      'Requisição recebida',
      key ? `Idempotency-Key ${key}` : 'Sem Idempotency-Key',
    );

    const json = res.json.bind(res) as (body: unknown) => Response;
    res.json = (body: unknown) => {
      trace.response = body;
      return json(body);
    };

    const started = Date.now();
    res.on('finish', () => {
      trace.httpStatus = res.statusCode;
      trace.responseMs = Date.now() - started;
      const ok = res.statusCode < 400;
      traceStore.step(
        trace,
        'api',
        ok ? 'ok' : 'error',
        `Resposta ${res.statusCode} enviada ao cliente`,
        `${trace.responseMs}ms desde a chegada da requisição`,
      );
      if (trace.outcome === 'running') {
        traceStore.setOutcome(trace, ok ? 'waiting' : 'error');
      }
    });

    traceStore.run(trace, next);
  }
}
