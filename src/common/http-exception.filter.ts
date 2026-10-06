import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import type { Request, Response } from 'express';
import { ZodValidationException } from 'nestjs-zod';
import { ZodError } from 'zod';
import { traceStore } from '../tracing/trace-store';

export interface ErrorBody {
  statusCode: number;
  error: string;
  message: string;
  details?: unknown;
  path: string;
  method: string;
  timestamp: string;
}

/**
 * Todo erro sai no mesmo formato JSON, inclusive erros inesperados (500)
 * e erros de validação do Zod, para que clientes (e o painel) possam exibi-los.
 */
@Catch()
export class AllExceptionsFilter implements ExceptionFilter {
  private readonly logger = new Logger('HTTP');

  catch(exception: unknown, host: ArgumentsHost) {
    const ctx = host.switchToHttp();
    const req = ctx.getRequest<Request>();
    const res = ctx.getResponse<Response>();

    let status: number = HttpStatus.INTERNAL_SERVER_ERROR;
    let message = 'Erro interno';
    let details: unknown;

    if (exception instanceof ZodValidationException) {
      status = HttpStatus.BAD_REQUEST;
      message = 'Falha de validação';
      const zodError = exception.getZodError() as ZodError;
      details = zodError.issues.map((i) => ({
        field: i.path.join('.'),
        message: i.message,
      }));
    } else if (exception instanceof HttpException) {
      status = exception.getStatus();
      const body = exception.getResponse();
      if (typeof body === 'string') {
        message = body;
      } else {
        const b = body as { message?: string | string[]; details?: unknown };
        message = Array.isArray(b.message)
          ? b.message.join('; ')
          : (b.message ?? exception.message);
        details = b.details;
      }
    } else if (exception instanceof Error) {
      message = exception.message;
    }

    const body: ErrorBody = {
      statusCode: status,
      error: HttpStatus[status] ?? 'ERROR',
      message,
      details,
      path: req.url,
      method: req.method,
      timestamp: new Date().toISOString(),
    };

    const trace = traceStore.current();
    if (trace && trace.outcome === 'running') {
      traceStore.step(
        trace,
        'api',
        'error',
        `Erro ${status}: ${message}`,
        details === undefined ? undefined : JSON.stringify(details),
      );
    }

    // A resposta vai junto no log para aparecer em "Logs da aplicação" no painel.
    const line = `${req.method} ${req.url} → ${status} ${message} | resposta: ${JSON.stringify(body)}`;
    if (status >= 500) {
      this.logger.error(
        line,
        exception instanceof Error ? exception.stack : undefined,
      );
    } else if (status === 409) {
      // Transação duplicada: destacada como erro, não como aviso.
      this.logger.error(line);
    } else {
      this.logger.warn(line);
    }

    res.status(status).json(body);
  }
}
