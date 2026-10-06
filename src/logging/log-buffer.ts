import { ConsoleLogger, LogLevel } from '@nestjs/common';

export interface LogEntry {
  seq: number;
  at: string;
  level: LogLevel;
  context: string;
  message: string;
  stack?: string;
}

/**
 * Buffer circular com as últimas N linhas de log, lido pelo painel em /debug/logs.
 * É um singleton de processo porque o logger do Nest é criado antes do container de DI.
 */
export class LogBuffer {
  private readonly entries: LogEntry[] = [];
  private seq = 0;

  constructor(private readonly capacity = 300) {}

  push(entry: Omit<LogEntry, 'seq' | 'at'>) {
    this.entries.push({
      ...entry,
      seq: ++this.seq,
      at: new Date().toISOString(),
    });
    if (this.entries.length > this.capacity) this.entries.shift();
  }

  /** Retorna as entradas com seq > `after` (o painel faz polling incremental). */
  since(after = 0): LogEntry[] {
    return this.entries.filter((e) => e.seq > after);
  }

  clear() {
    this.entries.length = 0;
  }
}

export const logBuffer = new LogBuffer();

const IGNORED_CONTEXTS = new Set([
  'InstanceLoader',
  'RoutesResolver',
  'RouterExplorer',
  'NestFactory',
]);

/** Logger do Nest que imprime normalmente e também alimenta o LogBuffer. */
export class BufferedLogger extends ConsoleLogger {
  log(message: unknown, context?: string) {
    this.capture('log', message, context);
    super.log(message, context);
  }
  warn(message: unknown, context?: string) {
    this.capture('warn', message, context);
    super.warn(message, context);
  }
  debug(message: unknown, context?: string) {
    this.capture('debug', message, context);
    super.debug(message, context);
  }
  error(message: unknown, stack?: string, context?: string) {
    this.capture('error', message, context, stack);
    super.error(message, stack, context);
  }

  private capture(
    level: LogLevel,
    message: unknown,
    context?: string,
    stack?: string,
  ) {
    const ctx = context ?? this.context ?? 'App';
    if (IGNORED_CONTEXTS.has(ctx)) return;
    logBuffer.push({
      level,
      context: ctx,
      message: typeof message === 'string' ? message : JSON.stringify(message),
      stack,
    });
  }
}
