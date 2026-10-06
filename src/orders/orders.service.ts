import {
  ConflictException,
  Inject,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { eq } from 'drizzle-orm';
import { fromCents, toCents } from '../common/money';
import { DRIZZLE, type Database } from '../db/db.module';
import { orders, type Order } from '../db/schema';
import {
  EventTypes,
  Topics,
  type DeliveryStatus,
  type OrderCreatedPayload,
  type PaymentStatus,
} from '../events/topics';
import { buildOrderReport } from './order-report';
import { deliveryStatusOf, paymentStatusOf } from './order-status';
import { IdempotencyService } from '../idempotency/idempotency.service';
import { OutboxService } from '../outbox/outbox.service';
import { traceStore } from '../tracing/trace-store';
import type { CreateOrderDto } from './dto/create-order.dto';

export interface OrderView {
  id: string;
  customerEmail: string;
  amount: string;
  amountCents: string;
  status: Order['status'];
  paymentStatus: PaymentStatus;
  paidAt: Date | null;
  deliveryStatus: DeliveryStatus;
  shippedAt: Date | null;
  deliveredAt: Date | null;
  createdAt: Date;
  confirmedAt: Date | null;
}

export interface CreateResult {
  status: number;
  body: unknown;
}

export function toView(o: Order): OrderView {
  return {
    id: o.id,
    customerEmail: o.customerEmail,
    amount: fromCents(o.amountCents),
    amountCents: o.amountCents.toString(),
    status: o.status,
    // Pago = confirmado pelo consumidor. É assíncrono: o POST sempre responde
    // PENDING; o PAID chega pela consulta ou pelo webhook (como a pacs.002 ACSC no Pix).
    paymentStatus: paymentStatusOf(o),
    paidAt: o.confirmedAt,
    deliveryStatus: deliveryStatusOf(o),
    shippedAt: o.shippedAt,
    deliveredAt: o.deliveredAt,
    createdAt: o.createdAt,
    confirmedAt: o.confirmedAt,
  };
}

@Injectable()
export class OrdersService {
  private readonly logger = new Logger('OrdersService');

  constructor(
    @Inject(DRIZZLE) private readonly db: Database,
    private readonly outbox: OutboxService,
    private readonly idempotency: IdempotencyService,
  ) {}

  /**
   * Uma transação faz tudo ou nada:
   *   idempotency_keys + orders + outbox
   */
  async create(
    dto: CreateOrderDto,
    idempotencyKey: string,
  ): Promise<CreateResult> {
    const requestHash = IdempotencyService.hash(dto);
    const trace = traceStore.current();
    traceStore.step(
      trace,
      'transaction',
      'info',
      'BEGIN: transação aberta',
      `Corpo validado: ${dto.customerEmail}, R$ ${dto.amount}`,
    );

    const result = await this.db.transaction(async (tx) => {
      const idem = await this.idempotency.begin(
        tx,
        idempotencyKey,
        requestHash,
      );
      if (idem.state === 'REPLAY') {
        const originalId = (idem.body as Partial<OrderView> | null)?.id;
        traceStore.step(
          trace,
          'idempotency',
          'error',
          'Transação duplicada: chave já usada',
          `A chave já criou o pedido ${originalId ?? '?'}. ROLLBACK, nenhum pedido novo`,
        );
        traceStore.setOutcome(trace, 'duplicate');
        // Conta o reenvio (vai para o relatório final) e só lança DEPOIS do
        // commit: lançar aqui dentro desfaria a contagem.
        await this.idempotency.recordDuplicate(tx, idempotencyKey);
        return { duplicateOf: originalId ?? null };
      }
      traceStore.step(
        trace,
        'idempotency',
        'ok',
        'Chave nova reservada',
        'INSERT em idempotency_keys com status IN_PROGRESS',
      );

      const [order] = await tx
        .insert(orders)
        .values({
          customerEmail: dto.customerEmail,
          amountCents: toCents(dto.amount),
        })
        .returning();
      traceStore.link(trace, { orderId: order.id });
      traceStore.step(
        trace,
        'transaction',
        'ok',
        'Pedido inserido como PENDING',
        `orders.id = ${order.id}`,
      );

      const payload: OrderCreatedPayload = {
        orderId: order.id,
        customerEmail: order.customerEmail,
        amount: fromCents(order.amountCents),
        amountCents: order.amountCents.toString(),
      };

      const eventId = await this.outbox.add(tx, {
        aggregateType: 'order',
        aggregateId: order.id,
        eventType: EventTypes.OrderCreated,
        topic: Topics.OrderCreated,
        payload: { ...payload },
      });
      traceStore.link(trace, { eventId });
      traceStore.step(
        trace,
        'transaction',
        'ok',
        'Evento OrderCreated gravado na outbox',
        `outbox.id = ${eventId}, tópico ${Topics.OrderCreated}, status pending`,
      );

      const body = toView(order);
      await this.idempotency.complete(tx, idempotencyKey, 201, body);
      this.logger.log(
        `Pedido ${order.id.slice(0, 8)} criado (R$ ${body.amount}) + evento na outbox, na mesma transação`,
      );
      return { status: 201, body };
    });
    if ('duplicateOf' in result) {
      this.logger.error(
        `Transação duplicada: Idempotency-Key ${idempotencyKey} já criou o pedido ${result.duplicateOf?.slice(0, 8) ?? '?'}, nenhum pedido novo`,
      );
      throw new ConflictException({
        message: `Transação duplicada: a Idempotency-Key ${idempotencyKey} já foi usada para criar um pedido`,
        details: { idempotencyKey, originalOrderId: result.duplicateOf },
      });
    }
    traceStore.step(
      trace,
      'transaction',
      'ok',
      'COMMIT: pedido e evento gravados juntos',
      'A partir daqui o relay pode enxergar o evento',
    );
    return result;
  }

  /** Resultado da transação em passos numerados (o mesmo do webhook order.completed). */
  async report(id: string) {
    const report = await buildOrderReport(this.db, id);
    if (!report) throw new NotFoundException(`Pedido ${id} não encontrado`);
    return report;
  }

  async findOne(id: string): Promise<OrderView> {
    const [order] = await this.db
      .select()
      .from(orders)
      .where(eq(orders.id, id));
    if (!order) throw new NotFoundException(`Pedido ${id} não encontrado`);
    return toView(order);
  }
}
