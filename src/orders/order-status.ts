import type { Order } from '../db/schema';
import type { DeliveryStatus, PaymentStatus } from '../events/topics';

/**
 * Regras únicas de status expostas ao cliente (consulta, webhooks e relatório):
 *  - pagamento: PAID quando o consumidor confirmou o pedido;
 *  - entrega: SHIPPED quando a transportadora aceitou, DELIVERED quando avisou a entrega.
 */
export function paymentStatusOf(o: Pick<Order, 'confirmedAt'>): PaymentStatus {
  return o.confirmedAt ? 'PAID' : 'PENDING';
}

export function deliveryStatusOf(
  o: Pick<Order, 'shippedAt' | 'deliveredAt'>,
): DeliveryStatus {
  if (o.deliveredAt) return 'DELIVERED';
  if (o.shippedAt) return 'SHIPPED';
  return 'PENDING';
}
