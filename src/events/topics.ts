/**
 * Fonte única dos nomes de tópico.
 * No projeto original o produtor publicava em "OrderCreated" e o consumidor
 * assinava "order.created": dois tópicos diferentes, e nada era consumido.
 * Com os nomes centralizados aqui, esse erro não compila mais.
 */
export const Topics = {
  OrderCreated: 'order.created',
  /** Pedido confirmado: o serviço de envio assina e chama a transportadora. */
  OrderConfirmed: 'order.confirmed',
} as const;

export type Topic = (typeof Topics)[keyof typeof Topics];

export const EventTypes = {
  OrderCreated: 'OrderCreated',
  OrderConfirmed: 'OrderConfirmed',
} as const;

export interface OrderCreatedPayload {
  orderId: string;
  customerEmail: string;
  amount: string; // decimal em string, ex.: "199.90"
  amountCents: string; // bigint serializado
}

export interface OrderConfirmedPayload {
  orderId: string;
  customerEmail: string;
  amount: string;
  amountCents: string;
}

/** Eventos que o cliente final pode assinar por webhook. */
export const WebhookEvents = {
  /** Pedido confirmado pelo consumidor: a transação terminou com sucesso. */
  OrderConfirmed: 'order.confirmed',
  /** Transportadora confirmou a entrega: fim do fluxo. */
  OrderDelivered: 'order.delivered',
  /** Resultado final: relatório numerado do fluxo, com as duplicidades. */
  OrderCompleted: 'order.completed',
  /** Disparado pelo botão "Enviar teste" para validar a integração. */
  Test: 'webhook.test',
} as const;

export type WebhookEventType =
  (typeof WebhookEvents)[keyof typeof WebhookEvents];

/** Status do pagamento exposto ao cliente: PAID quando o pedido é confirmado. */
export type PaymentStatus = 'PENDING' | 'PAID';
/** Status da entrega: SHIPPED quando a transportadora aceitou, DELIVERED quando entregou. */
export type DeliveryStatus = 'PENDING' | 'SHIPPED' | 'DELIVERED';

/** Endpoint com este evento recebe todos, inclusive tipos criados no futuro. */
export const ALL_WEBHOOK_EVENTS = '*';

export interface OrderConfirmedWebhookData {
  orderId: string;
  customerEmail: string;
  amount: string;
  amountCents: string;
  status: 'CONFIRMED';
  paymentStatus: 'PAID';
  paidAt: string;
  deliveryStatus: DeliveryStatus;
  confirmedAt: string;
  message: string;
}

export interface OrderDeliveredWebhookData {
  orderId: string;
  status: 'DELIVERED';
  paymentStatus: PaymentStatus;
  paidAt: string | null;
  deliveryStatus: 'DELIVERED';
  carrier: string;
  trackingCode: string | null;
  deliveredAt: string;
  receivedBy: string | null;
  message: string;
}
