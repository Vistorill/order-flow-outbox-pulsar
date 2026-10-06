import { createZodDto } from 'nestjs-zod';
import { z } from 'zod';
import { ALL_WEBHOOK_EVENTS, WebhookEvents } from '../../events/topics';

const events = z
  .array(
    z.enum([
      WebhookEvents.OrderConfirmed,
      WebhookEvents.OrderDelivered,
      WebhookEvents.OrderCompleted,
      WebhookEvents.Test,
      ALL_WEBHOOK_EVENTS,
    ]),
  )
  .min(1, 'Assine pelo menos um evento');

export const createEndpointSchema = z.object({
  url: z.url({
    protocol: /^https?$/,
    error: 'URL inválida (use http:// ou https://)',
  }),
  description: z.string().max(200).optional(),
  // Padrão: todos os eventos, inclusive os que forem criados depois.
  events: events.default([ALL_WEBHOOK_EVENTS]),
});

export const updateEndpointSchema = z.object({
  url: createEndpointSchema.shape.url.optional(),
  description: z.string().max(200).nullable().optional(),
  events: events.optional(),
  active: z.boolean().optional(),
});

export class CreateEndpointDto extends createZodDto(createEndpointSchema) {}
export class UpdateEndpointDto extends createZodDto(updateEndpointSchema) {}
