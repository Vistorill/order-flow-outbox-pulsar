import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  Param,
  ParseUUIDPipe,
  Patch,
  Post,
  Query,
} from '@nestjs/common';
import { CreateEndpointDto, UpdateEndpointDto } from './dto/webhook.dto';
import { WebhooksService } from './webhooks.service';

const uuid = new ParseUUIDPipe();
const limitOf = (q?: string) => Math.min(Math.max(Number(q) || 50, 1), 500);

/**
 * API de webhooks: o cliente final cadastra uma URL e passa a receber
 * `order.confirmed` quando o pedido dele é confirmado.
 */
@Controller('webhooks')
export class WebhooksController {
  constructor(private readonly webhooks: WebhooksService) {}

  /** Responde o segredo completo UMA vez: guarde-o para validar a assinatura. */
  @Post('endpoints')
  createEndpoint(@Body() dto: CreateEndpointDto) {
    return this.webhooks.createEndpoint(dto);
  }

  @Get('endpoints')
  listEndpoints() {
    return this.webhooks.listEndpoints();
  }

  @Patch('endpoints/:id')
  updateEndpoint(
    @Param('id', uuid) id: string,
    @Body() dto: UpdateEndpointDto,
  ) {
    return this.webhooks.updateEndpoint(id, dto);
  }

  @Delete('endpoints/:id')
  deleteEndpoint(@Param('id', uuid) id: string) {
    return this.webhooks.deleteEndpoint(id);
  }

  @Post('endpoints/:id/test')
  @HttpCode(202)
  sendTest(@Param('id', uuid) id: string) {
    return this.webhooks.sendTest(id);
  }

  /** Reenvia o order.completed de um pedido entregue (só para quem ainda não recebeu). */
  @Post('orders/:orderId/completed')
  @HttpCode(202)
  resendCompleted(@Param('orderId', uuid) orderId: string) {
    return this.webhooks.resendCompleted(orderId);
  }

  @Get('deliveries')
  listDeliveries(@Query('limit') limit?: string) {
    return this.webhooks.listDeliveries(limitOf(limit));
  }

  /** Entrega com o histórico de tentativas (headers enviados, status, resposta). */
  @Get('deliveries/:id')
  getDelivery(@Param('id', uuid) id: string) {
    return this.webhooks.getDelivery(id);
  }

  @Post('deliveries/:id/retry')
  @HttpCode(202)
  retry(@Param('id', uuid) id: string) {
    return this.webhooks.retry(id);
  }
}
