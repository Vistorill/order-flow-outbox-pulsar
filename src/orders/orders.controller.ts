import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  Param,
  ParseUUIDPipe,
  Post,
  Res,
} from '@nestjs/common';
import type { Response } from 'express';
import { CreateOrderDto } from './dto/create-order.dto';
import { OrdersService } from './orders.service';

@Controller('orders')
export class OrdersController {
  constructor(private readonly orders: OrdersService) {}

  /**
   * Header obrigatório `Idempotency-Key`: reenviar a mesma requisição
   * (retry por timeout, duplo clique) é recusado com 409 sem criar outro pedido.
   */
  @Post()
  async create(
    @Headers('idempotency-key') key: string | undefined,
    @Body() dto: CreateOrderDto,
    @Res({ passthrough: true }) res: Response,
  ) {
    if (!key || key.trim().length < 8 || key.length > 128) {
      throw new BadRequestException(
        'Header Idempotency-Key é obrigatório (8 a 128 caracteres, ex.: um UUID)',
      );
    }
    const result = await this.orders.create(dto, key.trim());
    res.status(result.status);
    return result.body;
  }

  @Get(':id')
  findOne(@Param('id', new ParseUUIDPipe()) id: string) {
    return this.orders.findOne(id);
  }

  /** Status da transação e relatório numerado (funciona também no meio do fluxo). */
  @Get(':id/report')
  report(@Param('id', new ParseUUIDPipe()) id: string) {
    return this.orders.report(id);
  }
}
