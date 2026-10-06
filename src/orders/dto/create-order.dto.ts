import { createZodDto } from 'nestjs-zod';
import { z } from 'zod';
import { isValidDecimal } from '../../common/money';

/**
 * `amount` chega como STRING decimal ("199.90"), nunca como number:
 * evita que 0.1 + 0.2 vire 0.30000000000000004 antes de chegar ao banco.
 */
export const createOrderSchema = z.object({
  customerEmail: z.email('E-mail inválido'),
  amount: z
    .string('amount deve ser uma string decimal, ex.: "199.90"')
    .refine(isValidDecimal, 'Use até 2 casas decimais, ex.: "199.90"')
    .refine((v) => Number(v) > 0, 'amount deve ser maior que zero'),
});

export class CreateOrderDto extends createZodDto(createOrderSchema) {}
