# order-flow-outbox-pulsar

API em **NestJS** que demonstra o **Transactional Outbox Pattern** com **PostgreSQL** (Drizzle ORM) e **Apache Pulsar**, com consumidor idempotente (Inbox), `Idempotency-Key` na API, retry com backoff, dead letter e um **painel web** para acompanhar tudo em tempo real e injetar falhas.

![Painel com o broker fora do ar: eventos acumulando na outbox com erro e backoff](docs/painel.png)

---

## O problema

Ao criar um pedido, a aplicação precisa (1) gravar no banco e (2) avisar outros sistemas por um broker. Fazer as duas coisas separadamente gera inconsistência:

- grava e cai antes de publicar → **evento perdido**;
- publica e a transação dá rollback → **evento fantasma**.

Com o Outbox, o pedido e o evento são gravados **na mesma transação**. Um relay separado lê a tabela `outbox` e publica. A entrega é **at-least-once**, e o consumidor elimina duplicatas com a tabela `processed_events`.

## Arquitetura

```
POST /orders  (Idempotency-Key)
   │
   ▼  uma transação: idempotency_keys + orders + outbox
┌──────────────────────────────────────────────────────────┐
│ OutboxRelayService (a cada 1s)                            │
│  1. CLAIM   UPDATE ... SET status='processing',           │
│             locked_until=now()+lease  (FOR UPDATE SKIP     │
│             LOCKED, transação curta)                      │
│  2. PUBLISH fora de transação, key = aggregate_id         │
│  3. MARK    published | pending + backoff | failed (DLQ)  │
└──────────────────────────────────────────────────────────┘
   │
   ▼  Pulsar, tópico order.created, assinatura Key_Shared
OrderCreatedConsumer
   uma transação: INSERT processed_events ON CONFLICT DO NOTHING
                  + UPDATE orders SET status='CONFIRMED'
   duplicata → ignora e confirma (ack) · erro → rollback + nack → reentrega
```

### Fluxo ponta a ponta (11 passos)

Do pedido até a entrega na API da transportadora, o webhook de confirmação de entrega e o relatório final. Cada salto usa o mesmo princípio: **o próximo passo é gravado na mesma transação que conclui o anterior**, e toda chamada que pode ser repetida é idempotente.

```
 1. Cliente ──POST /orders (Idempotency-Key)──▶ API
 2. API ──uma transação──▶ orders (PENDING) + outbox (OrderCreated)
 3. Relay ──publica──▶ Pulsar  order.created
 4. Consumidor order-confirmation ──uma transação──▶ processed_events + orders (CONFIRMED)
 5.   ...na MESMA transação──▶ outbox (OrderConfirmed) + webhook_deliveries (order.confirmed)
 6. Relay ──publica──▶ Pulsar  order.confirmed
 7. Serviço de envio ──POST /shipments (Idempotency-Key = eventId)──▶ API da transportadora
        202 → shipments (ACCEPTED) + orders (SHIPPED)
        5xx/timeout → NACK → o Pulsar reentrega → nova chamada, mesma chave
 8. Transportadora entrega (do lado dela)
 9. Transportadora ──POST /webhooks/inbound/carrier (assinado HMAC)──▶ API
        assinatura ok → uma transação: inbound_webhooks (id único) + shipments/orders (DELIVERED)
                                       + webhook_deliveries (order.delivered)
10. Dispatcher ──POST assinado, retry com backoff──▶ endpoint do cliente final
11. Relatório final ──webhook order.completed──▶ cliente final
        frases numeradas: início da requisição, meio da transação, duplicidades, resultado
```

O **passo 11** resume o pedido em frases simples e numeradas e conta as duplicidades neutralizadas: requisições repetidas com a mesma chave (409), cópias de eventos entregues de novo pelo broker e avisos repetidos da transportadora. O mesmo relatório vai para o cliente no webhook `order.completed`, gravado na transação do passo 9:

```json
{
  "type": "order.completed",
  "data": {
    "orderId": "2e4b9ec0-…", "result": "DELIVERED", "paymentStatus": "PAID",
    "durationMs": 4800,
    "duplicates": { "requests": 1, "brokerRedeliveries": 0, "carrierWebhooks": 0, "total": 1 },
    "lines": [
      { "n": 1, "text": "Às 14:29:04 o cliente enviou POST /orders (R$ 150,00, cliente@loja.com) com a Idempotency-Key 80aae17e-…" },
      { "n": 2, "text": "Numa única transação, o pedido 2e4b9ec0 foi criado como PENDING e o evento OrderCreated foi gravado na outbox. …" },
      { "n": 3, "text": "A mesma requisição chegou de novo 1 vez com a mesma chave. Foi recusada com 409 (transação duplicada) …" },
      { "n": 8, "text": "Resultado final: pedido DELIVERED e PAID em 4,8s. Houve 1 duplicidade, neutralizada: um pedido, um pagamento, uma remessa." }
    ],
    "summary": "Pedido 2e4b9ec0 pago e entregue em 4,8s. Houve 1 duplicidade, neutralizada: …"
  }
}
```

Um aviso de entrega **novo** (outro id) para uma remessa que já está entregue responde `200` com `alreadyDelivered: true` e não notifica o cliente de novo.

No painel, a seção **Fluxo ponta a ponta** (no topo) mostra esses 11 passos numerados para o pedido escolhido, montados a partir do banco em tempo real: quem executa cada passo, quando aconteceu (e quanto depois do início), os ids envolvidos, a requisição e a resposta da transportadora e os headers do webhook recebido. O botão **Simular fluxo completo** cadastra o endpoint de demonstração do cliente (se não houver) e cria um pedido.

Por padrão a "outra API" é um simulador embutido (`/partner`, só com `DEBUG_PANEL=true`) que responde 202, espera `CARRIER_DELIVERY_DELAY_MS` e chama de volta com um webhook assinado com `CARRIER_WEBHOOK_SECRET`, retentando se a nossa API falhar. Para uma transportadora real, configure `CARRIER_API_URL` e `APP_PUBLIC_URL`.

### Garantias e como cada uma é implementada

| Garantia | Implementação | Teste |
|---|---|---|
| Pedido e evento são atômicos | `OrdersService.create` usa uma transação | `rollback desfaz pedido e evento` |
| Nenhum evento é perdido | Relay com lease: se cair depois de publicar, outro ciclo republica | `queda após publicar` |
| Efeito aplicado uma vez | `processed_events` + efeito na mesma transação | `entrega duplicada é ignorada` |
| Retry de cliente não duplica | `Idempotency-Key` com hash do corpo | `requisições simultâneas com a mesma chave` |
| Vários relays em paralelo | `FOR UPDATE SKIP LOCKED` | `dois relays concorrentes` |
| Falha persistente não trava a fila | Backoff exponencial com jitter + `failed` após N tentativas | `broker fora do ar` |
| Ordem por pedido | `partitionKey`/`orderingKey` = `aggregate_id` + `Key_Shared` | — (comportamento do Pulsar) |
| Dinheiro exato | Valor entra como string, guardado em centavos `bigint` | `money.spec.ts` |

## Como rodar

Pré-requisitos: Node.js 20+, pnpm e Docker.

```bash
pnpm install
cp .env.example .env          # Windows PowerShell: Copy-Item .env.example .env
docker compose up -d          # Postgres 17 + Pulsar 4.1 (o Pulsar leva ~30s para ficar pronto)
pnpm db:migrate
pnpm start:dev
```

Abra **http://localhost:3000/** para usar o painel.

### Sem Pulsar (modo memória)

Para rodar só com o Postgres, sem o container do Pulsar, defina `BROKER=memory` no `.env`. O broker em memória tem a mesma semântica de ack, nack e reentrega. É ele que os testes usam.

### Testes

```bash
pnpm test          # unitários (dinheiro, backoff, broker em memória)
pnpm test:e2e      # integração contra o Postgres do docker-compose (banco outbox_test)
```

Os testes e2e usam o banco `outbox_test`, criado automaticamente na primeira subida do volume (`docker/init-test-db.sql`). Por segurança, a suíte se recusa a rodar se a `DATABASE_URL` não terminar em `_test`.

### Testes de falha ao vivo (`pnpm test:chaos`)

Usa os mesmos interruptores do painel ("Injetar falhas") contra uma instância rodando. Para cada falha: liga, cria um pedido, confere o comportamento descrito, desliga e confere a recuperação. As falhas são desligadas no final mesmo se algo quebrar (inclusive com Ctrl+C).

| # | Cenário | O que é verificado |
|---|---|---|
| 0 | Caminho feliz | 11/11 passos; consulta com `PAID` e `DELIVERED` |
| 1 | Broker fora do ar | Evento fica na outbox com erro e backoff; pedido continua `PENDING`; ao desligar, publica e o pedido vira `PAID` |
| 2 | Consumidor quebrado | Relay publica, o broker reentrega (NACKs); pedido `PENDING` (rollback); ao desligar, `PAID` uma vez |
| 3 | Queda após publicar | Outbox presa em `processing`; lease expira, evento sai de novo; consumidor descarta a cópia |
| 4 | Transportadora fora do ar | 503 e NACK; pedido `PAID` sem remessa; ao desligar, várias chamadas com a mesma Idempotency-Key e **uma** remessa |
| 5 | Cliente do webhook fora do ar | Retentativas assinadas com backoff até `failed`; reenvio manual; cliente confirma com 200 e assinatura válida |

```bash
pnpm test:chaos                                   # contra http://localhost:3000
pnpm test:chaos -- --only broker,carrier          # só alguns: happy, broker, consumer, crash, carrier, webhook
```

**Rode com uma única instância apontando para o banco e para o Pulsar**: outra instância (com a falha desligada) processaria os pedidos e mascararia o teste. Para não interferir no app de desenvolvimento, use uma instância isolada com banco próprio e broker em memória (mais rápida com lease e tentativas menores):

```bash
docker exec outbox-postgres psql -U outbox -d outbox -c "CREATE DATABASE outbox_chaos OWNER outbox"
DATABASE_URL=postgres://outbox:outbox@localhost:5445/outbox_chaos pnpm db:migrate
pnpm build
PORT=3996 BROKER=memory DATABASE_URL=postgres://outbox:outbox@localhost:5445/outbox_chaos \
  OUTBOX_LEASE_SECONDS=5 WEBHOOK_MAX_ATTEMPTS=3 CARRIER_DELIVERY_DELAY_MS=1500 node dist/main
pnpm test:chaos -- --url http://localhost:3996
```

## O painel

| Área | O que mostra |
|---|---|
| Faixa superior | Os quatro estágios: API → Outbox → Broker → Consumidor, com contadores ao vivo. Fica vermelho se houver dead letter, evento pendente há mais de 10s ou tópico sem assinatura |
| Novo pedido | Formulário com `Idempotency-Key`, e botões que provocam cada tipo de resposta: envio duplo (409), chave reutilizada (422), sem chave (400), dados inválidos (400), 10 pedidos de uma vez |
| Respostas | Status e corpo JSON de cada requisição |
| Fluxo ponta a ponta | Os 11 passos numerados do pedido escolhido, da API até o relatório final com as duplicidades, com botão "Simular fluxo completo" (`GET /debug/journeys`) |
| Injetar falhas | Broker fora do ar, consumidor quebrado, queda após publicar, API da transportadora fora do ar, cliente do webhook fora do ar |
| Testes rápidos | Prova de atomicidade (rollback) e ciclo manual do relay |
| Abas | Outbox (status, tentativas, próxima tentativa, último erro, reenfileirar, duplicar entrega), pedidos, eventos processados, tópicos do broker |
| Aba Mensageria | Caminho de cada mensagem: relay reservou, producer enviou, broker gravou (messageId, ledger, entry, latência), entrega à assinatura, consumidor, ACK/NACK (`GET /debug/messaging`) |
| Webhook do cliente final | Cadastro de endpoint (já preenchido com o receptor de demonstração `/demo-receiver`) e o segredo gerado |
| Aba Webhooks | Endpoints (enviar teste, ativar/desativar, remover), cada entrega com headers, assinatura, payload e histórico de tentativas, e o que o cliente recebeu com a validação da assinatura |
| Aba Pulsar | Fluxo completo dentro do Pulsar com números ao vivo do admin REST: producers, tópico, ledgers do BookKeeper, assinatura Key_Shared, consumidores, cursor e DLQ (`GET /debug/pulsar`) |
| Logs | Logs da aplicação ao vivo, com filtro por nível |
| Requisições em tempo real | Linha do tempo de cada `POST /orders`: idempotência, transação, relay e consumidor, com o tempo de cada passo e a resposta enviada (`GET /debug/traces`) |

### Roteiro de demo (10 minutos)

0. **Fluxo completo.** Clique em "Simular fluxo completo" e narre os 11 passos enquanto acendem; para mostrar duplicidade, clique antes em "Enviar 2x com a mesma chave" e veja o passo 11 contar o 409. Em seguida, ligue "API da transportadora fora do ar", simule de novo e mostre o passo 7 em vermelho com as tentativas subindo; desligue e veja o fluxo terminar sem remessa duplicada.
1. **Caminho feliz.** Crie um pedido e veja o evento ir de `pending` para `published` e o pedido virar `confirmado`.
2. **Idempotência.** Clique em "Enviar 2x com a mesma chave": três respostas, um pedido.
3. **Broker fora do ar.** Ligue a falha, crie 10 pedidos e veja as tentativas e o backoff na outbox. Desligue e veja tudo drenar.
4. **Consumidor quebrado.** Ligue, crie um pedido e veja os nacks e reentregas nos logs. Desligue: o pedido é confirmado uma única vez.
5. **Queda após publicar.** Ligue, crie um pedido, desligue. Depois de 15s (o lease) o evento sai de novo e o log mostra "Duplicata ignorada".
6. **Atomicidade.** Clique em "Testar atomicidade": o rollback desfaz pedido e evento juntos.

Com o Pulsar real, você também pode rodar `docker stop outbox-pulsar` em vez de usar o botão de falha.

## API

### `POST /orders`

| Header | Obrigatório | Descrição |
|---|---|---|
| `Idempotency-Key` | sim | 8 a 128 caracteres, ex.: um UUID por intenção de compra |

```json
{ "customerEmail": "cliente@exemplo.com", "amount": "199.90" }
```

`amount` é **string** com até 2 casas decimais.

| Status | Quando |
|---|---|
| `201` | Pedido criado |
| `400` | Corpo inválido (lista os campos em `details`) ou header ausente |
| `409` | Transação duplicada: a chave já criou um pedido (`details.originalOrderId` aponta para ele) ou ainda está em andamento |
| `422` | Mesma chave com corpo diferente |

**Status do pagamento (`paymentStatus`).** A confirmação é assíncrona (outbox → Pulsar → consumidor), então o `POST` sempre responde `"paymentStatus": "PENDING"` e `"paidAt": null`. O `PAID` chega de duas formas:

- **Consulta:** `GET /orders/:id` passa a mostrar `"paymentStatus": "PAID"` e `paidAt` assim que o consumidor confirma o pedido (de `CONFIRMED` em diante).
- **Webhook:** o `order.confirmed` enviado ao cliente leva `"paymentStatus": "PAID"` e `paidAt`.

É o mesmo modelo do Pix: o pagador só vê "pago" depois da pacs.002 com status `ACSC`.

Todos os erros seguem o mesmo formato:

```json
{
  "statusCode": 400,
  "error": "BAD_REQUEST",
  "message": "Falha de validação",
  "details": [{ "field": "amount", "message": "Use até 2 casas decimais, ex.: \"199.90\"" }],
  "path": "/orders",
  "method": "POST",
  "timestamp": "2026-10-06T14:00:00.000Z"
}
```

### Webhooks para o cliente final

Quando o consumidor confirma um pedido, o cliente final recebe um `POST` na URL que cadastrou, com o evento `order.confirmed`.

**Garantias:**
- **Nada se perde nem sai sem motivo:** a entrega é gravada em `webhook_deliveries` na **mesma transação** que confirma o pedido. Rollback = nenhum aviso; commit = o aviso sai, mesmo se o app cair logo depois.
- **Um aviso por evento:** índice único `(endpoint_id, event_id)`. Se o broker reentregar o evento, nenhuma entrega nova é criada.
- **Retry:** 2xx é sucesso. Qualquer outra resposta, timeout ou erro de rede volta para a fila com backoff exponencial (1s, 2s, 4s...) até `WEBHOOK_MAX_ATTEMPTS`; depois fica `failed` até ser reenviado.
- **Histórico:** cada tentativa fica em `webhook_attempts` com os headers enviados, o status, a resposta (até 2000 caracteres), o erro e a duração.
- **Vários workers:** o dispatcher reserva com `FOR UPDATE SKIP LOCKED` e lease, como o relay.

**Cadastro:**

```http
POST /webhooks/endpoints
{ "url": "https://loja.exemplo.com/webhooks", "description": "Loja", "events": ["order.confirmed"] }
```

A resposta traz o `secret` (`whsec_...`) **uma única vez**. Nas listagens ele aparece mascarado.

- **Eventos:** sem `events`, o endpoint assina `["*"]`, ou seja, **todos os eventos, inclusive tipos criados depois** (como `order.completed`). Com uma lista fixa, um evento novo não chega até o endpoint assinar (`PATCH`).
- **Uma URL, um endpoint:** cadastrar de novo uma URL que já tem endpoint ativo responde `409` com `details.existingEndpointId`; senão o cliente receberia cada aviso em dobro.
- **Status em todos os avisos:** `order.confirmed`, `order.delivered` e `order.completed` trazem `paymentStatus` e `paidAt`; os de entrega trazem também `deliveryStatus`.

**O que o cliente recebe:**

```http
POST https://loja.exemplo.com/webhooks
content-type: application/json
webhook-id: 88cb082a-28d0-4409-b956-e5847b68fafd      ← igual em todos os retries: deduplique por ele
webhook-timestamp: 1791299947
webhook-signature: v1,hDv/kYtSZbCaDr4j0uuC1zRC7NKWYt2RDpGTvmA6N6w=
webhook-event: order.confirmed
webhook-attempt: 1

{
  "id": "60d3ba58-9fde-46c5-9fa0-a5e29c167161",
  "type": "order.confirmed",
  "createdAt": "2026-10-06T15:18:47.912Z",
  "data": {
    "orderId": "1e5377b1-7e1e-4707-a353-743c43547cd6",
    "customerEmail": "cliente@loja.com",
    "amount": "10.00",
    "amountCents": "1000",
    "status": "CONFIRMED",
    "paymentStatus": "PAID",
    "paidAt": "2026-10-06T15:18:47.905Z",
    "confirmedAt": "2026-10-06T15:18:47.905Z",
    "message": "Pedido 1e5377b1 pago e confirmado com sucesso: transação concluída."
  }
}
```

**Como o cliente valida** (formato [Standard Webhooks](https://www.standardwebhooks.com)):

```ts
import { createHmac, timingSafeEqual } from 'node:crypto';

// rawBody = corpo EXATAMENTE como chegou (não re-serialize o JSON)
const signed = `${headers['webhook-id']}.${headers['webhook-timestamp']}.${rawBody}`;
const expected = 'v1,' + createHmac('sha256', SECRET).update(signed).digest('base64');
const ok = headers['webhook-signature'].split(' ').some(
  (s) => s.length === expected.length && timingSafeEqual(Buffer.from(s), Buffer.from(expected)),
);
// Recuse também se |agora - webhook-timestamp| > 5 minutos (evita replay).
```

A mesma lógica está em `src/webhooks/webhook-signature.ts` (`verify`). O receptor de demonstração `POST /demo-receiver` usa essa função.

| Rota | Descrição |
|---|---|
| `POST /webhooks/endpoints` | Cadastra uma URL (responde o segredo) |
| `GET /webhooks/endpoints` | Lista (segredo mascarado) |
| `PATCH /webhooks/endpoints/:id` | Altera `url`, `description`, `events` ou `active` |
| `DELETE /webhooks/endpoints/:id` | Remove o endpoint e o histórico dele |
| `POST /webhooks/endpoints/:id/test` | Envia um `webhook.test` |
| `GET /webhooks/deliveries` | Últimas entregas |
| `GET /webhooks/deliveries/:id` | Entrega com todas as tentativas |
| `POST /webhooks/deliveries/:id/retry` | Reenvia agora (inclusive uma `failed`) |
| `POST /webhooks/orders/:orderId/completed` | Reenvia o resultado final de um pedido entregue, só para endpoints que ainda não o receberam |
| `POST /webhooks/inbound/carrier` | Recebe o `shipment.delivered` da transportadora (assinatura obrigatória) |

### Outras rotas

| Rota | Descrição |
|---|---|
| `GET /orders/:id` | Consulta um pedido: `status`, `paymentStatus` (`PENDING`/`PAID`) + `paidAt`, `deliveryStatus` (`PENDING`/`SHIPPED`/`DELIVERED`) + `shippedAt`/`deliveredAt` |
| `GET /orders/:id/report` | Status da transação e relatório em passos numerados (o mesmo do webhook `order.completed`); funciona também no meio do fluxo |
| `GET /health` | Estado do Postgres e do broker |
| `GET /debug/*`, `PUT /debug/chaos`, `POST /debug/*` | Rotas do painel (só com `DEBUG_PANEL=true`) |

`requests.http` tem todas as chamadas prontas para a extensão REST Client do VS Code.

## Variáveis de ambiente

Todas são validadas com Zod na subida, e um valor inválido impede a aplicação de iniciar.

| Variável | Padrão | Descrição |
|---|---|---|
| `DATABASE_URL` | — | Conexão do Postgres |
| `BROKER` | `pulsar` | `pulsar` ou `memory` |
| `PULSAR_URL` | `pulsar://localhost:6650` | Broker |
| `PULSAR_ADMIN_URL` | `http://localhost:8080` | Admin REST (estatísticas de tópicos) |
| `PORT` | `3000` | Porta HTTP |
| `OUTBOX_POLL_INTERVAL_MS` | `1000` | Intervalo do relay |
| `OUTBOX_BATCH_SIZE` | `50` | Eventos por lote |
| `OUTBOX_MAX_ATTEMPTS` | `8` | Tentativas antes de `failed` |
| `OUTBOX_LEASE_SECONDS` | `15` | Tempo até um evento `processing` ser considerado abandonado |
| `OUTBOX_RETENTION_DAYS` | `7` | Expurgo diário (03:00) de eventos publicados |
| `WEBHOOK_POLL_INTERVAL_MS` | `1000` | Intervalo do dispatcher de webhooks |
| `WEBHOOK_BATCH_SIZE` | `20` | Webhooks por lote (enviados em paralelo) |
| `WEBHOOK_MAX_ATTEMPTS` | `6` | Tentativas antes de `failed` |
| `WEBHOOK_TIMEOUT_MS` | `5000` | Tempo máximo esperando a resposta do cliente |
| `APP_PUBLIC_URL` | `http://localhost:PORT` | URL desta API que a transportadora chama de volta |
| `CARRIER_API_URL` | `http://localhost:PORT/partner` | API da transportadora (padrão: o simulador) |
| `CARRIER_NAME` | `Transportadora Simulada` | Nome mostrado no painel |
| `CARRIER_WEBHOOK_SECRET` | `whsec_carrier_demo_secret_123` | Segredo combinado para validar os webhooks da transportadora |
| `CARRIER_TIMEOUT_MS` | `5000` | Timeout da chamada à transportadora |
| `CARRIER_DELIVERY_DELAY_MS` | `4000` | Simulador: tempo até a "entrega" e o webhook de volta |
| `DEBUG_PANEL` | `true` | Painel e rotas `/debug`. **Desligue em produção** |

## Estrutura

```
src/
├── main.ts                        # logger com buffer, shutdown hooks, painel estático
├── app.module.ts
├── config/env.ts                  # validação das variáveis (Zod)
├── db/
│   ├── db.module.ts               # Drizzle + fechamento do pool
│   └── schema.ts                  # orders, outbox, processed_events, idempotency_keys
├── orders/                        # POST /orders, GET /orders/:id
├── idempotency/                   # Idempotency-Key
├── outbox/
│   ├── outbox.service.ts          # add(tx, evento): só existe dentro de transação
│   ├── outbox-relay.service.ts    # claim → publish → mark
│   ├── outbox-cleanup.service.ts  # expurgo diário
│   └── backoff.ts
├── broker/
│   ├── message-broker.ts          # interface
│   ├── pulsar.broker.ts           # Key_Shared, DLQ, cache de producer
│   ├── in-memory.broker.ts        # testes e modo offline
│   └── chaos.service.ts           # falhas simuladas
├── consumers/order-created.consumer.ts
├── events/topics.ts               # fonte única dos nomes de tópico
├── common/                        # dinheiro, filtro global de erros
├── logging/log-buffer.ts          # últimas linhas de log para o painel
├── debug/debug.controller.ts      # rotas do painel
└── health/health.controller.ts
public/index.html                  # painel (HTML + JS, sem build)
test/outbox.e2e-spec.ts            # 12 testes de integração
```

## Decisões e trade-offs

| Decisão | Alternativa | Por quê |
|---|---|---|
| Relay por polling | CDC com Debezium | Menos infraestrutura; latência de 1s é aceitável aqui. Em alto volume, CDC lê o WAL sem consultar a tabela |
| Claim com lease, publicar fora da transação | Publicar com a transação aberta | Não segura locks nem conexões durante a chamada de rede |
| At-least-once + consumidor idempotente | Exactly-once do broker | Exactly-once ponta a ponta não existe; a idempotência fica no consumidor |
| `Key_Shared` por `aggregate_id` | `Shared` | Paralelismo entre pedidos e ordem dentro do mesmo pedido |
| Valor como string, guardado em centavos `bigint` | `number` / `numeric` | Sem erro de ponto flutuante |
| Idempotency-Key gravada na mesma transação | Cache em memória ou Redis | A PK serializa requisições concorrentes com a mesma chave e sobrevive a restart |
| Interface `MessageBroker` | Pulsar direto nos serviços | Testes sem infraestrutura e troca de broker sem mexer no domínio |

## O que mudou em relação à versão anterior

| Problema na versão anterior | Correção |
|---|---|
| Produtor publicava em `OrderCreated` e o consumidor assinava `order.created`: nada era consumido | Nomes centralizados em `events/topics.ts` |
| Senha do `.env` diferente da do `docker-compose` | `.env.example` com as mesmas credenciais |
| Consumidor detectava duplicata mas não parava; logs com `'${}'` sem interpolar | `return` na duplicata, template strings |
| Intervalo do relay lido antes do `.env` carregar | Lido do `ConfigService` na inicialização |
| Producer com falha ficava em cache para sempre | Promise rejeitada sai do cache |
| Publicação com a transação aberta | Claim → publish → mark |
| `available_at` e `failed` nunca usados | Backoff, tentativas, dead letter e reenfileiramento |
| Sem ordem garantida | `ORDER BY created_at` + chave de ordenação + `Key_Shared` |
| `amount` como `number` | String decimal → centavos `bigint`, com `CHECK > 0` no banco |
| Sem `Idempotency-Key` | Implementada, com hash do corpo |
| Sem shutdown gracioso | Relay termina o lote; producers, consumidores e pool são fechados |
| Sem testes | 8 testes unitários + 12 de integração |
| Typos (`ordes`, `DRIZLE`, `Pulssar`, `POLI`) | Corrigidos |

## Próximos passos

- Trocar `orders` por pagamentos Pix: ledger de partidas dobradas, EndToEndId, mensagens pacs.008 / pacs.002 / pacs.004 e um simulador do SPI.
- Inbox persistente para mensagens externas (gravar o XML cru e processar depois).
- Métricas Prometheus (`outbox_oldest_pending_seconds`) e tracing com OpenTelemetry.
- CDC com Debezium como alternativa ao polling.
