# Processamento Distribuído de Apostas em Go — Jungle Gaming

Solução do desafio técnico para a vaga de Backend Developer Júnior. Aplicação desenvolvida em Go utilizando Uber Fx, PostgreSQL (com `sqlc` e `pgx/v5`), AWS SQS (LocalStack) e Keycloak (OAuth 2.0 / OIDC).

> Para consultar o enunciado original do desafio, veja [CHALLENGE.md](./CHALLENGE.md).
> Para detalhes arquiteturais e justificativas técnicas, veja [ARCHITECTURE.md](./ARCHITECTURE.md).

## 🚀 Como Executar o Projeto

### Pré-requisitos
- Docker e Docker Compose
- Go 1.26.1 (para execução nativa; mesma versão de `go.mod` e `Dockerfile`)
- k6 (somente para testes de carga)
- sqlc (opcional; necessário apenas para regenerar código após alterar queries)

### Ambiente Local

Na primeira execução, crie o arquivo local de configuração:

```bash
cp -n .env.example .env
```

O `.env` é carregado automaticamente pelo Compose, pela aplicação Go, pelos testes de integração e pelo script do k6. As URLs usam `localhost` porque a API, os testes, as migrations e o k6 são executados no host; os nomes de serviço do Compose são usados apenas na comunicação interna entre containers. Não sobrescreva um `.env` existente se já tiver configurações locais.

Suba os serviços e execute a checagem de build/testes:

```bash
docker compose up --build -d
```

O Compose inicia PostgreSQL, LocalStack/SQS e Keycloak, e provisiona as filas. O serviço `app-check` executa `go test ./...` no container e termina; isso é esperado. Ele não inicia a API HTTP, que será executada no host com `go run .`.

Serviços locais:

| Serviço | Endereço/identificador | Credenciais locais |
| --- | --- | --- |
| PostgreSQL | `localhost:5432`, banco `wagering` | `wagering` / `local-wagering-password` |
| SQS (LocalStack) | `http://localhost:4566`, região `us-east-1` | chave de acesso `test`, chave secreta `test` |
| Keycloak | `http://localhost:8081`, realm `wagering-local` | `admin` / `local-admin-password` |

As credenciais OAuth 2.0 `client_credentials` de teste são `provider-alpha` / `local-provider-alpha-secret`, `provider-beta` / `local-provider-beta-secret` e `internal-service` / `local-internal-service-secret`. Os clientes de provedor recebem `provider_id` distinto e todos recebem a audiência `wagering-api`.

Exemplo para obter um token de provedor:

```bash
curl --fail-with-body -X POST \
	http://localhost:8081/realms/wagering-local/protocol/openid-connect/token \
	-H 'Content-Type: application/x-www-form-urlencoded' \
	-d 'grant_type=client_credentials' \
	-d 'client_id=provider-alpha' \
	-d 'client_secret=local-provider-alpha-secret'
```

Para o token interno, use o mesmo endpoint com `client_id=internal-service` e `client_secret=local-internal-service-secret`.

Filas provisionadas automaticamente:

- `wager-transactions.fifo`: fila de entrada, tempo de visibilidade de 60 segundos e encaminhamento para a DLQ após 5 recebimentos.
- `wager-transactions-dlq.fifo`: DLQ FIFO, configurada para aceitar encaminhamento somente da fila de entrada.
- `wager-events.fifo`: destino FIFO dos eventos confirmados pela outbox; consumidores devem deduplicar pelo `eventId`.

Para operações de entrada, use `MessageGroupId` igual ao ID estável da carteira para serializar somente operações da mesma carteira. Use `MessageDeduplicationId` estável da operação/mensagem; a deduplicação FIFO é complementar à idempotência persistida no banco. Para eventos, use o ID do agregado como `MessageGroupId` e o `eventId` imutável como `MessageDeduplicationId`.

LocalStack aceita as credenciais fictícias acima e simula a API SQS local; isso não configura uma fronteira IAM de produção. Em produção, restrinja os consumidores a `ReceiveMessage`, `DeleteMessage` e `ChangeMessageVisibility` na fila de entrada, e os publicadores a `SendMessage` somente no destino de eventos. A aplicação não deve receber permissões de administração de filas. Não reutilize nenhuma credencial local fora do ambiente de desenvolvimento.

Comandos úteis:

```bash
docker compose ps
docker compose exec localstack awslocal sqs list-queues --region us-east-1
docker compose logs localstack-init
sqlc generate # gera o pacote db/ a partir de db/schema.sql e db/query.sql
```

### Banco e migrações

O valor monetário é persistido como unidades mínimas (`int64`/`BIGINT`) e a moeda permanece em uma coluna `CHAR(3)` separada; nenhuma conversão por ponto flutuante ocorre no banco ou no domínio. O DDL canônico está em `db/schema.sql` e as queries tipadas em `db/query.sql`.

As migrações versionadas usam `psql` dentro do container PostgreSQL, sem exigir `psql` instalado no host nem variáveis exportadas. O diretório `db/` é montado somente para leitura no container:

```bash
sh scripts/migrate.sh up
```

Para reverter a migration (operação destrutiva que remove as tabelas), execute `sh scripts/migrate.sh down`. Não rode o comando `down` durante a inicialização normal.

O teste de atomicidade usa PostgreSQL real e pode ser executado com:

```bash
go test ./repository -run TestStoreInTxRollsBackAllChangesOnCallbackError -v
```

Para consultar o estado, use `\dt` no `psql`. A reversão remove todas as tabelas desta migração; em ambientes compartilhados, faça backup e aplique somente após confirmar que não há dados a preservar. Os escritores financeiros devem abrir uma transação `pgx`, usar `repository.Store.QueriesInTx` e confirmar carteira, ledger, inbox e outbox juntos. `GetWalletForUpdate` fornece o bloqueio pessimista por carteira; o ledger possui um gatilho no banco que rejeita `UPDATE` e `DELETE`.

A estrutura de banco de dados está centralizada em `db/`:
- `db/schema.sql`: DDL do schema PostgreSQL consumido pelo `sqlc`.
- `db/query.sql`: queries SQL explícitas anotadas para geração de código.
- `db/*.go`: código Go com segurança de tipos gerado pelo `sqlc` (`package db`).

O Dockerfile valida o código Go com Go 1.26.1. O ambiente de dependências pode ser encerrado com `docker compose down`; para também apagar os dados locais, use `docker compose down -v`.

### Executar a API HTTP

Depois de subir os containers e aplicar a migration, inicie a API em um terminal:

```bash
go run .
```

Em outro terminal, confirme que a API e as dependências estão prontas com `curl --fail http://localhost:8080/health/ready`. A aplicação lê `.env` automaticamente; variáveis já definidas no processo têm precedência.

`go run .` inicia a aplicação Fx, o servidor HTTP, o pool de conexões PostgreSQL, o worker de referências, o consumidor de transações SQS e o publicador da outbox. O consumidor busca mensagens por long polling em `wager-transactions.fifo`; o publicador envia os registros confirmados da outbox para `wager-events.fifo`. A descoberta OIDC é carregada na inicialização, e a verificação de prontidão consulta o PostgreSQL e as duas filas SQS. Use uma URL do emissor acessível pela API e idêntica à declaração `iss` do token.

Os envelopes SQS usam `messageId` como identidade durável da inbox. O consumidor valida o envelope e o formato decimal estrito dos valores monetários, calcula o hash do corpo recebido para verificar reentregas na inbox e, em seguida, chama o mesmo serviço de apostas e usa o mesmo hash de negócio canônico da API HTTP. O registro e a conclusão da mensagem na inbox, o resultado da aposta, o ledger, a carteira e os registros da outbox são confirmados juntos. A mensagem só é excluída após essa confirmação. Rejeições de negócio definitivas são reconhecidas; erros transitórios recebem atrasos exponenciais de visibilidade, enquanto falhas permanentes ou causadas por mensagens malformadas são tentadas novamente imediatamente, conforme a política da fila que as encaminha à DLQ após cinco recebimentos. O tempo de visibilidade é de 60 segundos. No encerramento, o consumidor para de buscar mensagens, aguarda os processamentos ativos e libera as mensagens recebidas cujo processamento ainda não começou.

O publicador da outbox reivindica registros usando `FOR UPDATE SKIP LOCKED` do PostgreSQL; reivindicações com mais de um minuto podem ser recuperadas. A publicação é feita pelo menos uma vez, usando o `eventId` estável como `MessageDeduplicationId` FIFO e o ID do agregado como `MessageGroupId`. Se houver uma falha após `SendMessage`, mas antes de marcar o registro como publicado, o mesmo evento poderá ser enviado novamente. Por isso, os consumidores posteriores devem deduplicar pelo `eventId`. Envios com falha são reagendados com atraso exponencial, limitado a 15 minutos. O publicador expõe o atraso observado mais recente por meio de `LastPublishedLag` e registra `outboxLagMilliseconds` nos logs estruturados.

Execute o teste de janela de falha com PostgreSQL e LocalStack reais após aplicar a migração e iniciar as dependências locais:

```bash
go test ./messaging -run '^TestLocalStackCommitBeforeDeleteRedeliveryAndHTTPReplay$' -count=1 -v
```

Obtenha tokens locais usando o exemplo de `client_credentials` acima e, em seguida, chame a API:

```bash
curl -i -X POST http://localhost:8080/wallets \
	-H "Authorization: Bearer $INTERNAL_TOKEN" -H 'Content-Type: application/json' \
	-d '{"playerId":"player-1","initialBalance":{"amount":"100.00","currency":"BRL"}}'

curl -i -X POST http://localhost:8080/wagering/transactions \
	-H "Authorization: Bearer $PROVIDER_TOKEN" -H 'Idempotency-Key: provider-alpha:bet-1' \
	-H 'Content-Type: application/json' \
	-d '{"providerId":"provider-alpha","externalTransactionId":"bet-1","playerId":"player-1","walletId":"WALLET_ID","roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

Rotas internas: `POST /wallets`, `GET /wallets/{walletId}`, `GET /wallets/{walletId}/ledger` e `POST /wallets/{walletId}/reconciliation`. Rotas de provedores: `POST /wagering/transactions`, `GET /wagering/transactions/{transactionId}` e `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}`. A paginação do ledger retorna 50 registros por padrão; `limit` aceita valores de 1 a 100 e `nextCursor` é opaco. `/health/live` e `/health/ready` são públicos.

Mapeamento dos códigos HTTP: `400` para entrada inválida ou ausência da chave de idempotência; `401` para token bearer ausente, inválido ou expirado; `403` para identidade autenticada sem permissão; `404` para recurso inexistente ou inacessível ao provedor; `409` para conflito de carteira, idempotência ou operação; `422` para rejeição definitiva de negócio com `failureCode`; `202` para referência pendente; `503` para falha transitória de dependência ou resultado `FAILED` persistido.

O teste de integração HTTP/OIDC usa PostgreSQL e Keycloak locais. Os testes de integração carregam `.env` automaticamente e são ignorados quando suas dependências não estão disponíveis. Com os containers ativos e a migration aplicada, execute a suíte completa:

```bash
go test ./... -count=1
go test -race ./...
go vet ./...
```
`go test -race ./...` repete os testes com o detector de condições de corrida, importante para os fluxos concorrentes. `go vet ./...` analisa os pacotes em busca de construções suspeitas. O padrão `./...` executa o comando em todos os pacotes do módulo a partir do diretório raiz; não substitua por um nome de pasta externa como `ProjectsPrivate.`.

### Testes de concorrência e carga (k6)

O script [`scripts/k6-concurrency-test.js`](./scripts/k6-concurrency-test.js) requer k6 2.3.0 ou compatível, a API pronta, a migration aplicada e os containers locais ativos. O wrapper `scripts/run-k6.sh` carrega `.env`, obtém tokens `client_credentials` no `setup()` e valida saldo, ledger e reconciliação no `teardown()`. As credenciais locais são apenas para desenvolvimento e não devem ser reutilizadas fora desta máquina.

Com `go run .` ativo em outro terminal, rode os três cenários:

```bash
sh scripts/run-k6.sh duplicate
sh scripts/run-k6.sh compete
sh scripts/run-k6.sh parallel
```

`duplicate` envia a mesma operação em 50 VUs e exige exatamente um resultado novo e 49 replays. `compete` abre uma carteira com `100.00 BRL` e dispara duas apostas de `80.00 BRL`; uma deve ser processada e a outra rejeitada por `INSUFFICIENT_BALANCE`. `parallel` cria uma carteira por VU e envia uma aposta independente a cada uma. As execuções deixam os registros gerados no banco para auditoria; cada execução usa um prefixo novo via `RUN_ID` ou timestamp.

#### Medições observadas

Execução em 2026-10-01, Linux com 16 CPUs disponíveis, Go 1.26.1, k6 2.3.0, PostgreSQL 17.6, LocalStack 4.5.0 e Keycloak 26.2.5. API e dependências rodaram localmente; cada cenário durou menos de um segundo de tráfego e não constitui teste de capacidade sustentada. Vazão é a taxa de iterações do k6; p50/p95/p99 são valores de `http_req_duration`:

| Cenário | Carga | Vazão | p50 / p95 / p99 | Falhas HTTP e resultado |
| --- | --- | ---: | --- | --- |
| Duplicidade | 50 VUs, 50 envios | 374.15 iterações/s | 60.01 / 75.49 / 79.19 ms | 0/56; 1 novo, 49 replays, 1 débito |
| Disputa | 2 VUs, 2 apostas | 29.56 iterações/s | 10.08 / 15.30 / 15.32 ms | 1/8 resposta HTTP 422 esperada; 1 aprovação, 1 rejeição, saldo 20.00 |
| Carteiras independentes | 10 VUs, 10 carteiras | 53.52 iterações/s | 2.93 / 41.89 / 42.23 ms | 0/52; 10 aprovações, cada saldo 99.00 |

Todas as verificações funcionais do k6 passaram. O `http_req_failed` da disputa inclui a resposta 422 de negócio esperada. A coleta de `/metrics` após as execuções mostrou zero divergências e conflitos; o indicador de atraso da última publicação da outbox marcou 4160 ms naquele instante. A métrica de DLQ conta as tentativas observadas pelo processo no limite de encaminhamento, mas não substitui a métrica autoritativa do broker. Como os coletores são locais ao processo, agregue `/metrics` de cada instância no sistema de coleta para obter uma visão distribuída.

As variáveis podem ser consultadas e alteradas em [`.env.example`](./.env.example). A configuração Keycloak importada está em [`keycloak/realm.json`](./keycloak/realm.json), e a criação de filas/redrive em [`scripts/localstack/init-sqs.sh`](./scripts/localstack/init-sqs.sh).