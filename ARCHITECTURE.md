# Arquitetura

Este documento registra as decisões e o estado atual do desafio descrito em [`CHALLENGE.md`](./CHALLENGE.md). A composição Fx, API HTTP/OIDC, consumidor SQS, inbox transacional, publisher da outbox, endpoint Prometheus e cenários de carga k6 estão implementados. Testes de integração executam contra PostgreSQL, Keycloak e LocalStack reais; um teste inicia processos Go independentes para demonstrar idempotência e serialização financeira distribuída. O progresso e as verificações estão em [`CHECKLIST.md`](./CHECKLIST.md).

## Objetivos e Componentes

- Processar operações financeiras de forma equivalente via HTTP e SQS, com idempotência persistente e segurança entre múltiplas instâncias.
- Usar Go, Uber Fx, PostgreSQL, SQS (LocalStack local) e Keycloak (OAuth 2.0/OIDC); usar `sqlc` com `pgx/v5` e SQL explícito para tornar queries, transações e locks type-safe e verificáveis.
- Manter o domínio independente de Fx, HTTP, SQS e persistência. Adaptadores traduzem protocolos externos e chamam os mesmos casos de uso.
- PostgreSQL é a fonte de verdade financeira. SQS fornece entrega at-least-once; deduplicação FIFO não substitui constraints nem idempotência no banco.

Fluxo: o adaptador HTTP ou consumidor SQS valida autenticação/formato e chama o caso de uso; o caso de uso valida domínio e executa a transação PostgreSQL; depois do commit, uma mensagem SQS pode ser removida e um worker separado publica eventos registrados na outbox. Pendências de referência ficam duráveis no banco e são retomadas por worker.

## Infraestrutura Local

O Compose fixa PostgreSQL 17.6, LocalStack 4.5.0, Keycloak 26.2.5 e Go 1.26.1 (alinhado ao `go.mod`). PostgreSQL e os dados locais do LocalStack/Keycloak usam volumes nomeados; PostgreSQL, SQS e Keycloak têm health checks. Portas são publicadas somente em loopback. O realm `wagering-local` é importado no primeiro start e contém dois clientes de provedor com claims `provider_id` distintos, um cliente interno e a audiência `wagering-api`; segredos são valores dummy versionados exclusivamente para desenvolvimento.

Um inicializador idempotente cria `wager-transactions.fifo`, `wager-transactions-dlq.fifo` e `wager-events.fifo`. A fila de entrada tem visibility timeout de 60 segundos, `maxReceiveCount` 5 e redrive restrito à DLQ correspondente. A outbox publica no destino `wager-events.fifo`; `MessageGroupId` deve ser o ID do agregado e `MessageDeduplicationId`, o `eventId` estável. Na entrada, agrupar por carteira mantém ordem por carteira sem bloquear carteiras distintas.

LocalStack usa as credenciais fictícias do `.env` e simula a API SQS local; isso não representa uma política IAM de produção. O fluxo de desenvolvimento executa a API, migrations, testes e k6 no host, usando URLs `localhost`. Após copiar `.env.example` para `.env`, `docker compose up --build -d` inicia PostgreSQL, LocalStack, Keycloak e a checagem `app-check`; esse último serviço executa testes e encerra, não hospeda a API. `sh scripts/migrate.sh up` aplica a migration dentro do container PostgreSQL, e `go run .` carrega `.env` e inicia a API no host. Os testes de integração carregam o mesmo arquivo; `sh scripts/run-k6.sh <cenário>` o carrega para o k6.

## Domínio e Dinheiro

### Money e Wallet

- `Money` é um value object imutável representado por `int64` em centavos, acompanhado de código ISO 4217. BRL é a moeda dos cenários principais; o tipo ainda carrega moeda e rejeita operações entre moedas incompatíveis.
- O contrato externo recebe/devolve `amount` como string decimal com exatamente duas casas. Parsing, aritmética, comparação, serialização e persistência nunca usam `float32`/`float64`.
- Rejeitar strings vazias, valores negativos em entrada financeira externa, sinal positivo explícito, notação científica, `NaN`, `Infinity`, escala diferente de duas casas e overflow. Não arredondar silenciosamente; a moeda também deve pertencer ao conjunto ISO 4217 suportado.
- `Wallet` encapsula jogador, moeda, saldo, versão e timestamps. Construtores validam; reidratação não repete transições ou movimentações. Saldo nunca pode ser negativo. Versão começa em `1` e só avança quando o saldo efetivamente muda.
- A abertura com saldo positivo cria `OPENING`, crédito no ledger e eventos financeiros na mesma transação. Saldo inicial zero cria somente a carteira.

### WagerTransaction e Operações

- Estados: `PENDING`, `PENDING_REFERENCE`, `PROCESSED`, `REJECTED` e `FAILED`. Estados terminais não aceitam novas transições; erros de domínio são classificáveis e recusas não usam `panic`.
- Operações externas: `BET` debita valor positivo com saldo suficiente; `WIN` credita valor positivo; `LOSS` exige zero e não altera saldo/versão; `REFUND` credita integralmente uma `BET` processada; `ROLLBACK` desfaz integralmente `BET`, `WIN` ou `REFUND` processada.
- `OPENING` é exclusivamente interno, sem metadados externos, e é rejeitado em requisições HTTP/SQS de provedor.
- `REFUND` e `ROLLBACK` precisam referenciar transação do mesmo provedor, jogador, carteira, moeda e rodada; valor deve ser igual ao valor referenciado. Reversões parciais não são suportadas.
- Interpretação adotada: a mesma `BET` não pode receber `REFUND` e `ROLLBACK` bem-sucedidos que devolvam o mesmo débito. A aplicação serializa a decisão por referência e rejeita a segunda reversão conflitante. `ROLLBACK` de uma `REFUND` é uma operação distinta, debitando o valor devolvido se houver saldo; também só pode ocorrer uma vez.
- Reversão sem saldo suficiente é rejeitada com código de falha diferente de aposta sem saldo. `LOSS` produz `WagerTransactionProcessed`, mas não `WalletBalanceChanged`.

## Casos de Uso e Composição Fx

- O pacote `application` define as portas `Repository`, `Transaction` e `IDGenerator`, sem importar Fx, `pgx`, HTTP ou SQS. `WalletService.OpenWallet`, `WagerService.Process`, `WagerService.RetryPendingReferences` e `ReconciliationService.Reconcile` coordenam o domínio e os registros persistentes.
- A abertura cria a carteira e, se positiva, `OPENING`, crédito de ledger e dois snapshots de outbox dentro de `WithinTransaction`. Saldo zero grava apenas a carteira. A constraint única `(player_id, currency)` é convertida em `ErrWalletConflict` também quando duas aberturas correm juntas.
- O hash usa SHA-256 de JSON UTF-8 determinístico com chaves ordenadas lexicograficamente. Inclui os campos de negócio e o valor decimal normalizado (`Money.Decimal()`); exclui `idempotencyKey`, ID interno e metadados de transporte. O handler HTTP deriva `providerId` do token antes de chamar o serviço; SQS constrói o mesmo `domain.ExternalWagerInput` e usa o mesmo caso de uso/hash.
- O replay compara o hash persistido e devolve status, código de falha e saldo observado no resultado original. Reutilizar chave com hash diferente gera conflito; reapresentar a identidade externa com outra chave também conflita. Conflitos de constraint após corrida são lidos numa transação nova.
- `REFUND` e `ROLLBACK` sem referência processada ficam em `PENDING_REFERENCE`, com saldo original observado, tentativas persistidas e próxima execução. O worker usa backoff exponencial começando em 1 segundo, limitado a 15 minutos, e termina após 20 tentativas ou 24 horas. Referência pendente é reagendada; rejeitada/falhada resulta em `REFERENCE_NOT_PROCESSED`; expiração sem referência resulta em `REFERENCE_NOT_FOUND`.
- A mesma aposta não pode receber um `REFUND` e um `ROLLBACK` bem-sucedidos. `ROLLBACK` de `REFUND` é permitido e debita o crédito devolvido. Reversão sem fundos usa `REVERSAL_INSUFFICIENT_BALANCE`; aposta sem fundos usa `INSUFFICIENT_BALANCE`. Outros códigos incluem `REFERENCE_MISMATCH`, `REFERENCE_TYPE_INVALID` e `REFERENCE_ALREADY_REVERSED`.
- Reconciliação bloqueia a carteira durante a leitura, soma créditos menos débitos do ledger com aritmética inteira, retorna saldo armazenado, saldo calculado, diferença `stored - ledger` e contagem, sem atualizar registros.
- `app.Module` compõe configuração, pool `pgxpool`, adaptador PostgreSQL, cliente SQS, gerador de IDs, serviços, handler HTTP, logger e worker de referências. O listener HTTP abre no `OnStart` e drena requisições no `OnStop`; o pool valida conexão por ping de até 5 segundos e fecha após os consumidores. O teste completo Fx exige PostgreSQL, Keycloak, LocalStack e schema aplicado.

## PostgreSQL, Transações e Concorrência

Será adotado `sqlc` com driver `pgx/v5`, centralizando toda a definição e geração de acesso ao banco no diretório `db/`. `Money` será persistido em centavos `BIGINT`, com moeda em coluna separada.

### Centralização em db/ e Uso do sqlc

- Schema: definido em `db/schema.sql` com todas as tabelas, índices, chaves estrangeiras e constraints de integridade requeridas.
- Queries: definidas em `db/query.sql` utilizando SQL explícito anotado (`-- name: NomeDaQuery :one/:many/:exec`). Não são usados ORMs ou queries dinâmicas em string solta.
- Código Gerado: o `sqlc` compila os arquivos de `db/` para o pacote Go `db` (`db/db.go`, `db/models.go`, etc.), provendo interfaces e structs estritamente tipadas.
- Transações: repositórios utilizam `queries.WithTx(tx)` sobre uma transação explícita aberta via `pgx`, assegurando controle de commits e rollbacks.

Uma transação PostgreSQL explícita agrupa, conforme aplicável, estado/resultado da operação, saldo e versão, lançamento de ledger, registro de inbox e snapshots da outbox. Falha antes do commit faz rollback do conjunto. Abertura positiva agrupa carteira, `OPENING`, crédito no ledger e eventos. Nenhum evento externo é publicado antes do commit.

### Coordenação e Constraints

- Estratégia escolhida: lock pessimista por carteira usando `SELECT ... FOR UPDATE` durante a transação financeira. Locks são por linha; carteiras distintas podem avançar em paralelo. A constraint única protege a criação concorrente quando ainda não há linha para bloquear.
- Constraints/índices únicos protegem `(playerId, currency)`, `(providerId, externalTransactionId)`, chave de idempotência por provedor, `(walletId, transactionId)` no ledger, `(consumerName, messageId)` na inbox e uma abertura interna por carteira.
- O banco impede saldo negativo e lost updates; a garantia não depende de mutex local nem de SQS FIFO.
- Ledger é append-only. Além de não oferecer update/delete na aplicação, migrations devem proteger a tabela no banco contra `UPDATE` e `DELETE` para o papel da aplicação. A unicidade por carteira/transação impede lançamento financeiro duplicado.
- Referências e transações relacionadas usam chaves estrangeiras. Reversões concorrentes bloqueiam/resolvem a referência na mesma ordem para impedir devolução duplicada.

O primeiro migration versionado está em `db/migrations/0001_financial_schema_up.sql` e `0001_financial_schema_down.sql`; ele é aplicado com `psql` e inclui o DDL de `db/schema.sql` via `\\ir`. O pacote `repository` injeta `*db.Queries`, usa `WithTx` para a mesma transação e faz rollback quando a função financeira falha. A aplicação de migration e o teste de constraints devem ser executados contra PostgreSQL real; `go test ./...` valida o contrato SQL e a geração, mas não substitui esse teste de integração.

## Idempotência e Hash

- Identidade da operação financeira: `(providerId, externalTransactionId)`, única mesmo que uma nova chave de idempotência seja apresentada.
- HTTP exige `Idempotency-Key` e nunca a substitui silenciosamente. SQS usa `data.idempotencyKey` e adiciona deduplicação por `consumerName` + `messageId` na inbox.
- Calcular SHA-256 sobre JSON canônico UTF-8 com chaves ordenadas lexicograficamente. Incluir os campos de negócio: provedor, transação externa, jogador, carteira, rodada, jogo, tipo, dinheiro normalizado e referência externa se presente. Excluir chave de idempotência e metadados de transporte.
- HTTP e SQS usam a mesma normalização/canonicalização. Testes devem comprovar que payloads de negócio equivalentes produzem o mesmo hash.
- Mesma chave e mesmo hash retorna resultado persistido sem reaplicar efeitos. Chave com payload diferente ou identidade externa reapresentada com outra chave gera conflito.
- Persistir o saldo observado no processamento bem-sucedido para o replay devolver o resultado original mesmo após movimentações posteriores.
- Concorrência na primeira inserção é decidida pelas constraints únicas e transação. Se uma consulta inicial por chave não enxergar uma gravação concorrente, a consulta posterior pela identidade externa compara chave e hash: equivalência retorna o resultado persistido; conteúdo ou chave diferentes continuam sendo conflito, sem repetir a movimentação.

## Referências Pendentes e Recuperação

- Operações sem dependência são concluídas sincronamente sem commit intermediário de aceite `PENDING`.
- Quando `REFUND`/`ROLLBACK` chega antes da referência, persistir em `PENDING_REFERENCE` com próxima tentativa e contagem durável. Na entrada SQS, inbox e pendência confirmadas permitem remover a mensagem; o worker assume a continuidade.
- Política operacional inicial proposta: retry exponencial com jitter, começando em 1 segundo e limitado a 15 minutos; expiração após 24 horas ou 20 tentativas, o que ocorrer primeiro. Valores configuráveis devem ser ajustados e documentados com evidências.
- Se a referência ainda estiver pendente, reagendar. Se estiver rejeitada/falhada ou for incompatível, finalizar a dependente como `REJECTED` com `failureCode` estável. Referência ausente ao expirar usa `REFERENCE_NOT_FOUND` e gera evento de rejeição.
- Indisponibilidade transitória de banco/broker não é rejeição de domínio e não confirma efeitos parciais. Todo estado recuperável confirmado precisa de trabalho durável assumível por outra instância.

## Inbox, SQS e Outbox

### Entrada SQS

- Provisionar `wager-transactions.fifo` e `wager-transactions-dlq.fifo` com redrive. Credenciais/políticas controlam acesso ao broker; consumidor também valida domínio.
- Usar `messageId` do envelope como identidade persistente e conferir SHA-256 do corpo em reentregas. Hash diferente para o mesmo consumidor/mensagem é conflito, não replay válido.
- `MessageGroupId` será a identidade estável da carteira, serializando mensagens dessa carteira e permitindo paralelismo entre carteiras. `MessageDeduplicationId` será estável por operação/mensagem; FIFO é otimização, não garantia financeira.
- Remover mensagem somente após commit. Rejeições de negócio confirmadas são terminais. Erros transitórios fazem retry; erros permanentes/esgotados vão à DLQ. Mensagens inválidas seguem política de falha permanente, com log sem dados sensíveis.
- Valores operacionais: long polling de 20 segundos, visibility timeout de 60 segundos e `maxReceiveCount` 5; mensagens inválidas/permanentemente inconsistentes têm visibilidade zerada para acelerar o redrive. Falhas transitórias usam backoff exponencial limitado a 60 segundos.

### Publicação Outbox

- Cada linha contém `eventId` estável, agregado, tipo/versão, correlação/causação, timestamp UTC e snapshot imutável tipado, além de tentativas, próxima tentativa e estado de publicação/claim.
- Publisher independente disputa linhas entre instâncias com claim recuperável e `SELECT ... FOR UPDATE SKIP LOCKED`. A publicação ocorre somente para eventos cujo commit de origem foi confirmado.
- Se o processo cair após publicar e antes de marcar a linha como publicada, o evento pode ser enviado de novo com o mesmo `eventId`. Consumidores deduplicam pelo ID; não se promete exactly-once no broker.
- Falha de SQS mantém evento pendente com backoff exponencial até 15 minutos e claim recuperável de um minuto. O publisher registra atraso em log estruturado e mantém a última observação acessível por `LastPublishedLag`; a métrica `wager_outbox_lag_milliseconds` também fica disponível em `/metrics`.
- Eventos exigidos: `WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference` e `WalletBalanceChanged`. Este último inclui carteira, transação, direção, Money, saldo anterior/posterior e versão; só existe quando o saldo muda.

## Autenticação e Autorização

- Keycloak é o IdP; a API valida access tokens OAuth 2.0/OIDC com discovery/JWKS usando `go-oidc`, sem cadastro de senhas ou emissão própria. Verifica assinatura, `iss`, `aud`, validade temporal e `sub`.
- Para provedores, o claim confiável `provider_id` vem do token e é usado ao chamar o caso de uso. O campo `providerId` do JSON não concede acesso e é substituído pela identidade autenticada. A rota de consulta externa também exige que o provedor do path corresponda ao token.
- O cliente interno é reconhecido pelo `azp` exato `internal-service` e pela ausência de `provider_id`. Ele pode abrir/consultar carteiras e reconciliar; clientes de provedor não acessam essas rotas. Identidades sem `provider_id` válido ou `azp` interno são negadas.
- Provedores só leem operações próprias; consultas por ID interno com dono diferente respondem `404` para não confirmar a existência do registro. Replays são isolados por `provider_id` e a chave de idempotência continua persistida no caso de uso comum.
- Testes unitários assinam JWTs contra JWKS local para issuer, audience, expiração e assinatura; um teste opt-in usa Keycloak e PostgreSQL reais, cobrindo credencial inválida, abertura interna, forja de `providerId`, replay e isolamento entre provedores.
- `/health/live` e `/health/ready` são públicos; readiness testa PostgreSQL e acesso às filas SQS de entrada e saída. Credenciais SQS explícitas são opcionais como par e, sem elas, o SDK usa sua cadeia padrão.

## HTTP e Erros

O servidor usa `net/http`; HTTP e SQS chamam o mesmo serviço de aplicação para processar operações financeiras.

- `POST /wallets`, `GET /wallets/{walletId}`, `GET /wallets/{walletId}/ledger` e `POST /wallets/{walletId}/reconciliation` são restritas ao cliente interno.
- `POST /wagering/transactions` exige identidade de provedor e `Idempotency-Key`; o header é armazenado sem substituição e `OPENING` é rejeitado.
- `GET /wagering/transactions/{transactionId}` e `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}` mantêm isolamento no dono autenticado, inclusive em replays.
- Ledger usa cursor Base64 URL-safe contendo timestamp + ID, com keyset SQL em ordem `created_at DESC, id DESC`; limite padrão 50 e faixa válida 1-100. Cursor é paginação, não credencial.
- Reconciliação retorna saldo armazenado, saldo calculado, diferença `stored - ledger` e contagem, sem alterar saldo.
- Mapeamento implementado: `400` entrada inválida; `401` credencial ausente/inválida/expirada; `403` sem permissão; `404` recurso inexistente/não acessível; `409` conflito de carteira/idempotência/operação; `422` rejeição financeira terminal com `failureCode`; `202` referência pendente; `503` indisponibilidade transitória ou estado `FAILED`. Respostas não incluem detalhes internos.

## Fx, Lifecycle e Shutdown

- `fx.Module`, `fx.Provide`, `fx.Invoke` e construtores compõem configuração, pool, repositórios, casos de uso, handlers e workers. Domínio não importa Fx.
- `fx.Lifecycle` inicia após validação de configuração/dependências e encerra os componentes em ordem: primeiro parar entradas/servidores/workers, depois fechar clientes e pool.
- Em `SIGTERM`, parar polling SQS e novas requisições, cancelar contextos, concluir handlers em andamento dentro do prazo ou liberar mensagens ainda não iniciadas, aguardar término observável e então fechar dependências. Claims de outbox não confirmados ficam recuperáveis após um minuto.
- Goroutines de workers precisam de cancelamento, coordenação de término e tratamento de erro; falha no startup/shutdown deve ser observável.

## Observabilidade e Verificação

- Logs JSON correlacionam `correlationId`, `messageId`, `transactionId`, `walletId` e `providerId` quando disponíveis. Nunca registrar tokens, segredos ou payload financeiro completo.
- `GET /metrics` expõe texto Prometheus com `wager_transactions_total`, `wager_idempotent_replays_total`, `wager_retries_total`, `wager_dlq_messages_total`, `wager_concurrency_conflicts_total`, `wager_outbox_lag_milliseconds`, o histograma `wager_processing_duration_seconds` e `wallet_reconciliation_divergences_total`. Labels de status e subsistema são allowlisted para evitar cardinalidade controlada pelo cliente.
- Os coletores são concorrentes e mantidos em memória por processo; reinício zera os contadores e cada instância deve ser coletada separadamente pelo scraper. A métrica de DLQ representa tentativa local observada quando `ApproximateReceiveCount` atinge o limite, não a contagem autoritativa do broker.
- Logs JSON correlacionam IDs disponíveis sem registrar credenciais ou payload financeiro completo. Uma divergência aparece na resposta de reconciliação, em log `Warn` com diferença em unidades mínimas e na métrica correspondente.
- Testes unitários cobrem Money, Wallet, transições, tipos/zero, `OPENING`, erros e conflito de payload. Testes de integração usam PostgreSQL, Keycloak e LocalStack/MiniStack reais.
- O teste PostgreSQL de processos independentes demonstra 50 envios do mesmo wager em três processos, disputa de duas apostas de `80.00 BRL` contra saldo `100.00 BRL` em processos separados e progresso de três carteiras independentes. Cada subprocesso abre seu próprio pool PostgreSQL.
- Testar falhas entre commit e remoção SQS, commit e publicação outbox, publicação e confirmação, reentrega, reinício, referências fora de ordem, fluxos HTTP/SQS cruzados e reconciliação final.
- Gates: `gofmt`, `go test ./...`, `go test -race ./...`, `go vet ./...` e comandos de integração documentados.
- k6 complementa os testes Go para carga HTTP; comandos, ambiente e medições de 2026-10-01 estão registrados no README. Os três cenários validam 50 replays, disputa de saldo e dez carteiras independentes. São execuções curtas de verificação, não metas de capacidade nem testes sustentados.
- Não há meta mínima de RPS. Testes de carga devem ser reproduzíveis e informar metodologia.

## Limitações e Decisões a Confirmar

- Após a limpeza completa do ambiente Docker com `docker system prune -a --volumes`, foram refeitos o build das imagens, a inicialização dos serviços, as migrations e a execução documentada, com sucesso. Essa validação partiu de volumes removidos e banco local vazio.
- O teste opt-in com PostgreSQL, Keycloak e LocalStack reais cobre tokens `client_credentials`, restrição de rotas internas, replay equivalente e isolamento entre provedores. O teste de paginação também foi executado contra PostgreSQL real. Execute os comandos descritos no README com as dependências locais para repetir essas verificações.
- Keycloak e LocalStack locais não comprovam todos os comportamentos de produção do IdP/AWS; o teste de autenticação real também não cria um token expirado no Keycloak, por isso expiração, issuer e audience têm testes JWT determinísticos com JWKS local.
- Visibility timeout, backoff e redrive são defaults operacionais cobertos por testes de consumer e redrive real no LocalStack; devem ser recalibrados com medições de produção. `/metrics` e `LastPublishedLag` são locais a cada processo e precisam de scraping/agrupamento externo para visão distribuída.
- O escopo financeiro principal opera em BRL e escala de duas casas; suporte a moedas com escalas distintas não faz parte deste desafio.
- O k6 foi executado contra a API local com Keycloak, PostgreSQL e LocalStack ativos; as medições são específicas deste host e da carga curta registrada, sem generalização para produção.
