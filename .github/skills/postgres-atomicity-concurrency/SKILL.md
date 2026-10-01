---
name: postgres-atomicity-concurrency
description: "Use when implementing PostgreSQL repositories, pgx transactions, wallet balance changes, ledger, inbox/outbox, idempotency, or concurrent financial processing in this Go challenge."
---

# Atomicidade e Concorrência PostgreSQL

Use PostgreSQL como autoridade das invariantes financeiras distribuídas. Locks locais, deduplicação FIFO ou memória de processo não substituem constraints e transações. O acesso ao banco deve ser centralizado no diretório `db/`, utilizando `sqlc` com `pgx/v5` e SQL explícito para garantir type-safety, transações e locks verificáveis.

## sqlc e Centralização em db/

- Toda a camada de banco de dados fica centralizada em `db/`:
  - `db/schema.sql`: DDL canônico de tabelas, índices, constraints e regras de integridade consumido pelo `sqlc`.
  - `db/query.sql`: queries SQL explícitas anotadas para o `sqlc` (`-- name: QueryName :one/:many/:exec`). Sempre crie e edite queries através do `sqlc`.
  - Pacote Go gerado: código type-safe gerado pelo `sqlc` (`package db`), contendo models, interfaces `DBTX` e métodos de execução.
- Sempre rode `sqlc generate` após alterar `schema.sql` ou `query.sql`.
- No código de repositório, componha `*db.Queries` e injete transações com `queries.WithTx(tx)`, preservando o controle transacional explícito com `pgx.Tx`.

## Limite transacional

- Abra e controle transações PostgreSQL explicitamente com `pgx`; passe `context.Context`, trate erros e faça rollback em toda saída que não confirmar.
- Na mesma transação que confirma uma operação, grave todos os efeitos aplicáveis: estado/resultado da transação, saldo e versão da carteira, ledger, inbox e snapshots da outbox. Uma falha em qualquer gravação deve abortar o conjunto.
- Na abertura de carteira com saldo positivo, carteira, `OPENING`, crédito no ledger e eventos outbox pertencem ao mesmo commit. Saldo inicial zero não gera movimentação financeira.
- Nunca publique eventos antes do commit. Publishers leem a outbox confirmada e marcam publicação com disputa segura entre processos e recuperação de claims abandonados.

## Concorrência e integridade

- Coordene escritores por carteira com locking no banco, atualização atômica condicionada ou controle otimista com retry limitado. Evite locks globais e não dependa de mutex em memória.
- Impeça saldo negativo e lost updates no próprio banco; incremente a versão apenas em mudança efetiva de saldo.
- Imponha no schema unicidade de `(playerId, currency)`, identidade externa/idempotência, `(walletId, transactionId)` no ledger e `(consumerName, messageId)` na inbox, além das constraints de estado e não negatividade cabíveis.
- Proteja o ledger contra `UPDATE` e `DELETE` por mecanismos do banco. Toda correção deve ser um lançamento novo.
- Bloqueie ou serialize as transações necessárias para que reversões concorrentes não sejam aplicadas mais de uma vez.

## Verificação

- Teste com PostgreSQL real migrations, constraints, atomicidade, rollback e imutabilidade; mocks não demonstram essas propriedades.
- Teste a disputa de duas apostas distintas de `80.00 BRL` contra saldo `100.00 BRL`, esperando uma processada, uma rejeitada por saldo insuficiente, saldo `20.00 BRL` e um débito.
- Exercite pelo menos três instâncias independentes nos testes de concorrência requeridos e verifique a consistência do saldo com o ledger.