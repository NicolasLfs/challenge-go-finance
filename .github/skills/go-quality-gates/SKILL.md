---
name: go-quality-gates
description: "Use when generating, editing, reviewing, or validating Go code in this repository, especially concurrency, shared state, tests, build, race detection, or static analysis."
---

# Verificação de Código Go

Todo código Go deve compilar, ser formatado e passar pelos gates exigidos pelo desafio. Escreva código seguro para concorrência e compatível com `go test -race ./...` e `go vet ./...`.

## Práticas

- Execute `gofmt` nos arquivos Go modificados.
- Evite variáveis globais mutáveis. Se estado compartilhado for necessário, limite sua propriedade e sincronize todo acesso concorrente com mecanismos adequados; prefira dependências explícitas e estado local.
- Não mantenha goroutines sem cancelamento, coordenação de término ou tratamento observável de erro. Respeite contextos e o ciclo de vida Fx.
- Evite ignorar erros, especialmente em transações, rollback, fechamento de recursos, mensagens e publicação de eventos.
- Adicione testes focados antes da implementação conforme a skill TDD; não reduza o rigor dos testes de concorrência para acomodar uma implementação insegura.

## Gates

Ao alterar queries (`db/query.sql`) ou tabelas/constraints (`db/schema.sql`), execute `sqlc generate` para regenerar os tipos em `db/`.
Após a mudança, rode o teste focado e, quando aplicável, `go test ./...`, `go test -race ./...` e `go vet ./...`. Se um comando não puder ser executado, informe o bloqueio e não afirme que passou. Corrija problemas introduzidos pela mudança sem ampliar a tarefa para falhas preexistentes não relacionadas.