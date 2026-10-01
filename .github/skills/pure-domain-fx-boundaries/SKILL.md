---
name: pure-domain-fx-boundaries
description: "Use when designing or changing Go domain entities, value objects, use cases, dependency injection, Uber Fx modules, or boundaries between domain and HTTP, SQS, and PostgreSQL."
---

# Domínio Puro e Composição Fx

Siga os limites arquiteturais do `CHALLENGE.md`: o domínio e as regras financeiras não dependem de Fx, HTTP, SQS, pgx, banco ou bibliotecas de terceiros.

## Domínio

- Mantenha entidades e value objects em pacotes Go independentes de infraestrutura.
- Encapsule estado; use construtores com validação, operações explícitas e erros classificáveis. Rejeite valores não inicializados/inválidos e não use `panic` para recusas de negócio.
- Separe criação de reidratação. Reidratar estado persistido não deve reaplicar movimentações, transições ou emitir eventos.
- Passe `context.Context` às operações de I/O; não deixe contexto ou dependências de infraestrutura vazar para tipos de domínio sem necessidade.
- Faça os casos de uso coordenarem portas/interfaces de persistência e mensagens. O domínio não importa `db` (gerado pelo `sqlc`), `pgx` ou SQL; adaptadores de repositório implementam as interfaces mapeando tipos de domínio para os modelos gerados pelo `sqlc`.

## Composição

- Use Uber Fx somente na composição da aplicação: `fx.Module`, `fx.Provide`, `fx.Invoke` e `fx.Lifecycle`.
- Construa configuração, conexões, repositórios, casos de uso, handlers e workers via injeção por construtores; evite service locator e estado global mutável.
- Gerencie start, cancelamento, prazo de shutdown e término observável de servidores/workers pelo lifecycle. Pare novas entradas e conclua ou libere trabalho antes de fechar dependências.
- Não importe Fx nos pacotes de domínio para registrar ou iniciar lógica de negócio.

Antes de editar, confira os testes e a organização existentes. Teste invariantes do domínio sem infraestrutura e use integração para validar a composição e o ciclo de vida Fx.