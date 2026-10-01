---
name: tdd-challenge-workflow
description: "Use when implementing or changing Go domain behavior, financial use cases, HTTP or SQS flows, persistence, or tests for this wagering challenge. Requires tests based on CHALLENGE.md before production code."
---

# TDD Primeiro para o Desafio

Use esta skill ao implementar qualquer funcionalidade ou corrigir comportamento neste projeto. O contrato principal é [`CHALLENGE.md`](../../../CHALLENGE.md); não invente regras financeiras que contradigam o desafio.

## Fluxo obrigatório

1. Localize a seção relevante do `CHALLENGE.md` e leia o código e os testes próximos à camada que controla o comportamento.
2. Antes do código de produção, escreva ou atualize os testes que expressem o resultado esperado. Para domínio e casos de uso, priorize testes unitários; para contratos com PostgreSQL, HTTP, SQS, Fx ou concorrência, use testes de integração adequados.
3. Execute primeiro o teste novo e confirme que ele falha pelo comportamento ausente, não por erro de compilação ou configuração.
4. Implemente a menor alteração de produção capaz de fazer o teste passar, sem enfraquecer ou remover asserções.
5. Execute novamente o teste focado; depois rode as verificações relevantes para a mudança, incluindo `go test -race ./...` e `go vet ./...` quando aplicável.

## Cobertura a considerar

- Casos válidos, limites, entradas inválidas e classificação estável de erros.
- Replays, conflitos de idempotência, entrega duplicada e recuperação após falhas.
- Invariantes financeiras e concorrência no banco, não apenas em memória.
- Testes com dependências reais nos requisitos de integração; não substitua PostgreSQL, SQS ou IdP por mocks para afirmar garantias que dependem deles.

Se não for possível executar um teste de integração por falta de dependências, registre claramente o pré-requisito e não declare a garantia como verificada. Para mudanças puramente documentais ou de configuração, explique por que não há teste de comportamento de produção aplicável e valide o artefato diretamente.