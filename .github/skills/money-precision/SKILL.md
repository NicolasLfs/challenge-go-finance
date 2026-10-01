---
name: money-precision
description: "Use when parsing, calculating, serializing, persisting, testing, or reviewing Money, balances, wager amounts, ledger entries, or any monetary value in this Go project."
---

# Precisão Monetária

`CHALLENGE.md` define precisão monetária como requisito eliminatório. Nunca use `float32` ou `float64` para valores monetários, inclusive em parsing, cálculos intermediários, comparação, serialização, persistência, métricas ou testes.

## Regras

- Use unidades mínimas inteiras em `int64` ou uma biblioteca decimal de precisão exata. Prefira o padrão já adotado pelo projeto e documente limites e mapeamento para o PostgreSQL.
- O contrato externo usa `{"amount":"25.00","currency":"BRL"}`. Analise a string decimal sem conversão por ponto flutuante; serialize com escala fixa de duas casas.
- Rejeite valor vazio, sinalização não finita (`NaN`/`Infinity`), notação científica, escala superior a duas casas, moeda inválida ou negativa em entrada financeira externa. Não arredonde silenciosamente.
- Verifique overflow em parsing, soma, subtração e negação. Exija moeda compatível em aritmética e comparação.
- Permita valores negativos apenas em cálculos/diferenças internos previstos; saldo nunca pode ficar negativo. A política de zero deve respeitar o tipo da operação: saldo inicial e `LOSS` aceitam zero; `BET`, `WIN`, `REFUND` e `ROLLBACK` exigem valor positivo.
- Normalize entradas equivalentes somente se a política estiver documentada e for aplicada antes de calcular o hash de idempotência, igualmente para HTTP e SQS.

## Ao encontrar floats

Se uma solicitação ou implementação propuser floats para dinheiro, corrija a abordagem antes de prosseguir. Inclua testes para parsing, escala, limites, overflow, moedas incompatíveis, valores inválidos e serialização exata. Nunca converta um float para inteiro ou decimal como forma de recuperar precisão já perdida.