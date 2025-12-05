## Premissas e Políticas de Dados
- Preços ajustados de BOVA11 (Yahoo via yfR). Faltas de pregão mantidas; sem forward-fill de preço.
- SELIC diária derivada de séries anuais do SGS (1178; fallback 11) convertida por (1+aa)^(1/252)-1 e preenchida por forward-fill em dias sem observação.

## Estratégia e Hiperparâmetros
- EWMA λ_fast=0.940, λ_slow=0.970; refit GARCH a cada 5 dias úteis.
- Floor/Ceiling diário de volatilidade: [0.004, 0.080]. Suavização runmed k=5.
- Alvos de vol (a.a.): 10%, 12%, 15%; caps: 1, 1.5; alavancagem máxima dura: 1.50.
- Winsorização: desativada.

## Custos e Fricções
- Custos de transação (round-trip): 0, 10, 25 bps aplicados sobre |Δw_t|.
- Custo de financiamento: subtraído sobre (w_t-1)+ usando SELIC diária.
- TER do ETF modelado adicionalmente: 0.00% a.a. (0 se assumido já embutido nos preços).

## Testes e Robustez
- Métricas anuais: retorno, vol, Sharpe, Sortino, MDD, Calmar.
- IC por bootstrap MBB: B=500, bloco=10.
- Teste de Sharpe JK–Memmel vs. Buy-and-Hold.
- Controle de data snooping: MCS (se pacote disponível).
- Robustez por subperíodos não sobrepostos e janelas rolantes.
- Diagnósticos GARCH: Ljung–Box, ARCH LM e QQ-plot de resíduos padronizados.

## Restrições Práticas
- Sem vendas a descoberto; rebalanceamento diário; sem restrições explícitas de liquidez além de custos.
- Slippage adicional opcional via extra_slippage_bps.

## Reprodutibilidade
- Sessão e configuração salvas em 'saídas/tabelas/session_info.txt' e 'saídas/dados/config_vt.json'.
