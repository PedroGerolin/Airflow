# Roadmap e pendências

## Próximos passos (em ordem sugerida)
1. **Metabase** — conectar BigQuery e Snowflake (credenciais só-leitura já criadas) e montar:
   a "question" de saldo por cliente (substitui o print manual) e o painel de resultado financeiro.
2. **Automação de cobrança** (Fase 5 do plano): task no Airflow que lê `faturamento_cliente`
   (direto do warehouse, sem dashboard), monta a mensagem por cliente e envia via Evolution API
   (WhatsApp) com log de envio.
3. **Versão 3 do Snowflake** sem dbt (Snowpipe + Streams + Tasks): [`guias/snowflake-v3-snowpipe.md`](guias/snowflake-v3-snowpipe.md).

## Pendências técnicas
| Item | Detalhe | Prioridade |
|---|---|---|
| MCP `toolbox-snowflake` só com senha | Vai parar de conectar quando a Snowflake exigir MFA/chave. Opções: `Snowflake-Labs/mcp`, esperar suporte, script Python | alta |
| `AIRFLOW__CORE__FERNET_KEY` vazio | Senhas de Connections sem criptografia no Postgres. Gerar chave, passar por env, reabrir/salvar as Connections | média |
| Dados de comissão num repo público | `05_reference_data.sql`: nomes + percentuais. Tornar o repo privado ou tirar do git | média |
| Grant órfão no bucket | SA da integration Snowflake antiga (`squtmsjikk@…`) | baixa |
| `dbt test` desligado | Só `clients.yml` tem testes; DAG não chama `dbt test` | média |
| Download de contas a pagar | `enter_debts_page()` nunca é chamado; `contas-a-pagar.csv` é manual | média |
| Histórico do Agendador de Tarefas | Log `TaskScheduler/Operational` está desabilitado (ligar como Administrador) | baixa |
| Execução "perdida" por outros motivos | Conferir `NumberOfMissedRuns` e o log periodicamente | contínua |
| `TransferFile`/`Exporter` chamam `.execute()` direto | Bypassa retries/XCom do Airflow | baixa |
| Skills/agents do Claude Code | `.claude/skills` e `.claude/agents` ainda vazios (plano Fase 3) | baixa |

## Portfólio (Fase 6 do plano)
Sanitizar dados reais (clientes, funcionárias), dados sintéticos pra reprodução, README com
diagrama de arquitetura. Combina com tornar o repositório privado enquanto isso não for feito.
