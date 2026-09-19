# docs/ — memória de estudo e operação do projeto

Aqui fica tudo o que foi feito, por quê, e como refazer. Serve pra **reler e aprender** e pra
**reconstruir** a infraestrutura se algo se perder (ex.: fim do trial do Snowflake).

| Documento | Pra quê |
|---|---|
| [`DIARIO_DE_BORDO.md`](DIARIO_DE_BORDO.md) | Cronologia: o que foi feito em cada dia, comandos-chave e conceitos |
| [`RECRIAR_SNOWFLAKE.md`](RECRIAR_SNOWFLAKE.md) | Runbook passo a passo pra refazer o Snowflake numa conta nova |
| [`ROADMAP.md`](ROADMAP.md) | Pendências e próximas ideias (inclui a "versão 3" com Snowpipe) |
| [`guias/snowflake.md`](guias/snowflake.md) | Guia de estudo: objetos, comandos e conceitos do Snowflake |
| [`guias/snowflake-v3-snowpipe.md`](guias/snowflake-v3-snowpipe.md) | Plano da versão futura sem dbt (Snowpipe, Streams, Tasks) |
| [`guias/dbt-multiwarehouse.md`](guias/dbt-multiwarehouse.md) | dbt em BigQuery + Snowflake, dialetos, hooks, bugs reais |
| [`guias/gcp-iam.md`](guias/gcp-iam.md) | IAM, service accounts, menor privilégio (comandos em `gcp_setup/`) |
| [`guias/docker-e-agendamento.md`](guias/docker-e-agendamento.md) | Docker/compose, tarefa agendada do Windows, armadilhas do PowerShell |
| [`guias/git-e-seguranca.md`](guias/git-e-seguranca.md) | Segredos, `.gitignore`, reescrita de histórico |

Fontes de verdade **executáveis** (não duplicadas aqui): `dags/FisioVet/.dbt/snowflake_setup/`
(SQL do Snowflake), `gcp_setup/README.md` (comandos do GCP), `scripts/` (automação Windows),
`CLAUDE.md` (contexto que o Claude Code lê em toda sessão).

## Combinados de trabalho (valem para toda mudança, feita por quem for)
1. **Toda mudança de infraestrutura é registrada**: Snowflake → script numerado em `snowflake_setup/`;
   GCP → comandos em `gcp_setup/README.md`; Docker/agendamento/outros → `scripts/` ou o guia respectivo.
   Motivo: se o trial acabar ou a máquina for trocada, dá pra refazer tudo em outra conta.
2. **Toda mudança é explicada de forma didática** na hora: o que foi feito, quais comandos, por que,
   e o conceito por trás — pra você aprender, não só receber o resultado.
3. **O diário é atualizado** ao fim de cada sessão de trabalho (modelo no fim do `DIARIO_DE_BORDO.md`).
4. **Nenhum segredo** em arquivo versionado (ver [`guias/git-e-seguranca.md`](guias/git-e-seguranca.md)).
5. Validar pelo caminho real (rodar a DAG/`dbt run`), e comparar **paridade** entre BigQuery e
   Snowflake (`MAX(date)`, contagens), não só "terminou sem erro".
