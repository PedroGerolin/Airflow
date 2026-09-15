# .claude/

Configuração do Claude Code específica deste projeto.

- `skills/` — skills de projeto (ex: rodar `dbt run`/`dbt test` de um dos pipelines,
  validar uma DAG antes de subir, checar conexões do Airflow). Cada skill fica em
  `skills/<nome>/SKILL.md`.
- `agents/` — subagentes específicos deste projeto, se algum fluxo de trabalho justificar
  um (ex: revisão de modelos dbt).
- `settings.json` (ainda não criado) — permissões/allowlist de comandos para reduzir prompts
  de confirmação repetidos (ex: `docker-compose`, `dbt`, `git`).
- MCP servers do projeto (ex: um servidor MCP de BigQuery ou dbt) entram em `.mcp.json` na
  raiz do repo quando forem definidos — ainda não existe nenhum configurado.

Nada aqui ainda foi criado além desta estrutura — é o ponto de partida para adicionar
skills/agents/MCPs conforme forem definidos.
