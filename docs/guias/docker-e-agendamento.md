# Guia de estudo — Docker, docker-compose e agendamento no Windows

## Docker: os conceitos
- **Imagem**: o "molde" (ex.: `metabase/metabase`, ou a nossa, construída do `dockerfile` com
  Chromium + dependências). **Container**: uma instância rodando da imagem. **Volume**: pasta
  persistente fora do container (sobrevive a `down`).
- **docker-compose**: descreve vários containers (serviços) num arquivo. `docker compose up -d`
  cria e sobe em segundo plano; `docker compose down` **remove os containers e a rede**, mas
  **não apaga os volumes** (por isso o banco do Airflow e os dados do Metabase persistem).
- **Um projeto compose = um ciclo de vida.** O Airflow (`docker-compose.yaml` na raiz) sobe/desce
  todo dia; o Metabase tem compose **próprio** (`metabase/`) pra ficar sempre no ar.
  `restart: unless-stopped` religa o container quando o Docker Desktop reinicia.
- **Variáveis de ambiente não atravessam pro container sozinhas.** O container é um sistema
  isolado. Por isso o `docker-compose.yaml` tem `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE:
  ${SNOWFLAKE_PRIVATE_KEY_PASSPHRASE:-}` — "pegue do ambiente de quem rodou o compose e injete
  aqui". Mudou o compose → `docker compose up -d` recria os containers.
- **Volumes montados**: `dags/`, `plugins/`, `credential/`, `files/` são pastas do Windows vistas
  dentro do container (`/opt/airflow/...`). Editar um DAG no Windows vale na hora, sem rebuild;
  já mudar `dockerfile`/`requirements.txt` exige `docker compose build`.

## Rodar coisas dentro do container
```powershell
docker compose exec -T airflow-webserver airflow dags list-import-errors     # "No data found" = sem erro
docker compose exec -T airflow-webserver airflow dags trigger fisiovet -r <run_id>
docker compose exec -T airflow-webserver airflow tasks states-for-dag-run fisiovet <run_id>
docker compose exec -T airflow-webserver dbt run --target dev_snowflake --profiles-dir /opt/airflow/dags/FisioVet/.dbt --project-dir /opt/airflow/dags/FisioVet/.dbt
```
- `-T` = não alocar terminal interativo (obrigatório em scripts/tarefas agendadas).
- **Git Bash reescreve caminhos** `/opt/...` como caminho do Windows → prefixar
  `MSYS_NO_PATHCONV=1`. (PowerShell não tem esse problema.)
- **Cache do dbt (`target/`) é compartilhado** entre Windows e container (mesma pasta). Trocar de
  target/ambiente pode dar `KeyError: 'dbt_bigquery://macros/adapters.sql'` →
  `rm -rf dags/FisioVet/.dbt/target`.

## Docker Desktop precisa estar rodando
No Windows, o "daemon" mora dentro do Docker Desktop (pipe `dockerDesktopLinuxEngine`). Fechado =
todo comando `docker` falha com "error during connect". Nosso `run_daily_pipeline.ps1` detecta
isso (`docker version`), abre o `Docker Desktop.exe` e espera o daemon responder antes de seguir.

## Agendamento: a tarefa `Airflow_FisioVet_Daily`
Peças de uma tarefa agendada: **gatilho** (diário 06:00) · **ação** (`powershell.exe -File
scripts/run_daily_pipeline.ps1`) · **configurações/condições** (o que a torna robusta).
Criada por `scripts/create_scheduled_task.ps1` (precisa de **Administrador**).

| Configuração | Valor | Por quê |
|---|---|---|
| `StartWhenAvailable` | ligado | se perdeu o horário (notebook dormindo), roda assim que puder |
| `WakeToRun` | ligado | tenta acordar o notebook — **na prática não acorda** (ver abaixo) |
| `AllowStartIfOnBatteries` | ligado | o padrão do PowerShell **proíbe** rodar na bateria; em 19/09 isso fez a execução ser "perdida" sem catch-up |
| `DontStopIfGoingOnBatteries` | ligado | não matar a execução se desconectar o carregador |
| Logon | interativo | o Docker Desktop precisa da sessão do usuário |

**Modern Standby**: o notebook usa "espera moderna"; o temporizador de ativação **não** o acorda
(nos logs de energia a saída sempre vem com motivo `Lid` = tampa aberta). Resultado: a tarefa roda
via `StartWhenAvailable` quando o notebook fica disponível. Aceito.

Como verificar (não confie só no `LastTaskResult`, `0` = "o script terminou", não "a DAG deu certo"):
```powershell
Get-ScheduledTaskInfo -TaskName "Airflow_FisioVet_Daily"      # LastRunTime, NextRunTime, NumberOfMissedRuns
Select-String -Path C:\Airflow\logs\daily_pipeline.log -Pattern "Iniciando execucao|RESULTADO|ERRO:"
```

## Armadilhas do PowerShell 5.1 (custaram horas)
- **Redirecionar stderr de programa nativo** (`2>&1`, `2>$null`, `*>>`) embrulha cada linha num
  `NativeCommandError`; com `$ErrorActionPreference = "Stop"` isso **aborta o script** mesmo
  com o comando tendo dado certo (o `docker compose` escreve avisos benignos no stderr). Solução:
  `$ErrorActionPreference = "Continue"` e tratar erro real com `throw` explícito + `$LASTEXITCODE`.
- Variável de ambiente de **usuário** criada depois de abrir o terminal não aparece nele; ler
  direto do registro: `[Environment]::GetEnvironmentVariable('NOME','User')`.
- `Start-ScheduledTask` devolve na hora; acompanhe pelo log.
