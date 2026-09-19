# Guia de estudo — Git e segurança de segredos

## Conceitos que já custaram caro neste projeto
- **`.gitignore` só impede arquivos *novos* de entrarem.** Se o arquivo já foi commitado antes,
  o git continua rastreando (e publicando) as mudanças. Pra parar de rastrear: `git rm --cached
  <arquivo>` (mantém o arquivo no disco) **e** colocá-lo no `.gitignore`.
- **Apagar um segredo num commit novo não o remove do histórico.** Qualquer pessoa consegue ver
  commits antigos no GitHub. Foi o que aconteceu com a senha antiga do Snowflake em
  `profiles.yml`: repositório **público**, senha visível em commits antigos.
- **Reescrever o histórico** (`git filter-repo --path <arq> --invert-paths`) remove o arquivo de
  **todos** os commits (hashes mudam) e exige `push --force` — destrutivo, faça **backup do
  `.git`** antes. Depois, confirmar: `git log --all -p | grep -c "<trecho da senha>"` deve dar 0.
  Ainda assim, **trate a senha como comprometida** e troque-a.
- `git filter-repo` **remove o remote `origin`** por segurança (recriar com `git remote add`) e
  **reseta a working tree**, apagando arquivos gitignorados (foi assim que o `profiles.yml` sumiu).
- `git push --force-with-lease` recusa com `stale info` quando o seu clone não conhece o estado
  atual do remoto → `git fetch origin` antes.
- Branches divergentes (commits só no GitHub e só no local): `git merge origin/main` e resolver,
  em vez de forçar por cima e perder trabalho.

## Onde ficam os segredos hoje (padrão a manter)
| Segredo | Onde vive | Como o código o lê |
|---|---|---|
| Chave privada Snowflake (pipeline) | `credential/snowflake_rsa_key.p8` (gitignorado, criptografada) | `private_key_path` |
| Passphrase da chave | variável de **usuário** do Windows | `env_var('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')`; o compose a repassa ao container |
| Chave do Metabase (Snowflake / BigQuery) | `credential/` (gitignorado) | upload na UI do Metabase |
| Chave da SA do ETL (GCP) | `credential/gerolingcp-*.json` | `keyfile:` no profile / conexão do Airflow |
| Senha do MCP Snowflake | variável de usuário `SNOWFLAKE_PASSWORD` | `${SNOWFLAKE_PASSWORD}` no `~/.claude.json` |
| Login do site FisioVet | Airflow Connection `fisioVet` (banco do Airflow) | `Connection.get_connection_from_secrets` |

Regra: **nunca** escrever um segredo literal em arquivo do repo, do chat ou de config — usar
variável de ambiente (`env_var()`, `${VAR}`) ou arquivo gitignorado.

## Pendências de segurança conhecidas
- **`AIRFLOW__CORE__FERNET_KEY` vazio**: Airflow não criptografa senhas de Connections no banco
  (a do `fisioVet` está em texto puro no Postgres). Correção planejada (ver `CLAUDE.md`).
- **Dados de negócio num repo público**: `05_reference_data.sql` contém nomes de funcionárias e
  percentuais de comissão. Não é credencial, mas é sensível. Opções: tornar o repositório
  **privado** (mais simples) ou tirar esses dados do git (e purgar o histórico).
- O MCP do Snowflake ainda usa senha (ver `CLAUDE.md`).

## Como o Claude Code te protege (e por que às vezes "trava")
O modo automático bloqueou ações sensíveis: escrever senha em arquivo ("Credential Leakage"),
`git merge`/`push` sem regra de permissão ("Modify Shared Resources"/"Out-of-Place Publication")
e editar as próprias permissões ("Self-Modification"). O jeito de liberar é **você** adicionar a
regra em `.claude/settings.local.json` — o agente não pode se autoconceder permissão.
