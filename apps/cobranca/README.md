# App de cobrança (Streamlit)

Fila de cobrança mensal + cadastro de contatos, sobre tabelas do BigQuery (`FisioVet_App`, escritas pelo
app) e as views do dbt (`cobranca_pendencias`, `cobranca_sessoes`). Desenho e regras de negócio: `ROADMAP.md`
nas anotações do usuário (fora do repo). **Nenhum dado de cliente fica neste repositório**, só código.

## Rodar
```bash
cd apps/cobranca
docker compose up -d --build      # http://localhost:8501  (só neste computador: 127.0.0.1)
docker compose logs -f cobranca   # ver erros
docker compose down
```
Pré-requisito: a chave `credential/cobranca-app.json` (gerada em `gcp_setup/README.md`, seção 4).

## Testes
```bash
docker compose run --rm cobranca python tests/test_repo.py   # camada de dados (BigQuery real, dados de mentira)
docker compose run --rm cobranca python tests/test_app.py    # tela sem navegador (AppTest)
```
Os testes de integração criam dados de mentira (cliente `-1`, mês `1999-01`) e apagam no fim; os que
precisam de um ciclo real só rodam se ainda **não** houver ciclo iniciado.

## Estrutura
- `repo.py`: toda a conversa com o BigQuery (queries parametrizadas; só DML, nunca streaming insert).
- `app.py`: só a tela. Abas **Fila de cobrança** (ações em massa, contato do cliente, sessões) e **Contatos**
  (edição em tabela).
- Versão do Streamlit **fixada** em `requirements.txt` (a API de tabelas com seleção muda entre versões).

## O que a v1 NÃO faz (v2)
Modelos de mensagem, seleção da mensagem por cliente, links `wa.me` e registro de envios (`envios`).
