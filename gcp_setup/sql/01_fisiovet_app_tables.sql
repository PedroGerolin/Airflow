-- Dataset FisioVet_App: tabelas ESCRITAS PELO APP de cobranca (Streamlit). O dbt nao mexe aqui.
-- Rodar uma vez, com a conta owner:  bq query --project_id=gerolingcp --location=US --nouse_legacy_sql "<conteudo>"
-- Chaves (PK/FK) sao logicas: o BigQuery nao as impoe. Quem evita duplicata e o app, com MERGE.
-- Desenho completo: ROADMAP.md (secao "Cobranca por WhatsApp"), na pasta de estudos do usuario.

CREATE TABLE IF NOT EXISTS `gerolingcp.FisioVet_App.contatos` (
  CodigoCliente INT64 NOT NULL OPTIONS(description='PK logica. Mesmo codigo do Simples Vet (clients.Codigo)'),
  NomeContato STRING OPTIONS(description='Quem recebe a cobranca (o proprio cliente, familiar, secretaria...)'),
  TelefoneWhatsapp STRING OPTIONS(description='So digitos com DDI, ex.: 5511999999999'),
  Situacao STRING NOT NULL OPTIONS(description='ATIVO ou INCOBRAVEL. Incobravel some da fila'),
  NaoCobrarNoCiclo DATE OPTIONS(description='Se igual ao MesReferencia do ciclo atual: devendo, nao cobrar. Expira sozinho no ciclo seguinte'),
  Observacao STRING OPTIONS(description='Motivo / anotacao livre'),
  RevisadoEm TIMESTAMP OPTIONS(description='Nulo = contato ainda nao conferido pelo usuario'),
  AtualizadoEm TIMESTAMP
) OPTIONS(description='Contato de cobranca por cliente (1 linha por cliente)');

CREATE TABLE IF NOT EXISTS `gerolingcp.FisioVet_App.ciclos` (
  MesReferencia DATE NOT NULL OPTIONS(description='PK logica. Dia 1 do mes cobrado (2026-08-01 = cobranca de Agosto)'),
  IniciadoEm TIMESTAMP NOT NULL OPTIONS(description='Quando o usuario clicou em Iniciar cobranca. O ciclo mais recente define o corte')
) OPTIONS(description='Meses cuja cobranca foi iniciada');

CREATE TABLE IF NOT EXISTS `gerolingcp.FisioVet_App.mensagens` (
  Nome STRING NOT NULL OPTIONS(description='PK logica. Ex.: Inicial, Lembrete'),
  Texto STRING NOT NULL OPTIONS(description='Marcadores: {nome_contato} {nome_cliente} {mes} {lista_sessoes} {total} {pix}'),
  EhInicial BOOL NOT NULL OPTIONS(description='Mensagem padrao de quem ainda nao foi cobrado no ciclo (so uma verdadeira)'),
  Ativo BOOL NOT NULL,
  AtualizadoEm TIMESTAMP
) OPTIONS(description='Modelos de mensagem editaveis na tela');

CREATE TABLE IF NOT EXISTS `gerolingcp.FisioVet_App.envios` (
  EnvioId STRING NOT NULL OPTIONS(description='PK logica (uuid)'),
  CodigoCliente INT64 NOT NULL OPTIONS(description='FK logica -> contatos / clients'),
  MesReferencia DATE NOT NULL OPTIONS(description='FK logica -> ciclos (a rodada de cobranca)'),
  MensagemNome STRING NOT NULL OPTIONS(description='FK logica -> mensagens.Nome'),
  EnviadoEm TIMESTAMP NOT NULL,
  NomeContato STRING OPTIONS(description='Copia do contato no momento do envio'),
  TelefoneWhatsapp STRING OPTIONS(description='Copia do telefone no momento do envio'),
  ValorNoEnvio NUMERIC OPTIONS(description='Quanto foi cobrado naquele dia'),
  TextoEnviado STRING OPTIONS(description='Texto final; nao muda se o modelo for editado depois')
) OPTIONS(description='Historico de cobrancas enviadas. So insercao');

CREATE TABLE IF NOT EXISTS `gerolingcp.FisioVet_App.configuracoes` (
  Chave STRING NOT NULL OPTIONS(description='PK logica. Ex.: pix'),
  Valor STRING,
  AtualizadoEm TIMESTAMP
) OPTIONS(description='Pares chave/valor (ex.: pix = CNPJ). Fica fora do repo publico');
