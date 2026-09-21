-- Historico de ciclos (21/09/2026). Roda depois de 01..06; idempotente.
-- Problema: NFEmitidaNoCiclo e NaoCobrarNoCiclo guardam so o ULTIMO ciclo (a data e sobrescrita no ciclo seguinte), e
-- "quem ainda devia no fim do ciclo / quem quitou" nunca foi gravado. `envios` ja guarda tudo dos envios.
-- Solucao: ao INICIAR o ciclo seguinte, o app grava uma "foto" do ciclo anterior aqui (uma linha por cliente que
-- devia, foi cobrado ou teve NF/pausa naquele ciclo) e marca ciclos.FechadoEm. Nada e apagado.
ALTER TABLE `gerolingcp.FisioVet_App.ciclos`
  ADD COLUMN IF NOT EXISTS FechadoEm TIMESTAMP OPTIONS (description='Quando o ciclo foi encerrado (ao iniciar o proximo): a foto foi gravada em ciclos_historico');

CREATE TABLE IF NOT EXISTS `gerolingcp.FisioVet_App.ciclos_historico` (
  MesReferencia DATE NOT NULL OPTIONS(description='Ciclo fotografado (dia 1 do mes)'),
  CodigoCliente INT64 NOT NULL,
  NomeCliente STRING OPTIONS(description='Nome no cadastro na hora da foto'),
  QtdEnvios INT64 OPTIONS(description='Quantas mensagens foram registradas como enviadas neste ciclo'),
  PrimeiraCobrancaEm TIMESTAMP,
  UltimaCobrancaEm TIMESTAMP,
  UltimaMensagem STRING OPTIONS(description='Nome do modelo da ultima mensagem enviada'),
  ValorCobrado NUMERIC OPTIONS(description='Valor da primeira cobranca do ciclo (ValorNoEnvio)'),
  EmAbertoNoFechamento NUMERIC OPTIONS(description='Quanto ainda devia quando o ciclo foi encerrado (0 = quitou)'),
  SituacaoFinal STRING OPTIONS(description='EM_ABERTO (ainda devia) | QUITADO (nao devia mais ao encerrar)'),
  EstadoFila STRING OPTIONS(description='Estado na fila ao encerrar (A_COBRAR, COBRADO, NAO_COBRAR_NO_CICLO, INCOBRAVEL) ou NULL se quitado'),
  NotaFiscal STRING OPTIONS(description='COM_CPF | SEM_CPF | NULL (nao precisa) na hora da foto'),
  NFEmitida BOOL OPTIONS(description='A NF deste ciclo foi marcada como emitida'),
  NaoCobrarNoCiclo BOOL OPTIONS(description='Estava marcado "devendo, nao cobrar" neste ciclo'),
  Incobravel BOOL OPTIONS(description='Estava marcado como incobravel'),
  FechadoEm TIMESTAMP NOT NULL OPTIONS(description='Quando a foto foi gravada')
) OPTIONS(description='Foto de cada ciclo encerrado: quem foi cobrado, quantas vezes, NF, quem quitou. So insercao');
