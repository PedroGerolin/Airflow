-- Nota fiscal (21/09/2026). Roda depois de 01..04; idempotente (IF NOT EXISTS).
-- NotaFiscal: permanente, por cliente. COM_CPF | SEM_CPF | vazio (= nao precisa de NF).
-- NFEmitidaNoCiclo: mesmo truque de NaoCobrarNoCiclo — guarda o MesReferencia do ciclo em que a NF foi emitida;
--   se for igual ao ciclo atual, a NF esta emitida; ao iniciar o mes seguinte deixa de bater e "expira sozinha".
-- Adicionar coluna no BigQuery e so metadado: as linhas existentes ficam com NULL, nada e reescrito.
ALTER TABLE `gerolingcp.FisioVet_App.contatos`
  ADD COLUMN IF NOT EXISTS NotaFiscal STRING OPTIONS (description='Precisa de NF? COM_CPF | SEM_CPF | vazio = nao precisa'),
  ADD COLUMN IF NOT EXISTS NFEmitidaNoCiclo DATE OPTIONS (description='Se igual ao MesReferencia do ciclo atual: NF ja emitida neste ciclo. Expira sozinho no ciclo seguinte');
