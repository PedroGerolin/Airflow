{{
    config(
        materialized='view',
        schema='Analytics',
        enabled=(target.type == 'bigquery')
    )
}}
{# FILA DE COBRANCA: uma linha por cliente que deve algo ate o corte do ciclo atual (ver cobranca_sessoes).
   O estado (EstadoFila) e DERIVADO — nada de estado gravado alem de contatos/ciclos/envios (escritos pelo app):
     INCOBRAVEL           Situacao = 'INCOBRAVEL' (o app esconde por padrao, mas o total continua visivel)
     NAO_COBRAR_NO_CICLO  contatos.NaoCobrarNoCiclo = ciclo atual (expira sozinho no ciclo seguinte)
     COBRADO              ja tem ao menos 1 envio NESTE ciclo
     A_COBRAR             nenhum envio neste ciclo
   NomeCliente vem do CADASTRO (clients, refeito por inteiro a cada execucao), nao das vendas: as vendas antigas
   (fora da janela da exportacao) guardam o nome congelado de quando sairam da janela. So cai no nome das vendas
   se o cliente nao existir no cadastro.
   Nota fiscal: NotaFiscal (COM_CPF | SEM_CPF | NULL = nao precisa) e NFStatus (NULL | PENDENTE | EMITIDA), este
   derivado como o "nao cobrar": NFEmitidaNoCiclo = ciclo atual => EMITIDA; ao iniciar o mes seguinte volta a PENDENTE.
   TemCPF e so um sim/nao (o CPF em si NAO sai nesta view: o Metabase le este dataset).
   Contato: usa contatos (app) e, se o cliente nao tiver linha la, o telefone do cadastro do Simples Vet e o
   PRIMEIRO NOME do cliente. NomeContato = "como chamar" na mensagem ({nome_contato}). Animais = os animais com
   sessao em aberto (marcador {animais}; o app junta com " e "). #}
WITH sessoes AS (
    SELECT
        CodigoCliente,
        ANY_VALUE(NomeCliente) AS NomeCliente,
        ANY_VALUE(MesCiclo) AS MesCiclo,
        STRING_AGG(DISTINCT NomeAnimal, ', ' ORDER BY NomeAnimal) AS Animais,
        COUNT(*) AS QtdSessoes,
        SUM(Valor) AS TotalEmAberto,
        LOGICAL_OR(Parcial) AS TemBaixaParcial,
        MIN(MesSessao) AS MesMaisAntigo,
        MIN(DataSessao) AS SessaoMaisAntiga
    FROM {{ ref('cobranca_sessoes') }}
    GROUP BY CodigoCliente
),
cadastro AS (
    SELECT
        CAST(Codigo AS INT64) AS CodigoCliente,
        Nome,
        COALESCE(REGEXP_CONTAINS(CPF, r'^\d{11}$'), FALSE) AS TemCPF,
        CONCAT('55', REGEXP_REPLACE(REGEXP_EXTRACT(Telefone, r'(\(\d{2}\)\s*9\d{4}-?\d{4})'), r'\D', '')) AS TelefoneCadastro
    FROM {{ ref('clients') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Codigo ORDER BY Nome) = 1
),
envios AS (
    SELECT
        E.CodigoCliente,
        COUNTIF(E.MesReferencia = C.MesCiclo) AS QtdEnviosCiclo,
        MIN(IF(E.MesReferencia = C.MesCiclo, E.EnviadoEm, NULL)) AS PrimeiraCobrancaCiclo,
        MAX(IF(E.MesReferencia = C.MesCiclo, E.EnviadoEm, NULL)) AS UltimaCobrancaCiclo,
        ARRAY_AGG(IF(E.MesReferencia = C.MesCiclo, E.MensagemNome, NULL) IGNORE NULLS ORDER BY E.EnviadoEm DESC LIMIT 1)[SAFE_OFFSET(0)] AS UltimaMensagemCiclo,
        MAX(E.EnviadoEm) AS UltimaCobrancaQualquerCiclo
    FROM {{ source('FisioVet_App', 'envios') }} E
    CROSS JOIN (SELECT MAX(MesReferencia) AS MesCiclo FROM {{ source('FisioVet_App', 'ciclos') }}) C
    GROUP BY E.CodigoCliente
)
SELECT
    P.CodigoCliente,
    COALESCE(K.Nome, P.NomeCliente) AS NomeCliente,
    P.MesCiclo,
    P.Animais,
    P.QtdSessoes,
    P.TotalEmAberto,
    P.TemBaixaParcial,
    P.MesMaisAntigo,
    P.SessaoMaisAntiga,
    COALESCE(T.NomeContato, INITCAP(SPLIT(TRIM(COALESCE(K.Nome, P.NomeCliente)), ' ')[SAFE_OFFSET(0)])) AS NomeContato,
    COALESCE(T.TelefoneWhatsapp, K.TelefoneCadastro) AS TelefoneWhatsapp,
    COALESCE(REGEXP_CONTAINS(COALESCE(T.TelefoneWhatsapp, K.TelefoneCadastro), r'^55\d{10,11}$'), FALSE) AS TelefoneValido,
    IF(T.CodigoCliente IS NOT NULL, 'APP', 'CADASTRO') AS OrigemContato,
    T.RevisadoEm IS NOT NULL AS ContatoRevisado,
    COALESCE(T.Situacao, 'ATIVO') AS Situacao,
    T.Observacao,
    T.NotaFiscal,
    CASE
        WHEN T.NotaFiscal IS NULL THEN NULL
        WHEN T.NFEmitidaNoCiclo = P.MesCiclo THEN 'EMITIDA'
        ELSE 'PENDENTE'
    END AS NFStatus,
    COALESCE(K.TemCPF, FALSE) AS TemCPF,
    CASE
        WHEN COALESCE(T.Situacao, 'ATIVO') = 'INCOBRAVEL' THEN 'INCOBRAVEL'
        WHEN T.NaoCobrarNoCiclo = P.MesCiclo THEN 'NAO_COBRAR_NO_CICLO'
        WHEN COALESCE(E.QtdEnviosCiclo, 0) > 0 THEN 'COBRADO'
        ELSE 'A_COBRAR'
    END AS EstadoFila,
    COALESCE(E.QtdEnviosCiclo, 0) AS QtdEnviosCiclo,
    E.PrimeiraCobrancaCiclo,
    E.UltimaCobrancaCiclo,
    E.UltimaMensagemCiclo,
    E.UltimaCobrancaQualquerCiclo,
    DATE_DIFF(CURRENT_DATE('America/Sao_Paulo'), DATE(E.UltimaCobrancaCiclo, 'America/Sao_Paulo'), DAY) AS DiasDesdeUltimaCobranca,
    (SELECT MAX(date) FROM {{ ref('sales') }}) AS DadosAteData
FROM sessoes P
LEFT JOIN {{ source('FisioVet_App', 'contatos') }} T ON T.CodigoCliente = P.CodigoCliente
LEFT JOIN cadastro K ON K.CodigoCliente = P.CodigoCliente
LEFT JOIN envios E ON E.CodigoCliente = P.CodigoCliente
