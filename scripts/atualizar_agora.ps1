<#
.SYNOPSIS
    "Atualizar dados agora": dispara a MESMA tarefa agendada das 06:00 (Airflow_FisioVet_Daily), acompanha o
    log e mostra o resultado. Serve para trazer uma baixa que voce acabou de dar no sistema para a fila de
    cobranca sem esperar o dia seguinte. Leva uns 4 a 6 minutos (sobe o Docker, roda a DAG e derruba).

    Uso normal: atalho na area de trabalho (criado por scripts/create_desktop_shortcut.ps1).
    Teste/automacao: powershell -File scripts/atualizar_agora.ps1 -NoPause

    Se ja houver uma atualizacao em andamento (a das 06:00, por exemplo), NAO dispara outra: so acompanha.
    Manter este arquivo so com caracteres ASCII (PowerShell 5.1 le UTF-8 sem BOM como ANSI).
#>
param([switch]$NoPause)

$ErrorActionPreference = "Continue"
$TaskName = "Airflow_FisioVet_Daily"
$LogFile = "C:\Airflow\logs\daily_pipeline.log"
$TimeoutMin = 40

function Read-NewLog {
    param([int]$Skip)
    if (-not (Test-Path $LogFile)) { return @() }
    $all = @(Get-Content -Path $LogFile -ErrorAction SilentlyContinue)
    if ($all.Count -le $Skip) { return @() }
    return $all[$Skip..($all.Count - 1)]
}

$info = Get-ScheduledTaskInfo -TaskName $TaskName
$running = ($info.LastTaskResult -eq 267009)
if (-not $running) {
    $skip = @(Get-Content -Path $LogFile -ErrorAction SilentlyContinue).Count
    Write-Host "Iniciando a atualizacao (mesma execucao das 06:00)..." -ForegroundColor Cyan
    Start-ScheduledTask -TaskName $TaskName
} else {
    # ja rodando: mostra so o que ainda vai ser escrito a partir de agora
    $skip = @(Get-Content -Path $LogFile -ErrorAction SilentlyContinue).Count
    Write-Host "Ja ha uma atualizacao em andamento. Acompanhando..." -ForegroundColor Yellow
}

$inicio = Get-Date
$resultado = $null
$terminou = $false
while (((Get-Date) - $inicio).TotalMinutes -lt $TimeoutMin) {
    Start-Sleep -Seconds 5
    $novas = @(Read-NewLog -Skip $skip)
    $skip += $novas.Count      # avanca o ponteiro: cada linha do log e mostrada uma vez so
    foreach ($linha in $novas) {
        if ($linha -match "RESULTADO:") { $resultado = $linha }
        if ($linha -match "^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} - (Subindo|Aguardando|Disparando|Derrubando|RESULTADO|ERRO|=====)") {
            Write-Host $linha
        }
        if ($linha -match "Execucao diaria finalizada") { $terminou = $true }
    }
    if ($terminou) { break }
}

$minutos = [math]::Round(((Get-Date) - $inicio).TotalMinutes, 1)
Write-Host ""
if (-not $terminou) {
    Write-Host "Passou de $TimeoutMin minutos sem terminar. Veja o log: $LogFile" -ForegroundColor Red
} elseif ($resultado -match "SUCESSO") {
    Write-Host "PRONTO em $minutos min: dados atualizados. No app, clique em 'Atualizar dados'." -ForegroundColor Green
} else {
    Write-Host "A atualizacao NAO terminou bem ($resultado). Veja o log: $LogFile" -ForegroundColor Red
}
if (-not $NoPause) { Read-Host "Pressione Enter para fechar" | Out-Null }
