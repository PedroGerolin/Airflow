<#
.SYNOPSIS
    Sobe o Airflow via Docker, dispara a DAG fisiovet, espera terminar e derruba
    os containers de novo. Pensado para rodar via Agendador de Tarefas do Windows,
    sem precisar manter o Docker ligado o dia inteiro.

    "docker compose exec -T" (sem alocar TTY) e usado porque essa flag e obrigatoria
    quando o script roda sem console interativo (via Task Scheduler).

    IMPORTANTE: nunca usar "2>&1" ou "2>$null" em comandos nativos (docker/airflow)
    aqui - no PowerShell 5.1, isso envolve a saida em um ErrorRecord e, combinado
    com $ErrorActionPreference = "Stop", interrompe o script mesmo quando o comando
    teve sucesso (ex: o aviso benigno "AIRFLOW_UID not set" do docker compose).
#>

$ErrorActionPreference = "Stop"
$RepoDir = "C:\Airflow"
$LogFile = "$RepoDir\logs\daily_pipeline.log"
$DagId = "fisiovet"
$RunId = "scheduled_daily__" + (Get-Date -Format "yyyy-MM-ddTHH-mm-ss")

function Write-Log {
    param([string]$Message)
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - $Message"
    Add-Content -Path $LogFile -Value $line
}

Set-Location $RepoDir
Write-Log "===== Iniciando execucao diaria (run_id=$RunId) ====="

try {
    Write-Log "Subindo containers (docker compose up -d)"
    docker compose up -d | Out-String | Add-Content -Path $LogFile

    Write-Log "Aguardando webserver ficar saudavel"
    $ready = $false
    for ($i = 0; $i -lt 36; $i++) {
        Start-Sleep -Seconds 5
        $check = docker compose exec -T airflow-webserver airflow dags list-import-errors
        if ($check -match "No data found") {
            $ready = $true
            break
        }
    }
    if (-not $ready) {
        throw "Webserver nao ficou pronto a tempo (esperei 3 minutos)."
    }

    Write-Log "Disparando DAG $DagId com run_id=$RunId"
    docker compose exec -T airflow-webserver airflow dags trigger $DagId -r $RunId | Out-String | Add-Content -Path $LogFile

    Write-Log "Aguardando a execucao terminar (timeout 30 min)"
    $finished = $false
    $failed = $false
    for ($i = 0; $i -lt 120; $i++) {
        Start-Sleep -Seconds 15
        $states = docker compose exec -T airflow-webserver airflow tasks states-for-dag-run $DagId $RunId
        if ($states -match "end_task\s*\|\s*success") {
            $finished = $true
            break
        }
        if ($states -match "\|\s*failed\s*\|") {
            $failed = $true
            break
        }
    }

    Write-Log "----- Estado final das tasks -----"
    $finalStates = docker compose exec -T airflow-webserver airflow tasks states-for-dag-run $DagId $RunId
    Add-Content -Path $LogFile -Value ($finalStates | Out-String)

    if ($failed) {
        Write-Log "RESULTADO: FALHOU (pelo menos uma task com estado 'failed')"
    } elseif ($finished) {
        Write-Log "RESULTADO: SUCESSO"
    } else {
        Write-Log "RESULTADO: TIMEOUT (30 min) - verificar manualmente"
    }
}
catch {
    Write-Log "ERRO: $($_.Exception.Message)"
}
finally {
    Write-Log "Derrubando containers (docker compose down)"
    docker compose down | Out-String | Add-Content -Path $LogFile
    Write-Log "===== Execucao diaria finalizada ====="
}
