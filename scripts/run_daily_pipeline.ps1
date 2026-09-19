<#
.SYNOPSIS
    Garante que o Docker Desktop esteja rodando, sobe o Airflow via Docker,
    dispara a DAG fisiovet, espera terminar e derruba os containers de novo.
    Pensado para rodar via Agendador de Tarefas do Windows, sem precisar manter
    o Docker ligado o dia inteiro.

    "docker compose exec -T" (sem alocar TTY) e usado porque essa flag e obrigatoria
    quando o script roda sem console interativo (via Task Scheduler).

    IMPORTANTE: $ErrorActionPreference fica em "Continue" (nao "Stop") porque, no
    PowerShell 5.1, QUALQUER redirecionamento de stderr de um comando nativo
    (docker/airflow) - seja "2>&1", "2> $null" ou "*>>" - embrulha a saida num
    ErrorRecord/NativeCommandError. Com $ErrorActionPreference = "Stop", isso
    interrompe o script mesmo quando o comando teve sucesso (foi o que quebrou
    nos dois primeiros testes). O controle de erro real e feito via "throw"
    explicito (que sempre interrompe, independente do ErrorActionPreference) e
    checagem de $LASTEXITCODE, nao por excecao automatica de stderr.
#>

$ErrorActionPreference = "Continue"
$RepoDir = "C:\Airflow"
$LogFile = "$RepoDir\logs\daily_pipeline.log"
$DagId = "fisiovet"
$RunId = "scheduled_daily__" + (Get-Date -Format "yyyy-MM-ddTHH-mm-ss")
$DockerDesktopExe = "C:\Program Files\Docker\Docker\Docker Desktop.exe"

# O Agendador de Tarefas pode nao herdar variaveis de ambiente de usuario criadas depois do
# logon. Carrega explicitamente do registro (escopo User) as que o docker-compose repassa
# aos containers (dbt_run_snowflake autentica no Snowflake com chave + essa passphrase).
foreach ($name in @('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')) {
    if (-not [Environment]::GetEnvironmentVariable($name, 'Process')) {
        $value = [Environment]::GetEnvironmentVariable($name, 'User')
        if ($value) { [Environment]::SetEnvironmentVariable($name, $value, 'Process') }
    }
}

function Write-Log {
    param([string]$Message)
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - $Message"
    Add-Content -Path $LogFile -Value $line
}

function Test-DockerReady {
    docker version 2>&1 | Out-String | Add-Content -Path $LogFile
    return ($LASTEXITCODE -eq 0)
}

Set-Location $RepoDir
Write-Log "===== Iniciando execucao diaria (run_id=$RunId) ====="

try {
    if (-not (Test-DockerReady)) {
        Write-Log "Docker Desktop nao esta rodando. Iniciando..."
        Start-Process -FilePath $DockerDesktopExe

        $dockerReady = $false
        for ($i = 0; $i -lt 60; $i++) {
            Start-Sleep -Seconds 5
            if (Test-DockerReady) {
                $dockerReady = $true
                break
            }
        }
        if (-not $dockerReady) {
            throw "Docker Desktop nao ficou pronto a tempo (esperei 5 minutos)."
        }
        Write-Log "Docker Desktop pronto."
    } else {
        Write-Log "Docker Desktop ja estava rodando."
    }

    Write-Log "Subindo containers (docker compose up -d)"
    docker compose up -d 2>&1 | Out-String | Add-Content -Path $LogFile

    Write-Log "Aguardando webserver ficar saudavel"
    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        Start-Sleep -Seconds 5
        $check = docker compose exec -T airflow-webserver airflow dags list-import-errors 2>&1 | Out-String
        if ($check -match "No data found") {
            $ready = $true
            break
        }
    }
    if (-not $ready) {
        throw "Webserver nao ficou pronto a tempo (esperei 5 minutos)."
    }

    Write-Log "Disparando DAG $DagId com run_id=$RunId"
    docker compose exec -T airflow-webserver airflow dags trigger $DagId -r $RunId 2>&1 | Out-String | Add-Content -Path $LogFile

    Write-Log "Aguardando a execucao terminar (timeout 30 min)"
    $finished = $false
    $failed = $false
    for ($i = 0; $i -lt 120; $i++) {
        Start-Sleep -Seconds 15
        $states = docker compose exec -T airflow-webserver airflow tasks states-for-dag-run $DagId $RunId 2>&1 | Out-String
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
    $finalStates = docker compose exec -T airflow-webserver airflow tasks states-for-dag-run $DagId $RunId 2>&1 | Out-String
    Add-Content -Path $LogFile -Value $finalStates

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
    docker compose down 2>&1 | Out-String | Add-Content -Path $LogFile
    Write-Log "===== Execucao diaria finalizada ====="
}
