<#
.SYNOPSIS
    Garante que o Docker Desktop esteja rodando, sobe o Airflow via Docker,
    dispara a DAG fisiovet, espera terminar e derruba os containers de novo.
    Pensado para rodar via Agendador de Tarefas do Windows, sem precisar manter
    o Docker ligado o dia inteiro.

    "docker compose exec -T" (sem alocar TTY) e usado porque essa flag e obrigatoria
    quando o script roda sem console interativo (via Task Scheduler).

    TODA chamada ao docker passa por Invoke-Docker, que tem LIMITE DE TEMPO. Motivo: em
    21/09/2026 o motor do Docker travou depois do notebook acordar (o processo do Docker
    Desktop estava aberto, mas "docker version" nunca respondia) e o script ficou pendurado
    sem escrever nada no log. Sem limite, ate o "docker compose down" do bloco finally
    ficaria pendurado.

    IMPORTANTE: $ErrorActionPreference fica em "Continue" (nao "Stop") porque, no
    PowerShell 5.1, QUALQUER redirecionamento de stderr de um comando nativo
    (docker/airflow) - seja "2>&1", "2> $null" ou "*>>" - embrulha a saida num
    ErrorRecord/NativeCommandError. Com $ErrorActionPreference = "Stop", isso
    interrompe o script mesmo quando o comando teve sucesso (foi o que quebrou
    nos dois primeiros testes). Por isso Invoke-Docker NAO usa "2>&1": redireciona
    stdout/stderr para arquivos temporarios via Start-Process. O controle de erro real
    e feito via "throw" explicito e checagem do codigo de saida, nao por excecao
    automatica de stderr.
    Manter este arquivo so com caracteres ASCII (PowerShell 5.1 le UTF-8 sem BOM como ANSI).
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

# Roda "docker <args>" com limite de tempo. Retorna o texto (stdout + stderr) e deixa o codigo
# de saida em $script:DockerExit (-1 = estourou o tempo e o processo foi morto com a arvore).
# Os argumentos nao podem ter espacos (Start-Process junta o array sem aspas).
function Invoke-Docker {
    param([string[]]$Arguments, [int]$TimeoutSec = 120)
    $outFile = [System.IO.Path]::GetTempFileName()
    $errFile = [System.IO.Path]::GetTempFileName()
    $proc = Start-Process -FilePath "docker" -ArgumentList $Arguments -NoNewWindow -PassThru `
        -RedirectStandardOutput $outFile -RedirectStandardError $errFile
    $null = $proc.Handle   # sem isso o ExitCode pode voltar vazio no PowerShell 5.1
    if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
        taskkill /PID $proc.Id /T /F | Out-Null
        $script:DockerExit = -1
        $text = "[TIMEOUT apos ${TimeoutSec}s: docker $($Arguments -join ' ')]"
    } else {
        $script:DockerExit = $proc.ExitCode
        $text = "$(Get-Content -Path $outFile -Raw)$(Get-Content -Path $errFile -Raw)"
    }
    Remove-Item -Path $outFile, $errFile -ErrorAction SilentlyContinue
    return $text
}

function Test-DockerReady {
    $null = Invoke-Docker -Arguments @('version', '--format', '{{.Server.Version}}') -TimeoutSec 20
    return ($script:DockerExit -eq 0)
}

Set-Location $RepoDir
Write-Log "===== Iniciando execucao diaria (run_id=$RunId) ====="

try {
    if (-not (Test-DockerReady)) {
        if (Get-Process -Name "Docker Desktop" -ErrorAction SilentlyContinue) {
            Write-Log "Docker Desktop esta aberto, mas o motor nao responde. Aguardando (ate 5 min)..."
        } else {
            Write-Log "Docker Desktop nao esta rodando. Iniciando..."
            Start-Process -FilePath $DockerDesktopExe
        }

        $dockerReady = $false
        $deadline = (Get-Date).AddMinutes(5)
        while ((Get-Date) -lt $deadline) {
            Start-Sleep -Seconds 5
            if (Test-DockerReady) {
                $dockerReady = $true
                break
            }
        }
        if (-not $dockerReady) {
            throw "Docker nao respondeu em 5 minutos (motor travado?). Reinicie o Docker Desktop e rode o script de novo."
        }
        Write-Log "Docker Desktop pronto."
    } else {
        Write-Log "Docker Desktop ja estava rodando."
    }

    Write-Log "Subindo containers (docker compose up -d)"
    $up = Invoke-Docker -Arguments @('compose', 'up', '-d') -TimeoutSec 300
    Add-Content -Path $LogFile -Value $up
    if ($script:DockerExit -ne 0) {
        throw "docker compose up -d falhou (codigo $($script:DockerExit))."
    }

    Write-Log "Aguardando webserver ficar saudavel"
    $ready = $false
    $deadline = (Get-Date).AddMinutes(5)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 5
        $check = Invoke-Docker -Arguments @('compose', 'exec', '-T', 'airflow-webserver', 'airflow', 'dags', 'list-import-errors') -TimeoutSec 60
        if ($check -match "No data found") {
            $ready = $true
            break
        }
    }
    if (-not $ready) {
        throw "Webserver nao ficou pronto a tempo (esperei 5 minutos)."
    }

    Write-Log "Disparando DAG $DagId com run_id=$RunId"
    $trigger = Invoke-Docker -Arguments @('compose', 'exec', '-T', 'airflow-webserver', 'airflow', 'dags', 'trigger', $DagId, '-r', $RunId) -TimeoutSec 120
    Add-Content -Path $LogFile -Value $trigger

    Write-Log "Aguardando a execucao terminar (timeout 30 min)"
    $finished = $false
    $failed = $false
    $deadline = (Get-Date).AddMinutes(30)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 15
        $states = Invoke-Docker -Arguments @('compose', 'exec', '-T', 'airflow-webserver', 'airflow', 'tasks', 'states-for-dag-run', $DagId, $RunId) -TimeoutSec 60
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
    $finalStates = Invoke-Docker -Arguments @('compose', 'exec', '-T', 'airflow-webserver', 'airflow', 'tasks', 'states-for-dag-run', $DagId, $RunId) -TimeoutSec 60
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
    $down = Invoke-Docker -Arguments @('compose', 'down') -TimeoutSec 180
    Add-Content -Path $LogFile -Value $down
    Write-Log "===== Execucao diaria finalizada ====="
}
