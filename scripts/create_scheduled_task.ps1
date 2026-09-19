<#
.SYNOPSIS
    Cria a tarefa agendada "Airflow_FisioVet_Daily" que roda a DAG fisiovet
    todo dia as 06:00, acordando o notebook da suspensao se necessario.
    PRECISA ser executado num PowerShell como Administrador.
#>

$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"C:\Airflow\scripts\run_daily_pipeline.ps1`""

$trigger = New-ScheduledTaskTrigger -Daily -At "06:00"

# -AllowStartIfOnBatteries e -DontStopIfGoingOnBatteries sao obrigatorios: por padrao o
# New-ScheduledTaskSettingsSet cria a tarefa com DisallowStartIfOnBatteries=True, e uma
# execucao perdida por estar na bateria NAO faz catch-up (o Agendador ja reprograma pro
# dia seguinte). Foi exatamente o que aconteceu em 19/09/2026.
$settings = New-ScheduledTaskSettingsSet `
    -WakeToRun `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -DontStopOnIdleEnd `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1)

$principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive `
    -RunLevel Highest

Register-ScheduledTask -TaskName "Airflow_FisioVet_Daily" `
    -Action $action -Trigger $trigger -Settings $settings -Principal $principal `
    -Description "Sobe o Docker, roda a DAG fisiovet (BigQuery+Snowflake) e derruba os containers." `
    -Force

Write-Host "--- Tarefa criada ---"
Get-ScheduledTask -TaskName "Airflow_FisioVet_Daily" | Select-Object TaskName, State
