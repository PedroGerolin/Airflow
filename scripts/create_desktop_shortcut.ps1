<#
.SYNOPSIS
    Cria na area de trabalho o atalho "Atualizar dados da cobranca", que roda scripts/atualizar_agora.ps1.
    Rodar uma vez (nao precisa ser Administrador): powershell -File scripts/create_desktop_shortcut.ps1
    Manter este arquivo so com caracteres ASCII.
#>
$desktop = [Environment]::GetFolderPath("Desktop")
$lnkPath = Join-Path $desktop "Atualizar dados da cobranca.lnk"
$shell = New-Object -ComObject WScript.Shell
$s = $shell.CreateShortcut($lnkPath)
$s.TargetPath = "powershell.exe"
$s.Arguments = '-NoProfile -ExecutionPolicy Bypass -File "C:\Airflow\scripts\atualizar_agora.ps1"'
$s.WorkingDirectory = "C:\Airflow"
$s.Description = "Traz para a fila de cobranca as baixas e vendas novas (roda a mesma execucao das 06:00)"
$s.IconLocation = "$env:SystemRoot\System32\shell32.dll,238"
$s.Save()
"Atalho criado em: $lnkPath"
