# scripts/schedule_rnm.ps1
# Wrapper pour la task planifiée Windows — Refresh hebdomadaire prix RNM
# Exécution : vendredi 7 AM via schtasks

$ProjectRoot = "C:\Users\Admin\Documents\Claude\Projects\Outils Maraîchage\agriTools"
$LogDir = "$ProjectRoot\datalake\logs"
$LogFile = "$LogDir\rnm_refresh_$(Get-Date -Format 'yyyy-MM-dd_HHmmss').log"

# Créer le dossier logs s'il n'existe pas
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

# Log démarrage
$Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Write-Output "[$Timestamp] Démarrage refresh RNM FranceAgriMer" | Tee-Object -FilePath $LogFile -Append

# Exécuter la commande
cd $ProjectRoot
& uv run python -m ingestion.prix.rnm 2>&1 | Tee-Object -FilePath $LogFile -Append

# Log fin
$Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Write-Output "[$Timestamp] Fin refresh RNM (exit code: $LASTEXITCODE)" | Tee-Object -FilePath $LogFile -Append
