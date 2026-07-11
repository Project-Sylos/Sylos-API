# Deprecated: use POST /api/admin/clear-migrations or POST /api/admin/wipe-install (admin JWT required).
# This script will delete all data from the server and start fresh.

Write-Output "Cleaning api data..."

# Step one: delete all migration folders in the data/ folder, but not api-runtime.log or migrations.yaml
Get-ChildItem -Path "data/" | Where-Object {
    $_.PSIsContainer -or ($_.Name -ne "api-runtime.log" -and $_.Name -ne "migrations.yaml" -and $_.Name -ne "sylos.duckdb")
} | Remove-Item -Recurse -Force

# step two, clear the log file api-runtime.log
Clear-Content -Path "data/api-runtime.log"

# step three, clear the migrations.yaml file to where it's just 'migrations: {new line}'
Set-Content -Path "data/migrations.yaml" -Value "migrations:" -Encoding UTF8

Write-Output "Done cleaning api data."