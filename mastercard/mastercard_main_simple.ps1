
conda activate cartoframes3_3.9
cd '//chgoldfs/operations/dev_team/MayorDashboard/repo/mastercard/'
$out = python '//chgoldfs/operations/dev_team/MayorDashboard/repo/mastercard/mastercard_main.py'
if ($LASTEXITCODE -ne 0) { Write-Host $out; pause; exit 1}