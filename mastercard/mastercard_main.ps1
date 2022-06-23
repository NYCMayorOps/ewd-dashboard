
function Retry-Command {
    [CmdletBinding()]
    Param(
        [Parameter(Position=0, Mandatory=$true)]
        [scriptblock]$ScriptBlock,

        [Parameter(Position=1, Mandatory=$false)]
        [int]$Maximum = 5,

        [Parameter(Position=2, Mandatory=$false)]
        [int]$Delay = 100
    )

    Begin {
        $cnt = 0
    }

    Process {
        do {
            $cnt++
            try {
                $ScriptBlock.Invoke()
                return
            } catch {
                Write-Error $_.Exception.InnerException.Message -ErrorAction Continue
                Start-Sleep -Milliseconds $Delay
            }
        } while ($cnt -lt $Maximum)

        # Throw an error after $Maximum unsuccessful invocations. Doesn't need
        # a condition, since the function returns upon successful invocation.
        throw 'Execution failed.'
    }
}
conda activate cartoframes3_3.9
Retry-Command -ScriptBlock {

cd '//chgoldfs/operations/dev_team/MayorDashboard/repo/mastercard/'
$out = python '//chgoldfs/operations/dev_team/MayorDashboard/repo/mastercard/mastercard_main.py'
Write-Host $out
} -Maximum 10
if ($LASTEXITCODE -ne 0) { pause; exit 1}