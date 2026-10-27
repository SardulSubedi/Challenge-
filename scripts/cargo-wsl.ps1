param(
    [string]$Distribution = 'Ubuntu-24.04',
    [Parameter(Position = 0, ValueFromRemainingArguments = $true)]
    [string[]]$CargoArguments
)
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    & wsl -d $Distribution -- bash ./scripts/cargo-wsl.sh @CargoArguments
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
