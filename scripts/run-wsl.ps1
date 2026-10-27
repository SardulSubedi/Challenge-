# Plain script so collector arguments are forwarded without advanced binding.
$CollectorArguments = @($args)
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    & wsl -d Ubuntu-24.04 -- bash ./scripts/run-collector.sh @CollectorArguments
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
