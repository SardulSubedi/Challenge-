# Plain script so collector arguments are forwarded without advanced binding.
$CollectorArguments = @($args)
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $NativeRootFile = Join-Path (Get-Location).Path 'work/native-wsl-root.txt'
    if (Test-Path -LiteralPath $NativeRootFile) {
        $NativeRoot = (Get-Content -LiteralPath $NativeRootFile -Raw).Trim()
        if (!$NativeRoot.StartsWith('/')) { throw 'The native WSL checkout path must be absolute.' }
        & wsl -d Ubuntu-24.04 --cd $NativeRoot -- bash ./scripts/run-collector.sh @CollectorArguments
    } else {
        & wsl -d Ubuntu-24.04 -- bash ./scripts/run-collector.sh @CollectorArguments
    }
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
