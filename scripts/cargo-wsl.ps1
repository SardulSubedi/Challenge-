# Windows PowerShell consumes an unquoted -- when invoking a script. Keep a
# quoted '--' unchanged, or restore it before the first collector argument.
$CargoArguments = @($args)
if ($CargoArguments.Count -gt 0 -and $CargoArguments[0] -eq 'run' -and $CargoArguments -notcontains '--') {
    $CargoSwitches = @('--release', '-r', '--quiet', '-q', '--verbose', '-v', '-vv',
        '--locked', '--offline', '--frozen', '--all-features', '--no-default-features',
        '--ignore-rust-version', '--keep-going', '--future-incompat-report', '--timings', '--help', '-h')
    $CargoValues = @('--bin', '--example', '--package', '-p', '--features', '-F', '--profile',
        '--target', '--target-dir', '--manifest-path', '--message-format', '--color',
        '--config', '-Z', '--jobs', '-j', '--lockfile-path', '--artifact-dir')
    $ArgumentIndex = 1
    while ($ArgumentIndex -lt $CargoArguments.Count) {
        $ArgumentName = ($CargoArguments[$ArgumentIndex] -split '=', 2)[0]
        if ($CargoSwitches -contains $ArgumentName) { $ArgumentIndex++; continue }
        if ($CargoValues -contains $ArgumentName) {
            if ($CargoArguments[$ArgumentIndex] -notmatch '=') { $ArgumentIndex++ }
            $ArgumentIndex++
            continue
        }
        $CargoArguments = @($CargoArguments[0..($ArgumentIndex - 1)]) + @('--') + @($CargoArguments[$ArgumentIndex..($CargoArguments.Count - 1)])
        break
    }
}
$Distribution = 'Ubuntu-24.04'
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $NativeRootFile = Join-Path (Get-Location).Path 'work/native-wsl-root.txt'
    if (Test-Path -LiteralPath $NativeRootFile) {
        $NativeRoot = (Get-Content -LiteralPath $NativeRootFile -Raw).Trim()
        if (!$NativeRoot.StartsWith('/')) { throw 'The native WSL checkout path must be absolute.' }
        & wsl -d $Distribution --cd $NativeRoot -- bash ./scripts/cargo-wsl.sh @CargoArguments
    } else {
        & wsl -d $Distribution -- bash ./scripts/cargo-wsl.sh @CargoArguments
    }
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
