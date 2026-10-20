# Runs the pyperformance async benchmarks on unmodified CPython (main) and on the asyncio-multicore branch,
# then compares them. The branch leaves the default asyncio path untouched, so the expected result is
# "not significant" everywhere: this is the no-regression check.
#
# usage (PowerShell, from this directory):
#   .\pyperformance.ps1 -Base C:\src\cpython-main\PCbuild\amd64\python.exe `
#                       -Branch C:\src\cpython-multicore\PCbuild\amd64\python.exe
param(
    [Parameter(Mandatory = $true)][string]$Base,
    [Parameter(Mandatory = $true)][string]$Branch,
    [string]$Benchmarks = "async_tree,async_tree_cpu_io_mixed,async_tree_io,async_tree_memoization,async_generators,coroutines,asyncio_tcp,asyncio_tcp_ssl,asyncio_websockets",
    [switch]$Rigorous
)
$ErrorActionPreference = "Stop"

# pyperformance itself runs from a separate, stable Python; it builds a venv for each interpreter it measures.
if (-not (Test-Path .\.pyperf-venv)) {
    py -3.14 -m venv .pyperf-venv
    .\.pyperf-venv\Scripts\python.exe -m pip install --upgrade pip pyperformance
}
$pyperformance = ".\.pyperf-venv\Scripts\pyperformance.exe"
$extra = @()
if ($Rigorous) { $extra += "--rigorous" }

& $pyperformance run --python $Base   -b $Benchmarks @extra -o main.json
& $pyperformance run --python $Branch -b $Benchmarks @extra -o multicore.json
& $pyperformance compare main.json multicore.json --output_style table
