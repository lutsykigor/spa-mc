# Builds the two interpreters to compare, from your CPython fork, as release builds (never benchmark debug builds):
#   <Root>\cpython-main       the commit the branch started from (unmodified CPython)
#   <Root>\cpython-multicore  the asyncio-multicore branch
# and, with -FreeThreaded, a free-threaded build of main for the optional threads scenario.
# Then runs test_asyncio on both, so a broken build is caught before any benchmark.
#
# Needs Git and Visual Studio 2022 (Desktop development with C++). usage:
#   .\build.ps1 -Root C:\src [-FreeThreaded]
param(
    [string]$Root = "C:\src",
    [string]$Fork = "https://github.com/lutsykigor/cpython.git",
    [string]$Branch = "asyncio-multicore",
    [switch]$FreeThreaded
)
$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force $Root | Out-Null
$multicore = Join-Path $Root "cpython-multicore"
$main = Join-Path $Root "cpython-main"

if (-not (Test-Path $multicore)) { git clone --branch $Branch $Fork $multicore }
git -C $multicore fetch origin
git -C $multicore checkout $Branch
git -C $multicore pull --ff-only
$base = (git -C $multicore merge-base HEAD origin/main).Trim()
if (-not (Test-Path $main)) { git -C $multicore worktree add $main $base } else { git -C $main checkout $base }

foreach ($tree in @($main, $multicore)) {
    & "$tree\PCbuild\build.bat" -e -p x64
    if ($LASTEXITCODE) { throw "build failed in $tree" }
}
if ($FreeThreaded) {
    & "$main\PCbuild\build.bat" -e -p x64 --disable-gil
    if ($LASTEXITCODE) { throw "free-threaded build failed" }
}

foreach ($tree in @($main, $multicore)) {
    & "$tree\PCbuild\amd64\python.exe" -m test test_asyncio -j4
    if ($LASTEXITCODE) { throw "test_asyncio failed in $tree" }
}
Write-Host "main:      $main\PCbuild\amd64\python.exe"
Write-Host "multicore: $multicore\PCbuild\amd64\python.exe"
if ($FreeThreaded) { Write-Host "free-threaded: $main\PCbuild\amd64\python3.16t.exe" }
