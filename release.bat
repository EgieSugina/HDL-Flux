@echo off
setlocal EnableExtensions
cd /d "%~dp0"

set "VERSION=%~1"
if "%VERSION%"=="" (
  echo Usage: release.bat vX.Y.Z
  exit /b 1
)

echo %VERSION% | findstr /b /c:"v" >nul
if errorlevel 1 (
  echo Error: version tag must start with 'v' ^(example: v1.0.0^)
  exit /b 1
)

where git >nul 2>&1
if errorlevel 1 (
  echo Error: git not found.
  exit /b 1
)

git remote get-url origin >nul 2>&1
if errorlevel 1 (
  echo Error: git remote 'origin' not found.
  exit /b 1
)

for /f %%i in ('git status --porcelain') do (
  echo Error: working tree is not clean. Commit or stash changes first.
  exit /b 1
)

git rev-parse "%VERSION%" >nul 2>&1
if not errorlevel 1 (
  echo Error: tag '%VERSION%' already exists locally.
  exit /b 1
)

git ls-remote --exit-code --tags origin "refs/tags/%VERSION%" >nul 2>&1
if not errorlevel 1 (
  echo Error: tag '%VERSION%' already exists on origin.
  exit /b 1
)

echo [release] creating tag %VERSION%
git tag -a "%VERSION%" -m "Release %VERSION%"
if errorlevel 1 exit /b 1

echo [release] pushing current branch
git push origin HEAD
if errorlevel 1 exit /b 1

echo [release] pushing tag %VERSION%
git push origin "%VERSION%"
if errorlevel 1 exit /b 1

echo.
echo Done. GitHub Actions release workflow should start automatically for tag '%VERSION%'.
endlocal
