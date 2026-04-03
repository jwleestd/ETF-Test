@echo off
set GIT_DIR=C:\gitmeta\ETF-Test.git
set WORK_TREE=E:\??\ETF-TEST

git --git-dir "%GIT_DIR%" --work-tree "%WORK_TREE%" pull
if errorlevel 1 (
  echo [ERROR] git pull failed
  exit /b 1
)

git --git-dir "%GIT_DIR%" --work-tree "%WORK_TREE%" push
if errorlevel 1 (
  echo [ERROR] git push failed
  exit /b 1
)

echo [OK] Sync completed
