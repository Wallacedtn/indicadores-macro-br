@echo off
setlocal

REM Vai para a pasta do projeto
cd /d C:\Dev\tesouro

REM Ativa o ambiente virtual
call .venv\Scripts\activate.bat

REM Roda o pacote completo de dados pesados (SGS, ANBIMA, etc.)
python atualiza_dados_pesados.py

REM ============================
REM  BLOCO DE GIT AUTO COMMIT
REM ============================

REM Confere se isso aqui é um repositório git
git rev-parse --is-inside-work-tree >NUL 2>&1
if errorlevel 1 goto :fim

REM Verifica se há mudanças (status "sujo")
set HAS_CHANGES=0
git status --porcelain > "%TEMP%\git_status_tesouro.txt"

for /f %%i in (%TEMP%\git_status_tesouro.txt) do (
    set HAS_CHANGES=1
    goto :do_commit
)

del "%TEMP%\git_status_tesouro.txt"
goto :fim

:do_commit
del "%TEMP%\git_status_tesouro.txt"

REM Adiciona os arquivos (aqui está geral; se quiser, pode restringir)
git add .

REM Monta mensagem com data e hora
set DATA=%DATE%
set HORA=%TIME:~0,5%
git commit -m "Auto update dados pesados %DATA% %HORA%"

REM Envia para o GitHub
git push

:fim
endlocal
