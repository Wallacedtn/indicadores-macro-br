@echo off
REM Vai para a pasta do projeto
cd /d C:\Dev\tesouro

REM Ativa o ambiente virtual
call .venv\Scripts\activate.bat

REM Roda o pacote completo de dados pesados (SGS, ANBIMA, DI Futuro, Ibovespa, Tesouro Direto)
python atualiza_dados_pesados.py
