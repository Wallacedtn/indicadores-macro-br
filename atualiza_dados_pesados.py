# atualiza_dados_pesados.py
# -*- coding: utf-8 -*-

"""
Roda 1x/dia (via Agendador de Tarefas) para atualizar:
- Curvas ANBIMA
- DI Futuro (B3)
- Ibovespa (IPEA)

O Streamlit depois só lê os CSVs prontos.
"""

from datetime import datetime

from curvas_anbima import atualizar_todas_as_curvas
from di_futuro_b3 import atualizar_historico_di_futuro
from ibovespa_ipea import atualizar_historico_ibovespa
from dados_curto_prazo_br import atualizar_cache_curto_prazo
from tesouro_direto import atualizar_cache_tesouro_bruto
from dados_focus import atualizar_cache_focus

def main() -> None:
    print("=" * 80)
    print(f"[{datetime.now()}] Iniciando atualização de dados pesados...")
    print("=" * 80)

    # 0) Séries SGS (Selic/CDI/PTAX)
    try:
        print("[0/5] Atualizando cache SGS (Selic/CDI/PTAX)...")
        atualizar_cache_curto_prazo()
        print("    ✔ SGS curto prazo ok.")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar SGS curto prazo: {e}")

    # 1) Focus – expectativas anuais e Top5
    try:
        print("[1/5] Atualizando cache do Focus (anuais + Top5)...")
        atualizar_cache_focus()
        print("    ✔ Focus (anuais + Top5) ok.")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar Focus: {e}")

    # 1) Curvas ANBIMA
    try:
        print("[2/5] Atualizando curvas ANBIMA...")
        atualizar_todas_as_curvas()
        print("    ✔ Curvas ANBIMA ok.")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar curvas ANBIMA: {e}")

    # 2) DI Futuro B3
    try:
        print("[3/5] Atualizando histórico DI Futuro (B3)...")
        df_di = atualizar_historico_di_futuro()
        print(f"    ✔ DI Futuro ok ({len(df_di)} linhas).")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar DI Futuro: {e}")

       # 3) Ibovespa IPEA
    try:
        print("[4/5] Atualizando histórico Ibovespa (IPEA)...")
        df_ibov = atualizar_historico_ibovespa()
        print(f"    ✔ Ibovespa ok ({len(df_ibov)} linhas).")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar Ibovespa: {e}")

    # 4) Tesouro Direto
    try:
        print("[5/5] Atualizando histórico Tesouro Direto...")
        df_td = atualizar_cache_tesouro_bruto()
        print(f"    ✔ Tesouro Direto ok ({len(df_td)} linhas).")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar Tesouro Direto: {e}")


    print("=" * 80)
    print(f"[{datetime.now()}] Fim da atualização.")
    print("=" * 80)


if __name__ == "__main__":
    main()
