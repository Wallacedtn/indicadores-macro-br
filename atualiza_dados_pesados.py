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
from pathlib import Path
import pandas as pd

from curvas_anbima import atualizar_todas_as_curvas
from di_futuro_b3 import atualizar_historico_di_futuro
from ibovespa_ipea import atualizar_historico_ibovespa
from dados_curto_prazo_br import atualizar_cache_curto_prazo
from tesouro_direto import atualizar_cache_tesouro_bruto
from dados_focus import atualizar_cache_focus
from balanca_comercial_bcb import atualizar_csv_balanca_comercial
from risco_brasil_spread_10y import atualizar_spread_10y
from dados_macro_fiscal_br import atualizar_divida_bruta_csv, atualizar_resultado_primario_csv
from ipca_ibge import atualizar_ipca_mensal_csv
from caged_saldo_brasil import atualizar_caged_saldo_brasil_csv
from fgv_confianca import atualizar_fgv_indice, rebuild_sondagens_fgv_consolidado, importar_sondagens_fgv_csv_wide
from ibcbr_bcb import atualizar_ibcbr_csv
from atividade_ibge import atualizar_pim_csv, atualizar_pms_csv, atualizar_pmc_csv





def main() -> None:
    print("=" * 80)
    print(f"[{datetime.now()}] Iniciando atualização de dados pesados...")
    print("=" * 80)

    # 0) Séries SGS (Selic/CDI/PTAX)
    try:
        print("[0/11] Atualizando cache SGS (Selic/CDI/PTAX)...")
        atualizar_cache_curto_prazo()
        print("    ✔ SGS curto prazo ok.")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar SGS curto prazo: {e}")

    # 1) Focus – expectativas anuais e Top5
    try:
        print("[1/11] Atualizando cache do Focus (anuais + Top5)...")
        atualizar_cache_focus()
        print("    ✔ Focus (anuais + Top5) ok.")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar Focus: {e}")

    # 2) Curvas ANBIMA
    try:
        print("[2/11] Atualizando curvas ANBIMA...")
        atualizar_todas_as_curvas()
        print("    ✔ Curvas ANBIMA ok.")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar curvas ANBIMA: {e}")

    # 3) DI Futuro B3
    try:
        print("[3/11] Atualizando histórico DI Futuro (B3)...")
        df_di = atualizar_historico_di_futuro()
        print(f"    ✔ DI Futuro ok ({len(df_di)} linhas).")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar DI Futuro: {e}")

    # 4) Ibovespa IPEA
    try:
        print("[4/11] Atualizando histórico Ibovespa (IPEA)...")
        df_ibov = atualizar_historico_ibovespa()
        print(f"    ✔ Ibovespa ok ({len(df_ibov)} linhas).")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar Ibovespa: {e}")

    # 5) Tesouro Direto
    try:
        print("[5/11] Atualizando histórico Tesouro Direto...")
        df_td = atualizar_cache_tesouro_bruto()
        print(f"    ✔ Tesouro Direto ok ({len(df_td)} linhas).")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar Tesouro Direto: {e}")
    
    # 6) Risco-País 10Y (Brasil/USA)
    try:
        print("[6/11] Atualizando Risco-País 10Y (Brasil/USA)...")
        atualizar_spread_10y()  # gera data/curto_prazo/risco_brasil_spread_10y.csv
        print("    ✔ Risco-País 10Y ok.")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar Risco-País 10Y: {e}")


    # 7) Balança Comercial – saldo mensal (BCB/SGS)
    try:
        print("[7/11] Atualizando balança comercial mensal (BCB/SGS)...")
        atualizar_csv_balanca_comercial()  # gera data/setor_externo/balanca_comercial_mensal_usd.csv
        print("    ✔ Balança Comercial ok.")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar Balança Comercial: {e}")
    
    # 8) Dívida Bruta GG (% PIB)
    try:
        print("[8/11] Atualizando Dívida Bruta GG (% PIB)...")
        df_div = atualizar_divida_bruta_csv()
        print(f"    ✔ Dívida Bruta GG ok ({len(df_div)} linhas).")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar Dívida Bruta GG: {e}")
    
    # 9) Resultado Primário GC (R$ bi nominais)
    try:
        print("[9/11] Atualizando Resultado Primário GC (R$ bi)...")
        df_prim = atualizar_resultado_primario_csv()
        print(f"    ✔ Resultado Primário ok ({len(df_prim)} linhas).")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar Resultado Primário: {e}")
    
    # 10) Novo Caged – saldo de empregos formais
    try:
        print("[10/11] Atualizando Novo Caged – saldo de empregos formais...")
        df_caged = atualizar_caged_saldo_brasil_csv()
        print(f"    ✔ Caged saldo ok ({len(df_caged)} linhas).")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar Caged saldo: {e}")
    
    # 11) IPCA mensal (IBGE/SIDRA)
    try:
        print("[11/11] Atualizando IPCA mensal (IBGE/SIDRA)...")
        df_ipca = atualizar_ipca_mensal_csv()  # gera data/precos/ipca_mensal_ibge.csv
        print(f"    ✔ IPCA mensal ok ({len(df_ipca)} linhas).")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar IPCA mensal: {e}")
    
    # 12) FGV – Índices de Confiança (Antecedentes)
    try:
        print("[12/12] Atualizando índices de confiança (ICC/ICI/ICS/ICOM/ICST/ICE)...")

        # Preferência: CSV 'wide' baixado manualmente do portal da FGV
        raw_wide = Path(__file__).resolve().parent / "data" / "sondagens_fgv" / "indicadoresatualizado.csv"
        if raw_wide.exists() and raw_wide.stat().st_size > 0:
            importar_sondagens_fgv_csv_wide(raw_wide, salvar_individual=False)
        else:
            # Fallback: scraping dos releases (gera os CSVs individuais + consolidado)
            for s in ["ICC", "ICI", "ICS", "ICOM", "ICST", "ICE"]:
                atualizar_fgv_indice(s, update_consolidado=False)
            rebuild_sondagens_fgv_consolidado()

        print("    ✔ FGV ok.")
    except Exception as e:
        print(f"    ❌ Erro FGV confiança: {e}")

    # 13) IBC-Br (BCB/SGS) – atividade (coincidente)
    try:
        print("[13/13] Atualizando IBC-Br (BCB/SGS)...")
        out_path = atualizar_ibcbr_csv()  # gera data/atividade/ibcbr.csv
        df_ibc = pd.read_csv(out_path)
        print(f"    ✔ IBC-Br ok ({len(df_ibc)} linhas).")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar IBC-Br: {e}")

    # 14) IBGE Coincidentes – PIM/PMS/PMC (SIDRA -> CSV)
    try:
        print("[14/14] Atualizando IBGE Coincidentes (PIM/PMS/PMC)...")
        df_pim = atualizar_pim_csv()
        df_pms = atualizar_pms_csv()
        df_pmc = atualizar_pmc_csv()
        print(f"    ✔ PIM ok ({len(df_pim)} linhas) | PMS ok ({len(df_pms)} linhas) | PMC ok ({len(df_pmc)} linhas).")
    except Exception as e:
        print(f"    ❌ Erro ao atualizar IBGE Coincidentes: {e}")



    print("=" * 80)
    print(f"[{datetime.now()}] Fim da atualização.")
    print("=" * 80)


if __name__ == "__main__":
    main()
