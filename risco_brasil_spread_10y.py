# risco_brasil_spread_10y.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path
from datetime import date, timedelta
from typing import Optional, Tuple

import pandas as pd

from us10y_fred import carregar_serie_us10y

# --------------------------------------------------------------------
# Caminhos dos arquivos
# --------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

CURVAS_ANBIMA_FULL = BASE_DIR / "data" / "curvas_tesouro" / "curvas_anbima" / "curvas_anbima_full.csv"
CSV_SPREAD = BASE_DIR / "data" / "curto_prazo" / "risco_brasil_spread_10y.csv"

# 10 anos em dias úteis (aprox.)
DI_DIAS_ANO = 252
TARGET_ANOS = 10.0
TARGET_DU = int(DI_DIAS_ANO * TARGET_ANOS)  # ~2520 dias úteis


# --------------------------------------------------------------------
# 1) Série histórica 10Y Brasil local a partir da curva ANBIMA
# --------------------------------------------------------------------
def carregar_serie_10y_brasil_historica() -> pd.DataFrame:
    """
    Lê o curvas_anbima_full.csv e, para cada data_ref, escolhe o ponto de
    prazo mais próximo de 10 anos (em dias úteis).

    Retorna DataFrame com:
        - data: date
        - br_10y: taxa prefixada 10Y (em % a.a.)
    """
    if not CURVAS_ANBIMA_FULL.exists():
        raise FileNotFoundError(f"[10Y BR] Arquivo não encontrado: {CURVAS_ANBIMA_FULL}")

    df = pd.read_csv(CURVAS_ANBIMA_FULL)

    # garante que data_ref é data
    df["data_ref"] = pd.to_datetime(df["data_ref"], errors="coerce").dt.date
    df = df.dropna(subset=["data_ref"])

    # distância até 10 anos em prazo DU
    df["dist"] = (df["PRAZO_DU"] - TARGET_DU).abs()

    # para cada data_ref, pega a linha de menor dist (mais próximo de 10Y)
    idx_min = df.groupby("data_ref")["dist"].idxmin()
    df_10y = df.loc[idx_min, ["data_ref", "TAXA_PREF"]].copy()

    df_10y.rename(columns={"data_ref": "data", "TAXA_PREF": "br_10y"}, inplace=True)
    df_10y.sort_values("data", inplace=True)
    df_10y.reset_index(drop=True, inplace=True)

    return df_10y


# --------------------------------------------------------------------
# 2) Monta/atualiza a série histórica do spread 10Y BR–US
# --------------------------------------------------------------------
def atualizar_spread_10y() -> None:
    """
    Monta a série histórica do spread 10Y Brasil local – US10Y e salva
    em CSV_SPREAD com colunas:
        - data
        - br_10y
        - us10y
        - spread_pb
    """
    # 10Y Brasil local (curva ANBIMA histórica)
    df_br = carregar_serie_10y_brasil_historica()  # data, br_10y

    # US10Y (FRED) – já temos pronto no módulo us10y_fred.py
    df_us = carregar_serie_us10y()                 # data, valor
    df_us = df_us.rename(columns={"valor": "us10y"})

    # junta pelas datas em comum
    df = pd.merge(df_br, df_us, on="data", how="inner")
    df = df.dropna(subset=["br_10y", "us10y"])

    if df.empty:
        print("[spread 10y] Série vazia após merge BR/US. Nada foi salvo.")
        return

    # spread em pontos-base
    df["spread_pb"] = (df["br_10y"] - df["us10y"]) * 100.0

    df.sort_values("data", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # garante que a pasta existe
    CSV_SPREAD.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(CSV_SPREAD, index=False, float_format="%.1f", encoding="utf-8")

    ult = df.iloc[-1]
    print(
        f"[spread 10y] Série atualizada até {ult['data']} – "
        f"{ult['spread_pb']:.1f} p.b. (BR10Y={ult['br_10y']:.2f}%, "
        f"US10Y={ult['us10y']:.2f}%). "
        f"Total de {len(df)} linhas em {CSV_SPREAD}"
    )


# --------------------------------------------------------------------
# 3) Função que o card usa para ler o CSV e montar nível / Δ / média 12m
# --------------------------------------------------------------------
def carregar_risco_brasil_spread_10y():
    """
    Lê o CSV de spread 10Y BR–US e devolve:
      - nivel: último spread (p.b.)
      - delta_aa: variação em 12 meses (p.b., se der pra calcular)
      - referencia: 'MM/AAAA' da última observação
      - media_12m: média 12m do spread (p.b.)
      - delta_d1: variação em relação ao dia útil anterior (p.b.)
      - inicio_mes: spread no primeiro dia do mês atual (p.b.)
      - inicio_data_mes: data desse primeiro dia (datetime.date)
    """
    if not CSV_SPREAD.exists():
        print(f"[spread 10y] CSV ainda não existe: {CSV_SPREAD}")
        return (None, None, None, None, None, None, None)

    df = pd.read_csv(CSV_SPREAD)

    if df.empty or "data" not in df.columns or "spread_pb" not in df.columns:
        print(f"[spread 10y] CSV vazio ou sem colunas esperadas em {CSV_SPREAD}")
        return (None, None, None, None, None, None, None)

    # data em datetime.date
    df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date
    df = df.dropna(subset=["data", "spread_pb"]).copy()

    if df.empty:
        print("[spread 10y] Série vazia após limpeza.")
        return (None, None, None, None, None, None, None)

    # ordena por data
    df = df.sort_values("data").reset_index(drop=True)

    # último ponto
    ult = df.iloc[-1]
    data_ult = ult["data"]
    nivel = float(ult["spread_pb"])

    # variação vs D-1
    delta_d1 = None
    if len(df) >= 2:
        penult = df.iloc[-2]
        delta_d1 = float(nivel - float(penult["spread_pb"]))

    # valor no início do mês (primeira linha do mesmo mês/ano)
    inicio_mes = None
    inicio_data_mes = None
    mask_mes = [
        (d.year == data_ult.year and d.month == data_ult.month)
        for d in df["data"]
    ]
    df_mes = df[mask_mes]
    if not df_mes.empty:
        inicio_mes = float(df_mes.iloc[0]["spread_pb"])
        inicio_data_mes = df_mes.iloc[0]["data"]

    # janela de 12 meses
    limite_12m = data_ult - timedelta(days=365)
    df_12m = df[df["data"] >= limite_12m].copy()

    media_12m = None
    if not df_12m.empty:
        media_12m = float(df_12m["spread_pb"].mean())

    # delta em 12 meses (se tiver ponto "antigo" pra comparar)
    delta_aa = None
    df_antes = df[df["data"] <= limite_12m]
    if not df_antes.empty:
        base = float(df_antes.iloc[-1]["spread_pb"])
        delta_aa = nivel - base

    referencia = data_ult.strftime("%d/%m/%Y")

    return (
        nivel,
        delta_aa,
        referencia,
        media_12m,
        delta_d1,
        inicio_mes,
        inicio_data_mes,
    )


if __name__ == "__main__":
    # permite rodar direto: python risco_brasil_spread_10y.py
    atualizar_spread_10y()
