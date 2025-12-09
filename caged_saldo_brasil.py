# caged_saldo_brasil.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path
from datetime import date
from typing import Optional

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
CSV_CAGED_SALDO = BASE_DIR / "data" / "mercado_trabalho" / "caged_saldo_brasil.csv"

# Série do "Novo Caged – saldo de empregos formais"
# (use o mesmo serid que você já usa hoje no indicadores_macro_br)
CAGED_SERIE_ID = "CAGED12_SALDON12"

IPEADATA_URL = (
    "http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO="
    f"'{CAGED_SERIE_ID}')"
)


def baixar_caged_saldo_ipeadata(max_anos: int = 10) -> pd.DataFrame:
    """
    Baixa a série do Novo Caged (saldo de empregos formais) no Ipeadata
    e devolve um DataFrame com colunas:
        - data (datetime64[ns])
        - valor (float)
    Limitado aos últimos `max_anos` anos.
    """
    resp = requests.get(IPEADATA_URL, timeout=20)
    resp.raise_for_status()
    dados = resp.json()

    valores = dados.get("value", [])
    if not valores:
        raise ValueError("Resposta vazia do Ipeadata para CAGED.")

    df = pd.DataFrame(valores)

    # As colunas podem variar, mas geralmente:
    #   - "VALDATA" ou similar para data
    #   - "VALVALOR" para o valor
    # Vamos fazer um mapeamento mais robusto:

    col_data = None
    col_valor = None

    for col in df.columns:
        if "DATA" in col.upper():
            col_data = col
        if "VALOR" in col.upper():
            col_valor = col

    if col_data is None or col_valor is None:
        raise ValueError(
            f"Não encontrei colunas de data/valor na resposta do Ipeadata. "
            f"Colunas: {list(df.columns)}"
        )

    df["data"] = pd.to_datetime(df[col_data], errors="coerce").dt.date
    df["valor"] = pd.to_numeric(df[col_valor], errors="coerce")

    df = df.dropna(subset=["data", "valor"]).sort_values("data")

    # limita aos últimos N anos
    if not df.empty:
        corte = pd.to_datetime(date.today()) - pd.DateOffset(years=max_anos)
        df = df[pd.to_datetime(df["data"]) >= corte].reset_index(drop=True)

    return df


def atualizar_caged_saldo_brasil_csv(max_anos: int = 10) -> pd.DataFrame:
    """
    Atualiza o CSV usado pelo painel:
        data/mercado_trabalho/caged_saldo_brasil.csv
    """
    df = baixar_caged_saldo_ipeadata(max_anos=max_anos)

    CSV_CAGED_SALDO.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CSV_CAGED_SALDO, index=False)

    print(
        f"[CAGED] CSV atualizado com {len(df)} linhas em {CSV_CAGED_SALDO}"
    )
    return df


def carregar_caged_saldo_csv(max_anos: int = 10) -> Optional[pd.DataFrame]:
    """
    Carrega o CSV local (se existir) e devolve DataFrame limpinho.

    Se não existir, retorna None (para o painel eventualmente cair
    em um fallback online, se você quiser).
    """
    if not CSV_CAGED_SALDO.exists():
        return None

    df = pd.read_csv(CSV_CAGED_SALDO)

    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")

    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    df = df.dropna(subset=["data", "valor"]).sort_values("data")

    if not df.empty:
        corte = pd.Timestamp.today() - pd.DateOffset(years=max_anos)
        df = df[df["data"] >= corte].reset_index(drop=True)

    return df


if __name__ == "__main__":
    atualizar_caged_saldo_brasil_csv()
