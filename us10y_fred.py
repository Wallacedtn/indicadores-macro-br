# us10y_fred.py
# -*- coding: utf-8 -*-

from datetime import date, timedelta
from typing import Optional

import pandas as pd
import requests

# 1) COLE A SUA API KEY AQUI
FRED_API_KEY = "01b9a063f0338d2e0ac0c246a95690e2"  # <- substitua pela sua key real
FRED_SERIES_US10Y = "DGS10"  # Treasury 10 anos

def carregar_serie_us10y(
    inicio: date = date(2000, 1, 1),
    fim: Optional[date] = None,
) -> pd.DataFrame:
    """
    Baixa a série DGS10 (10Y US Treasury) da FRED e devolve
    um DataFrame com colunas:
    - data  (datetime.date)
    - valor (yield em % a.a.)
    """
    if fim is None:
        fim = date.today()

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": FRED_SERIES_US10Y,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": inicio.strftime("%Y-%m-%d"),
        "observation_end": fim.strftime("%Y-%m-%d"),
    }

    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    dados = resp.json().get("observations", [])

    if not dados:
        return pd.DataFrame(columns=["data", "valor"])

    df = pd.DataFrame(dados)

    # 'date' vem como string "YYYY-MM-DD"
    df["data"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # 'value' vem como string; "." significa "sem dado"
    df["valor"] = (
        df["value"]
        .replace(".", pd.NA)
        .pipe(pd.to_numeric, errors="coerce")
    )

    df = (
        df.dropna(subset=["data", "valor"])
        .sort_values("data")
        .reset_index(drop=True)
    )

    return df[["data", "valor"]]


def obter_us10y_mais_recente(ate_quando: Optional[date] = None) -> Optional[float]:
    """
    Devolve o último valor disponível de US10Y (DGS10) até 'ate_quando'.
    Se não encontrar nada, devolve None.
    """
    if ate_quando is None:
        ate_quando = date.today()

    # Busca só uns 2 anos pra trás pra não ficar pesado
    inicio = ate_quando - timedelta(days=365 * 2)
    df = carregar_serie_us10y(inicio=inicio, fim=ate_quando)

    if df.empty:
        return None

    df = df[df["data"] <= ate_quando]
    if df.empty:
        return None

    return float(df.iloc[-1]["valor"])
