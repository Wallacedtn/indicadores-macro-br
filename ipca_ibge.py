# ipca_ibge.py
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# Tabela/variável IPCA no SIDRA
IBGE_TABELA_IPCA = 1737
IBGE_VARIAVEL_IPCA = 63   # variação mensal (%)
IBGE_NIVEL_BRASIL = "n1/all"

# Caminho do CSV que o painel vai usar
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_PRECOS_DIR = DATA_DIR / "precos"
IPCA_MENSAL_CSV = DATA_PRECOS_DIR / "ipca_mensal_ibge.csv"


def _parse_periodo(p: str) -> pd.Timestamp:
    """
    Converte período do SIDRA em datetime.
    Exemplos:
    - '202510' -> 2025-10-01
    - '2025-10' ou '2025-10-01' -> parse automático
    """
    p = str(p).strip()
    if len(p) == 6 and p.isdigit():
        ano = int(p[:4])
        mes = int(p[4:])
        return datetime(ano, mes, 1)
    try:
        return pd.to_datetime(p)
    except Exception:
        return pd.NaT


def baixar_ipca_mensal_ultimos_anos() -> pd.DataFrame:
    """
    Baixa o IPCA mensal (%), últimos 60 meses, direto do SIDRA.
    Retorna DataFrame com colunas ['data', 'valor'].
    """
    url = (
        f"https://apisidra.ibge.gov.br/values/"
        f"t/{IBGE_TABELA_IPCA}/{IBGE_NIVEL_BRASIL}/v/{IBGE_VARIAVEL_IPCA}/p/last60"
    )

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    dados = resp.json()

    if not dados:
        raise ValueError("SIDRA (IPCA) retornou lista vazia.")

    header = dados[0]
    linhas = dados[1:]
    df = pd.DataFrame(linhas)

    # Descobre coluna de período (mais robusto)
    col_periodo = None
    for col in df.columns:
        titulo = str(header.get(col, "")).lower()
        if any(
            p in titulo
            for p in ["mês (código)", "mes (código)", "mês", "mes", "período", "periodo"]
        ):
            col_periodo = col
            break

    if col_periodo is None:
        if "D3C" in df.columns:
            col_periodo = "D3C"
        elif "D2C" in df.columns:
            col_periodo = "D2C"
        else:
            col_periodo = df.columns[0]

    col_valor = "V"  # coluna padrão de valor no SIDRA

    df["data"] = df[col_periodo].apply(_parse_periodo)
    df["valor"] = pd.to_numeric(
        df[col_valor].astype(str).str.replace(",", "."),
        errors="coerce",
    )

    df = (
        df[["data", "valor"]]
        .dropna()
        .sort_values("data")
        .drop_duplicates(subset=["data"], keep="last")
        .reset_index(drop=True)
    )
    return df


def atualizar_ipca_mensal_csv() -> pd.DataFrame:
    """
    Baixa IPCA mensal do SIDRA e grava em:
      data/precos/ipca_mensal_ibge.csv

    Esse CSV é o que o indicadores_macro_br.py vai ler no modo offline-first.
    """
    try:
        df = baixar_ipca_mensal_ultimos_anos()

        DATA_PRECOS_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(IPCA_MENSAL_CSV, index=False, encoding="utf-8")

        logger.info(
            "CSV do IPCA mensal atualizado: %d linhas em %s",
            len(df),
            IPCA_MENSAL_CSV,
        )

        # logzinho no console pra sanity check
        ultimo = df.iloc[-1]
        print(
            f"Último mês IPCA: {ultimo['data'].strftime('%m/%Y')} | "
            f"variação: {ultimo['valor']:.2f}%"
        )

        return df
    except Exception as e:
        logger.error(f"Erro ao atualizar IPCA mensal: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    atualizar_ipca_mensal_csv()
