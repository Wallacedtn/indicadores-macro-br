from __future__ import annotations

from pathlib import Path
from datetime import date
import logging

import pandas as pd
import requests

logger = logging.getLogger(__name__)


# =============================================================================
# Configuração de diretórios
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_SETOR_EXTERNO_DIR = DATA_DIR / "setor_externo"
DATA_SETOR_EXTERNO_DIR.mkdir(parents=True, exist_ok=True)

BALANCA_COMERCIAL_CSV = DATA_SETOR_EXTERNO_DIR / "balanca_comercial_mensal_usd.csv"

# Série do BCB:
# 22707 - Balança comercial - Balanço de Pagamentos - mensal - saldo
SGS_BALANCA_COMERCIAL = 22707


def _hoje_str() -> str:
    """Data de hoje em dd/mm/aaaa (formato que o BCB espera)."""
    return date.today().strftime("%d/%m/%Y")


def baixar_balanca_comercial_bcb(
    codigo_serie: int = SGS_BALANCA_COMERCIAL,
    data_inicial: str = "01/01/2000",
    data_final: str | None = None,
) -> pd.DataFrame:
    """
    Baixa a série mensal da balança comercial (US$ milhões) do SGS/BCB
    e devolve um DataFrame com colunas:

      - data
      - saldo_usd_milhoes
    """
    if data_final is None:
        data_final = _hoje_str()

    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados"
        f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
    )

    logger.info("Baixando série %s do BCB: %s", codigo_serie, url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    dados = resp.json()
    if not dados:
        raise RuntimeError("Série da balança comercial veio vazia do BCB.")

    df = pd.DataFrame(dados)

    # Normaliza
    df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
    df["saldo_usd_milhoes"] = pd.to_numeric(df["valor"], errors="coerce")

    df = df[["data", "saldo_usd_milhoes"]].dropna().sort_values("data").reset_index(drop=True)
    return df


def atualizar_csv_balanca_comercial() -> None:
    """
    Baixa a série do BCB e grava em:
      data/setor_externo/balanca_comercial_mensal_usd.csv
    """
    try:
        df = baixar_balanca_comercial_bcb()
        BALANCA_COMERCIAL_CSV.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(BALANCA_COMERCIAL_CSV, index=False, encoding="utf-8")
        logger.info(
            "CSV da balança comercial atualizado: %s linhas em %s",
            len(df),
            BALANCA_COMERCIAL_CSV,
        )

        # Mensagenzinha de sanity check no console
        ultimo = df.iloc[-1]
        print(
            f"Último mês: {ultimo['data'].strftime('%m/%Y')} | "
            f"saldo: {ultimo['saldo_usd_milhoes'] / 1000:.1f} US$ bi"
        )
    except Exception as e:
        logger.error(f"Erro ao atualizar balança comercial: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    atualizar_csv_balanca_comercial()
