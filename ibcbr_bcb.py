# ibcbr_bcb.py
from __future__ import annotations

from pathlib import Path
from datetime import date
import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
DATA_ATIVIDADE_DIR = BASE_DIR / "data" / "atividade"
IBC_BR_CSV = DATA_ATIVIDADE_DIR / "ibcbr.csv"

# BCB/SGS: IBC-Br dessazonalizado (nível)
SERIE_IBCBR_SA = 24363

def atualizar_ibcbr_csv(
    data_inicial: str = "01/01/2003",
    data_final: str | None = None,
    out_path: Path = IBC_BR_CSV,
) -> Path:
    """
    Baixa IBC-Br (SA) do SGS e salva em CSV (data,valor) em data/atividade/ibcbr.csv.
    """
    DATA_ATIVIDADE_DIR.mkdir(parents=True, exist_ok=True)
    if data_final is None:
        data_final = date.today().strftime("%d/%m/%Y")

    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{SERIE_IBCBR_SA}/dados"
        f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
    )

    r = requests.get(url, timeout=60)
    r.raise_for_status()
    dados = r.json() or []

    df = pd.DataFrame(dados)
    if df.empty:
        raise RuntimeError("IBC-Br: API retornou vazio.")

    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
    df["valor"] = pd.to_numeric(df["valor"].astype(str).str.replace(",", "."), errors="coerce")
    df = df.dropna(subset=["data", "valor"]).sort_values("data").reset_index(drop=True)

    df.to_csv(out_path, index=False)
    print(f"[IBC-Br] CSV atualizado: {out_path} ({len(df)} linhas). Último mês: {df['data'].iloc[-1].strftime('%m/%Y')}")
    return out_path
