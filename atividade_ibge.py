# atividade_ibge.py
from __future__ import annotations

import logging
from pathlib import Path
from datetime import date
import pandas as pd
import requests
from typing import Optional

logging.basicConfig(level=logging.WARNING)


BASE_DIR = Path(__file__).resolve().parent
DATA_ATIVIDADE_DIR = BASE_DIR / "data" / "atividade"
DATA_ATIVIDADE_DIR.mkdir(parents=True, exist_ok=True)

PIM_CSV = DATA_ATIVIDADE_DIR / "pim_pf.csv"
PMS_CSV = DATA_ATIVIDADE_DIR / "pms.csv"
PMC_CSV = DATA_ATIVIDADE_DIR / "pmc.csv"


def _get_json(url: str) -> list[dict]:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json() or []


def _parse_periodo_sidra(row: dict) -> Optional[pd.Timestamp]:
    # Preferência: D3C vem como YYYYMM
    s = str(row.get("D3C", "")).strip()
    if s.isdigit() and len(s) == 6:
        return pd.to_datetime(s, format="%Y%m", errors="coerce")
    # fallback: tenta D3N tipo "2025 outubro" (pega só o ano e mês se conseguir)
    s2 = str(row.get("D3N", "")).strip()
    # tenta pegar YYYY e o número do mês via map simples
    if len(s2) >= 4 and s2[:4].isdigit():
        ano = int(s2[:4])
        meses = {
            "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4, "maio": 5, "junho": 6,
            "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12
        }
        s2_low = s2.lower()
        for nome, mm in meses.items():
            if nome in s2_low:
                return pd.Timestamp(year=ano, month=mm, day=1)
    return None


def _sidra_to_df(url: str, col_value: str = "V") -> pd.DataFrame:
    dados = _get_json(url)
    if not dados:
        return pd.DataFrame(columns=["data", "valor"])
    df = pd.DataFrame(dados)
    df["data"] = df.apply(lambda r: _parse_periodo_sidra(r) or pd.NaT, axis=1)
    df["valor"] = pd.to_numeric(df[col_value].astype(str).str.replace(",", "."), errors="coerce")
    df = df.dropna(subset=["data", "valor"]).sort_values("data").reset_index(drop=True)
    return df[["data", "valor"]]


def _montar_3_metricas(url_mom: str, url_ytd: str, url_12m: str) -> pd.DataFrame:
    df_m = _sidra_to_df(url_mom).rename(columns={"valor": "var_mom"})
    df_y = _sidra_to_df(url_ytd).rename(columns={"valor": "acum_ano"})
    df_12 = _sidra_to_df(url_12m).rename(columns={"valor": "acum_12m"})

    df = df_m.merge(df_y, on="data", how="outer").merge(df_12, on="data", how="outer")
    df = df.sort_values("data").reset_index(drop=True)
    return df


def atualizar_pmc_csv(out_path: Path = PMC_CSV) -> pd.DataFrame:
    try:
        base = "https://apisidra.ibge.gov.br/values/"
        url_mom = base + "t/8880/n1/all/v/11708/p/last60/c11046/56734/d/v11708%201"
        url_ytd = base + "t/8880/n1/all/v/11710/p/last60/c11046/56734/d/v11710%201"
        url_12m = base + "t/8880/n1/all/v/11711/p/last60/c11046/56734/d/v11711%201"

        df = _montar_3_metricas(url_mom, url_ytd, url_12m)
        df.to_csv(out_path, index=False, date_format="%Y-%m-%d")

        if df.empty:
            print(f"[PMC] CSV atualizado (vazio): {out_path} (0 linhas).")
        else:
            print(f"[PMC] CSV atualizado: {out_path} ({len(df)} linhas). Último mês: {df['data'].iloc[-1].strftime('%m/%Y')}")
        return df
    except Exception as e:
        logging.error(f"Erro ao atualizar PMC: {e}")
        raise



def atualizar_pms_csv(out_path: Path = PMS_CSV) -> pd.DataFrame:
    try:
        base = "https://apisidra.ibge.gov.br/values/"
        url_mom = base + "t/5906/n1/all/v/11623/p/last60/c11046/56726/d/v11623%201"
        url_ytd = base + "t/5906/n1/all/v/11625/p/last60/c11046/56726/d/v11625%201"
        url_12m = base + "t/5906/n1/all/v/11626/p/last60/c11046/56726/d/v11626%201"

        df = _montar_3_metricas(url_mom, url_ytd, url_12m)
        df.to_csv(out_path, index=False, date_format="%Y-%m-%d")

        if df.empty:
            print(f"[PMS] CSV atualizado (vazio): {out_path} (0 linhas).")
        else:
            print(f"[PMS] CSV atualizado: {out_path} ({len(df)} linhas). Último mês: {df['data'].iloc[-1].strftime('%m/%Y')}")
        return df
    except Exception as e:
        logging.error(f"Erro ao atualizar PMS: {e}")
        raise



def atualizar_pim_csv(out_path: Path = PIM_CSV) -> pd.DataFrame:
    try:
        base = "https://apisidra.ibge.gov.br/values/"
        url_mom = base + "t/8888/n1/all/v/11601/p/last60/c544/129314/d/v11601%201"
        url_ytd = base + "t/8888/n1/all/v/11603/p/last60/c544/129314/d/v11603%201"
        url_12m = base + "t/8888/n1/all/v/11604/p/last60/c544/129314/d/v11604%201"

        df = _montar_3_metricas(url_mom, url_ytd, url_12m)
        df.to_csv(out_path, index=False, date_format="%Y-%m-%d")

        if df.empty:
            print(f"[PIM-PF] CSV atualizado (vazio): {out_path} (0 linhas).")
        else:
            print(f"[PIM-PF] CSV atualizado: {out_path} ({len(df)} linhas). Último mês: {df['data'].iloc[-1].strftime('%m/%Y')}")
        return df
    except Exception as e:
        logging.error(f"Erro ao atualizar PIM: {e}")
        raise



def main() -> None:
    print("=" * 80)
    print(f"[{date.today()}] Atualizando IBGE Coincidentes (PIM/PMS/PMC) via SIDRA...")
    print("=" * 80)
    atualizar_pim_csv()
    atualizar_pms_csv()
    atualizar_pmc_csv()
    print("=" * 80)
    print("Fim.")
    print("=" * 80)


if __name__ == "__main__":
    main()
