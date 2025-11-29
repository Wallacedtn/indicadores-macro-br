# tesouro_direto.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import io
from datetime import date
from functools import lru_cache
from typing import List, Optional

import pandas as pd
import requests
from pathlib import Path

# Pasta onde vamos guardar o CSV bruto do Tesouro Direto
BASE_DIR = Path(__file__).resolve().parent
TD_DATA_DIR = BASE_DIR / "data" / "curvas_tesouro" / "tesouro_direto"
TD_DATA_PATH = TD_DATA_DIR / "tesouro_direto_bruto.csv"


TESOURO_CSV_URL = (
    "https://www.tesourotransparente.gov.br/ckan/dataset/"
    "df56aa42-484a-4a59-8184-7676580c81e3/resource/"
    "796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv"
)


@lru_cache(maxsize=1)
def carregar_tesouro_bruto() -> pd.DataFrame:
    """
    Carrega o histórico de Preço e Taxa do Tesouro Direto.

    Comportamento:
    - 1º tenta ler do CSV local em data/tesouro_direto/tesouro_direto_bruto.csv
    - Se der erro ou não existir, faz o download do Tesouro Transparente
      e salva o CSV para as próximas vezes.
    """
    # 1) Tenta primeiro o CSV local
    try:
        if TD_DATA_PATH.exists():
            df = pd.read_csv(TD_DATA_PATH)
            if not df.empty:
                return df
    except Exception:
        # Se o CSV estiver corrompido, ignora e força o download
        pass

    # 2) Fallback: baixa on-line do Tesouro Transparente
    resp = requests.get(TESOURO_CSV_URL, timeout=60)
    resp.raise_for_status()


    df = pd.read_csv(
        io.StringIO(resp.text),
        sep=";",
        decimal=",",
        encoding="latin1",
    )

    # 3) Salva o CSV bruto para uso futuro
    try:
        TD_DATA_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(TD_DATA_PATH, index=False)
    except Exception:
        # Não queremos quebrar se der erro só na hora de salvar
        pass

    return df

def atualizar_cache_tesouro_bruto() -> pd.DataFrame:
    """
    Força o download do CSV oficial do Tesouro Direto e atualiza o cache local.

    Usado pelo job pesado (atualiza_dados_pesados.py), fora do Streamlit.
    """
    resp = requests.get(TESOURO_CSV_URL, timeout=60)
    resp.raise_for_status()


    df = pd.read_csv(
        io.StringIO(resp.text),
        sep=";",
        decimal=",",
        encoding="latin1",
    )

    TD_DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(TD_DATA_PATH, index=False)

    # Limpa o cache da função carregar_tesouro_bruto para usar o dado novo
    try:
        carregar_tesouro_bruto.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass

    return df


def _encontrar_coluna(df: pd.DataFrame, *pedacos: str) -> Optional[str]:
    """
    Procura uma coluna cujo nome (lower/strip) contenha TODOS os pedaços informados.
    Exemplo: _encontrar_coluna(df, "data", "base") => "Data Base"
    """
    mapa = {c.lower().strip(): c for c in df.columns}
    for nome_lower, original in mapa.items():
        if all(p.lower() in nome_lower for p in pedacos):
            return original
    return None


def carregar_tesouro_ultimo_dia() -> pd.DataFrame:
    """
    A partir do histórico bruto, filtra apenas o último dia disponível
    e devolve uma tabela com as taxas de compra atuais de cada título,
    já com prazo aproximado em anos.
    """
    df = carregar_tesouro_bruto().copy()
    if df.empty:
        return df

    # Descobre nomes reais das colunas no CSV
    col_data_base = (
        _encontrar_coluna(df, "data", "base")
        or _encontrar_coluna(df, "data")
    )
    col_data_venc = _encontrar_coluna(df, "venc")

    # 🔧 aqui o ajuste importante:
    # primeiro tenta "nome"+"titul" (ex: "Nome Titulo")
    # se não achar, tenta qualquer coisa com "titulo" (ex: "Titulo")
    col_nome = (
        _encontrar_coluna(df, "nome", "titul")
        or _encontrar_coluna(df, "titulo")
    )

    col_sigla = _encontrar_coluna(df, "sigla")
    col_taxa_compra = _encontrar_coluna(df, "taxa", "compra")

    rename_map = {}
    if col_data_base:
        rename_map[col_data_base] = "data_base"
    if col_data_venc:
        rename_map[col_data_venc] = "data_vencimento"
    if col_nome:
        rename_map[col_nome] = "nome_titulo"
    if col_sigla:
        rename_map[col_sigla] = "sigla_titulo"
    if col_taxa_compra:
        rename_map[col_taxa_compra] = "taxa_compra"

    df = df.rename(columns=rename_map)

    # Converte datas, se existir coluna
    if "data_base" in df.columns:
        df["data_base"] = pd.to_datetime(
            df["data_base"], dayfirst=True, errors="coerce"
        )
        data_mais_recente = df["data_base"].max()
        df_ultimo = df[df["data_base"] == data_mais_recente].copy()
    else:
        # fallback: se não achou data_base, pega tudo
        df_ultimo = df.copy()

    if "data_vencimento" in df_ultimo.columns:
        df_ultimo["data_vencimento"] = pd.to_datetime(
            df_ultimo["data_vencimento"], dayfirst=True, errors="coerce"
        )

    # Converte taxa de compra pra número
    if "taxa_compra" in df_ultimo.columns:
        df_ultimo["taxa_compra"] = pd.to_numeric(
            df_ultimo["taxa_compra"], errors="coerce"
        )

    # Calcula prazo em anos (se tiver vencimento)
    if "data_vencimento" in df_ultimo.columns:
        hoje_ts = pd.Timestamp(date.today())
        mask_ok = df_ultimo["data_vencimento"].notna()
        df_ultimo["prazo_anos"] = pd.NA
        df_ultimo.loc[mask_ok, "prazo_anos"] = (
            (df_ultimo.loc[mask_ok, "data_vencimento"] - hoje_ts).dt.days / 365.25
        )

    # Seleciona colunas principais que interessam agora
    colunas_basicas: List[str] = [
        "data_base",
        "nome_titulo",
        "sigla_titulo",
        "data_vencimento",
        "prazo_anos",
        "taxa_compra",
    ]
    cols_existentes = [c for c in colunas_basicas if c in df_ultimo.columns]

    if not cols_existentes:
        return df_ultimo

    return df_ultimo[cols_existentes].sort_values("prazo_anos")
