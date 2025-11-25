# analise_tesouro_vs_curva.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import pandas as pd

from tesouro_direto import carregar_tesouro_ultimo_dia
from curvas_anbima import montar_curva_anbima_hoje


def classificar_spread(spread_bps: float, limiar_bps: float = 20.0) -> str:
    """
    Classifica o spread em:
    - 'Barato'   se o Tesouro estiver pagando >= limiar_bps acima da curva
    - 'Caro'     se estiver pagando <= -limiar_bps abaixo da curva
    - 'No preço' se estiver dentro desse intervalo
    """
    if pd.isna(spread_bps):
        return "Sem dado"

    if spread_bps >= limiar_bps:
        return "Barato"
    if spread_bps <= -limiar_bps:
        return "Caro"
    return "No preço"


def comparar_tesouro_pre_vs_curva() -> pd.DataFrame:
    """
    Devolve um DataFrame com:
    - nome_titulo (Prefixado / Prefixado c/ juros)
    - data_vencimento
    - prazo_anos
    - taxa_compra (Tesouro)
    - taxa_pre_anbima (curva pré ANBIMA)
    - spread_bps
    - Sinal (Barato/Caro/No preço)
    - data_base_tesouro (data-base da planilha do Tesouro Direto)
    - data_curva_anbima (data da curva ANBIMA usada na comparação)
    """
    df_tesouro = carregar_tesouro_ultimo_dia()
    curva = montar_curva_anbima_hoje()

    # Datas de referência
    data_tesouro = None
    if "data_base" in df_tesouro.columns:
        try:
            data_tesouro = pd.to_datetime(df_tesouro["data_base"]).max()
        except Exception:
            # Se não conseguir converter, pega o primeiro valor "cru" mesmo
            data_tesouro = df_tesouro["data_base"].iloc[0]

    data_curva = None
    if "data_curva" in curva.columns:
        try:
            data_curva = pd.to_datetime(curva["data_curva"]).max()
        except Exception:
            data_curva = curva["data_curva"].iloc[0]

    # Curva pré
    df_pre_anbima = (
        curva[["Vértice (anos)", "Juro Nominal (%)"]]
        .rename(
            columns={
                "Vértice (anos)": "prazo_anos_curva",
                "Juro Nominal (%)": "taxa_pre_anbima",
            }
        )
        .sort_values("prazo_anos_curva")
    )

    # Tesouro válido
    df_tesouro_valid = (
        df_tesouro[df_tesouro["taxa_compra"].notna()]
        .copy()
        .sort_values("prazo_anos")
    )

    # Garantir tipos numéricos
    df_pre_anbima["prazo_anos_curva"] = (
        pd.to_numeric(df_pre_anbima["prazo_anos_curva"], errors="coerce")
        .astype("float64")
    )
    df_tesouro_valid["prazo_anos"] = (
        pd.to_numeric(df_tesouro_valid["prazo_anos"], errors="coerce")
        .astype("float64")
    )

    df_pre_anbima = df_pre_anbima.dropna(subset=["prazo_anos_curva"])
    df_tesouro_valid = df_tesouro_valid.dropna(subset=["prazo_anos"])

    # Merge por prazo (vértice mais próximo)
    df_cmp_pre = pd.merge_asof(
        df_tesouro_valid.sort_values("prazo_anos"),
        df_pre_anbima.sort_values("prazo_anos_curva"),
        left_on="prazo_anos",
        right_on="prazo_anos_curva",
        direction="nearest",
    )

    # Spread
    df_cmp_pre["spread"] = df_cmp_pre["taxa_compra"] - df_cmp_pre["taxa_pre_anbima"]
    df_cmp_pre["spread_bps"] = df_cmp_pre["spread"] * 100
    df_cmp_pre["Sinal"] = df_cmp_pre["spread_bps"].apply(classificar_spread)

    # Filtro: tirar lixo (taxa muito baixa)
    df_cmp_pre = df_cmp_pre[df_cmp_pre["taxa_compra"] > 1].copy()

    # Filtrar apenas prefixados
    if "nome_titulo" in df_cmp_pre.columns:
        mask_prefixado = df_cmp_pre["nome_titulo"].str.contains(
            "Prefixado", case=False, na=False
        )
        df_cmp_pre = df_cmp_pre[mask_prefixado].copy()

    # Ordenar pelos mais "baratos"
    df_cmp_pre = df_cmp_pre.sort_values("spread_bps", ascending=False)

    # Adiciona colunas de data (iguais em todas as linhas, mas úteis para o Streamlit)
    if data_tesouro is not None:
        df_cmp_pre["data_base_tesouro"] = data_tesouro
    if data_curva is not None:
        df_cmp_pre["data_curva_anbima"] = data_curva

    colunas_saida = [
        "nome_titulo",
        "data_vencimento",
        "prazo_anos",
        "taxa_compra",
        "taxa_pre_anbima",
        "spread_bps",
        "Sinal",
        "data_base_tesouro",
        "data_curva_anbima",
    ]
    colunas_existentes = [c for c in colunas_saida if c in df_cmp_pre.columns]

    return df_cmp_pre[colunas_existentes]


def comparar_tesouro_ipca_vs_curva() -> pd.DataFrame:
    """
    Devolve um DataFrame com:
    - nome_titulo (IPCA+ c/ ou s/ juros)
    - data_vencimento
    - prazo_anos
    - taxa_compra (Tesouro, juro real)
    - taxa_ipca_anbima (curva real ANBIMA)
    - spread_bps
    - Sinal
    - data_base_tesouro (data-base da planilha do Tesouro Direto)
    - data_curva_anbima (data da curva ANBIMA usada na comparação)
    """
    df_tesouro = carregar_tesouro_ultimo_dia()
    curva = montar_curva_anbima_hoje()

    # Datas de referência
    data_tesouro = None
    if "data_base" in df_tesouro.columns:
        try:
            data_tesouro = pd.to_datetime(df_tesouro["data_base"]).max()
        except Exception:
            data_tesouro = df_tesouro["data_base"].iloc[0]

    data_curva = None
    if "data_curva" in curva.columns:
        try:
            data_curva = pd.to_datetime(curva["data_curva"]).max()
        except Exception:
            data_curva = curva["data_curva"].iloc[0]

    # Curva IPCA (real)
    df_ipca_anbima = (
        curva[["Vértice (anos)", "Juro Real (%)"]]
        .rename(
            columns={
                "Vértice (anos)": "prazo_anos_curva",
                "Juro Real (%)": "taxa_ipca_anbima",
            }
        )
        .sort_values("prazo_anos_curva")
    )

    # Tesouro válido
    df_tesouro_valid = (
        df_tesouro[df_tesouro["taxa_compra"].notna()]
        .copy()
        .sort_values("prazo_anos")
    )

    # Filtrar apenas IPCA+
    if "nome_titulo" in df_tesouro_valid.columns:
        mask_ipca = df_tesouro_valid["nome_titulo"].str.contains(
            "IPCA", case=False, na=False
        )
        df_tesouro_valid = df_tesouro_valid[mask_ipca].copy()

    # Garantir tipos numéricos
    df_ipca_anbima["prazo_anos_curva"] = (
        pd.to_numeric(df_ipca_anbima["prazo_anos_curva"], errors="coerce")
        .astype("float64")
    )
    df_tesouro_valid["prazo_anos"] = (
        pd.to_numeric(df_tesouro_valid["prazo_anos"], errors="coerce")
        .astype("float64")
    )

    df_ipca_anbima = df_ipca_anbima.dropna(subset=["prazo_anos_curva"])
    df_tesouro_valid = df_tesouro_valid.dropna(subset=["prazo_anos"])

    # Merge por prazo (vértice mais próximo)
    df_cmp_ipca = pd.merge_asof(
        df_tesouro_valid.sort_values("prazo_anos"),
        df_ipca_anbima.sort_values("prazo_anos_curva"),
        left_on="prazo_anos",
        right_on="prazo_anos_curva",
        direction="nearest",
    )

    # Spread real
    df_cmp_ipca["spread"] = df_cmp_ipca["taxa_compra"] - df_cmp_ipca["taxa_ipca_anbima"]
    df_cmp_ipca["spread_bps"] = df_cmp_ipca["spread"] * 100
    df_cmp_ipca["Sinal"] = df_cmp_ipca["spread_bps"].apply(classificar_spread)

    # Filtro: tirar lixo (taxa muito baixa)
    df_cmp_ipca = df_cmp_ipca[df_cmp_ipca["taxa_compra"] > 1].copy()

    # Ordenar pelos mais "baratos"
    df_cmp_ipca = df_cmp_ipca.sort_values("spread_bps", ascending=False)

    # Adiciona colunas de data (iguais em todas as linhas, mas úteis para o Streamlit)
    if data_tesouro is not None:
        df_cmp_ipca["data_base_tesouro"] = data_tesouro
    if data_curva is not None:
        df_cmp_ipca["data_curva_anbima"] = data_curva

    colunas_saida = [
        "nome_titulo",
        "data_vencimento",
        "prazo_anos",
        "taxa_compra",
        "taxa_ipca_anbima",
        "spread_bps",
        "Sinal",
        "data_base_tesouro",
        "data_curva_anbima",
    ]
    colunas_existentes = [c for c in colunas_saida if c in df_cmp_ipca.columns]

    return df_cmp_ipca[colunas_existentes]