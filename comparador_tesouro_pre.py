# comparador_tesouro_pre.py
# -*- coding: utf-8 -*-

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


def main() -> None:
    # 1) Carrega Tesouro Direto (último dia)
    df_tesouro = carregar_tesouro_ultimo_dia()
    print("=== Tesouro Direto (último dia) - primeiras linhas ===")
    print(df_tesouro.head(), "\n")

    # 2) Carrega curva ANBIMA de hoje
    curva = montar_curva_anbima_hoje()
    print("=== Curva ANBIMA - primeiras linhas ===")
    print(curva.head(), "\n")

    # 3) Prepara curva PRÉ ANBIMA (vértice x juro nominal)
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

    # 4) Limpa Tesouro (tira linhas sem taxa) e ordena por prazo
    df_tesouro_valid = (
        df_tesouro[df_tesouro["taxa_compra"].notna()]
        .copy()
        .sort_values("prazo_anos")
    )

    # 5) Garante que as colunas de prazo são FLOAT nas duas bases
    df_pre_anbima["prazo_anos_curva"] = (
        pd.to_numeric(df_pre_anbima["prazo_anos_curva"], errors="coerce")
        .astype("float64")
    )
    df_tesouro_valid["prazo_anos"] = (
        pd.to_numeric(df_tesouro_valid["prazo_anos"], errors="coerce")
        .astype("float64")
    )

    # tira linhas sem prazo definido
    df_pre_anbima = df_pre_anbima.dropna(subset=["prazo_anos_curva"])
    df_tesouro_valid = df_tesouro_valid.dropna(subset=["prazo_anos"])

    # 6) Casa cada título do Tesouro com o vértice da curva mais próximo (por prazo)
    df_cmp_pre = pd.merge_asof(
        df_tesouro_valid.sort_values("prazo_anos"),
        df_pre_anbima.sort_values("prazo_anos_curva"),
        left_on="prazo_anos",
        right_on="prazo_anos_curva",
        direction="nearest",
    )

    # 7) Calcula o spread (Tesouro - Curva) em pontos-base
    df_cmp_pre["spread"] = df_cmp_pre["taxa_compra"] - df_cmp_pre["taxa_pre_anbima"]
    df_cmp_pre["spread_bps"] = df_cmp_pre["spread"] * 100  # taxas já são % a.a.

    # 7.1) Classifica como Barato / Caro / No preço
    df_cmp_pre["Sinal"] = df_cmp_pre["spread_bps"].apply(classificar_spread)

    # 7.2) Filtro simples de “lixo”: remove taxas de compra muito baixas (0.01, 0.05 etc.)
    df_cmp_pre = df_cmp_pre[df_cmp_pre["taxa_compra"] > 1].copy()

    # 7.3) Se tivermos o nome do título, filtra só os prefixados
    if "nome_titulo" in df_cmp_pre.columns:
        mask_prefixado = df_cmp_pre["nome_titulo"].str.contains(
            "Prefixado", case=False, na=False
        )
        df_cmp_pre = df_cmp_pre[mask_prefixado].copy()

    # 7.4) Ordena pelos mais "baratos" (maior spread para cima)
    df_cmp_pre = df_cmp_pre.sort_values("spread_bps", ascending=False)

    # 8) Mostra o resultado (primeiras 30 linhas)
    colunas_saida = [
        "nome_titulo",
        "data_vencimento",
        "prazo_anos",
        "taxa_compra",
        "taxa_pre_anbima",
        "spread_bps",
        "Sinal",
    ]
    colunas_existentes = [c for c in colunas_saida if c in df_cmp_pre.columns]

    print("=== Comparação Tesouro Prefixado x Curva Pré ANBIMA (amostra) ===")
    print(
        df_cmp_pre[colunas_existentes]
        .head(30)
        .to_string(index=False)
    )

    # 9) Resumo rápido por sinal
    print("\n=== Contagem por Sinal (apenas prefixados) ===")
    print(df_cmp_pre["Sinal"].value_counts())


if __name__ == "__main__":
    main()
