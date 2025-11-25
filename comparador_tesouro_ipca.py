# comparador_tesouro_ipca.py
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

    # 3) Prepara curva IPCA ANBIMA (vértice x juro real)
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

    # 4) Limpa Tesouro (tira linhas sem taxa) e ordena por prazo
    df_tesouro_valid = (
        df_tesouro[df_tesouro["taxa_compra"].notna()]
        .copy()
        .sort_values("prazo_anos")
    )

    # 5) Garante que as colunas de prazo são FLOAT nas duas bases
    df_ipca_anbima["prazo_anos_curva"] = (
        pd.to_numeric(df_ipca_anbima["prazo_anos_curva"], errors="coerce")
        .astype("float64")
    )
    df_tesouro_valid["prazo_anos"] = (
        pd.to_numeric(df_tesouro_valid["prazo_anos"], errors="coerce")
        .astype("float64")
    )

    # tira linhas sem prazo definido
    df_ipca_anbima = df_ipca_anbima.dropna(subset=["prazo_anos_curva"])
    df_tesouro_valid = df_tesouro_valid.dropna(subset=["prazo_anos"])

    # 6) FILTRA APENAS TÍTULOS IPCA+ (com ou sem juros semestrais)
    if "nome_titulo" in df_tesouro_valid.columns:
        mask_ipca = df_tesouro_valid["nome_titulo"].str.contains(
            "IPCA", case=False, na=False
        )
        df_tesouro_valid = df_tesouro_valid[mask_ipca].copy()

    # 7) Casa cada título do Tesouro com o vértice da curva real mais próximo (por prazo)
    df_cmp_ipca = pd.merge_asof(
        df_tesouro_valid.sort_values("prazo_anos"),
        df_ipca_anbima.sort_values("prazo_anos_curva"),
        left_on="prazo_anos",
        right_on="prazo_anos_curva",
        direction="nearest",
    )

    # 8) Calcula o spread real (Tesouro - Curva real) em pontos-base
    df_cmp_ipca["spread"] = df_cmp_ipca["taxa_compra"] - df_cmp_ipca["taxa_ipca_anbima"]
    df_cmp_ipca["spread_bps"] = df_cmp_ipca["spread"] * 100  # taxas já são % a.a.

    # 8.1) Classifica como Barato / Caro / No preço
    df_cmp_ipca["Sinal"] = df_cmp_ipca["spread_bps"].apply(classificar_spread)

    # 8.2) Filtro simples de “lixo”: remove taxas de compra muito baixas (0.01, 0.05 etc.)
    df_cmp_ipca = df_cmp_ipca[df_cmp_ipca["taxa_compra"] > 1].copy()

    # 8.3) Ordena pelos mais "baratos" (maior spread para cima)
    df_cmp_ipca = df_cmp_ipca.sort_values("spread_bps", ascending=False)

    # 9) Mostra o resultado (primeiras 30 linhas)
    colunas_saida = [
        "nome_titulo",
        "data_vencimento",
        "prazo_anos",
        "taxa_compra",
        "taxa_ipca_anbima",
        "spread_bps",
        "Sinal",
    ]
    colunas_existentes = [c for c in colunas_saida if c in df_cmp_ipca.columns]

    print("=== Comparação Tesouro IPCA+ x Curva IPCA ANBIMA (amostra) ===")
    print(
        df_cmp_ipca[colunas_existentes]
        .head(30)
        .to_string(index=False)
    )

    # 10) Resumo rápido por sinal
    print("\n=== Contagem por Sinal (apenas IPCA+) ===")
    print(df_cmp_ipca["Sinal"].value_counts())


if __name__ == "__main__":
    main()
