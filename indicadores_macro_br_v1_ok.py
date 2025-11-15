# indicadores_macro_br.py
# -*- coding: utf-8 -*-

import requests
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import streamlit as st
from typing import Optional, Dict, List

# =============================================================================
# INDICADORES QUE ESTE APP ACOMPANHA
# =============================================================================
# INFLAÇÃO / PREÇOS (IBGE / SIDRA)
# - IPCA (variação mensal)      -> tabela 1737, v=63
# - IPCA-15 (variação mensal)   -> tabela 3065, v=355
#
# JUROS / CÂMBIO (BCB / SGS)
# - Selic Meta (% a.a.)         -> série 432
# - CDI diário (% a.d.)         -> série 12
# - Dólar PTAX - venda (R$/US$) -> série 10813
#
# ATIVIDADE ECONÔMICA (IBGE)
# - Varejo (PMC) – volume (tabela 8880, varejo restrito, Brasil):
#       v/11708 -> variação M/M-1 com ajuste sazonal
#       v/11710 -> variação acumulada no ano
#       v/11711 -> variação acumulada em 12 meses
#
# - Serviços (PMS) – volume (tabela 5906, Brasil):
#       v/11623 -> variação M/M-1 com ajuste sazonal
#       v/11625 -> variação acumulada no ano
#       v/11626 -> variação acumulada em 12 meses
#
# - Indústria (PIM-PF) – em construção
# =============================================================================


# =============================================================================
# CONFIGURAÇÕES BÁSICAS
# =============================================================================

SGS_SERIES = {
    "selic_meta_aa": 432,
    "cdi_diario": 12,
    "ptax_venda": 10813,
}

IBGE_TABELA_IPCA = 1737
IBGE_VARIAVEL_IPCA = 63   # variação mensal (%)

IBGE_TABELA_IPCA15 = 3065
IBGE_VARIAVEL_IPCA15 = 355

IBGE_NIVEL_BRASIL = "n1/all"  # nível Brasil

# Tabela PMC (comércio varejista – índice e variação do volume de vendas)
IBGE_TABELA_PMC = 8880

# Tabela PMS (serviços – índice e variação do volume de serviços)
IBGE_TABELA_PMS = 5906


# =============================================================================
# FUNÇÕES AUXILIARES DE DATA
# =============================================================================

def _hoje_str() -> str:
    """Data de hoje em dd/mm/aaaa (usado no BCB)."""
    return date.today().strftime("%d/%m/%Y")


def _um_ano_atras_str() -> str:
    """Data de 1 ano atrás em dd/mm/aaaa."""
    dt = date.today() - relativedelta(years=1)
    return dt.strftime("%d/%m/%Y")


def _formata_mes(dt: pd.Timestamp) -> str:
    """Formata data mensal como mm/aaaa."""
    if pd.isna(dt):
        return "-"
    return dt.strftime("%m/%Y")


def _parse_periodo(p: str) -> pd.Timestamp:
    """Converte período do SIDRA (ex: '202510') em datetime (1º dia do mês)."""
    p = str(p)
    if len(p) == 6 and p.isdigit():
        ano = int(p[:4])
        mes = int(p[4:])
        return datetime(ano, mes, 1)
    try:
        return pd.to_datetime(p)
    except Exception:
        return pd.NaT


# =============================================================================
# BANCO CENTRAL (SGS)
# =============================================================================

def buscar_serie_sgs(
    codigo: int,
    data_inicial: Optional[str] = None,
    data_final: Optional[str] = None
) -> pd.DataFrame:
    """
    Busca série temporal na API SGS do Banco Central.
    Retorna DataFrame com colunas ['data', 'valor'].
    """
    if data_inicial is None:
        data_inicial = _um_ano_atras_str()
    if data_final is None:
        data_final = _hoje_str()

    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
        f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
    )

    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    dados = resp.json()

    if not dados:
        return pd.DataFrame(columns=["data", "valor"])

    df = pd.DataFrame(dados)
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["valor"] = pd.to_numeric(df["valor"].str.replace(",", "."), errors="coerce")
    df = df.sort_values("data").reset_index(drop=True)
    return df


def buscar_selic_meta_aa() -> pd.DataFrame:
    """Meta Selic (% a.a.)."""
    return buscar_serie_sgs(SGS_SERIES["selic_meta_aa"])


def buscar_cdi_diario() -> pd.DataFrame:
    """CDI diário (% a.d.), último ano."""
    return buscar_serie_sgs(SGS_SERIES["cdi_diario"])


def buscar_ptax_venda() -> pd.DataFrame:
    """Dólar PTAX - venda (R$/US$)."""
    return buscar_serie_sgs(SGS_SERIES["ptax_venda"])


# =============================================================================
# IBGE / SIDRA GENÉRICO (IPCA, IPCA-15, etc.)
# =============================================================================

def buscar_serie_mensal_ibge(
    tabela: int,
    variavel: int,
    nivel: str = IBGE_NIVEL_BRASIL
) -> pd.DataFrame:
    """
    Busca uma série mensal simples na API SIDRA do IBGE.
    Retorna DataFrame com ['data', 'valor'].
    """
    url = f"https://apisidra.ibge.gov.br/values/t/{tabela}/{nivel}/v/{variavel}/p/all"

    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    dados = resp.json()

    if not dados:
        return pd.DataFrame(columns=["data", "valor"])

    header = dados[0]
    linhas = dados[1:]
    df = pd.DataFrame(linhas)

    # Descobre coluna de período
    col_periodo = None
    for col in df.columns:
        titulo = header.get(col, "")
        if any(p in titulo for p in ["Mês (Código)", "Mês", "Período"]):
            col_periodo = col
            break

    if col_periodo is None:
        if "D3C" in df.columns:
            col_periodo = "D3C"
        else:
            col_periodo = df.columns[0]

    col_valor = "V"  # coluna padrão SIDRA

    df["data"] = df[col_periodo].apply(_parse_periodo)
    df["valor"] = pd.to_numeric(
        df[col_valor].astype(str).str.replace(",", "."),
        errors="coerce"
    )

    df = df[["data", "valor"]].dropna().sort_values("data").reset_index(drop=True)
    return df


def buscar_ipca_ibge() -> pd.DataFrame:
    """IPCA - variação mensal (%)."""
    return buscar_serie_mensal_ibge(IBGE_TABELA_IPCA, IBGE_VARIAVEL_IPCA)


def buscar_ipca15_ibge() -> pd.DataFrame:
    """IPCA-15 - variação mensal (%)."""
    return buscar_serie_mensal_ibge(IBGE_TABELA_IPCA15, IBGE_VARIAVEL_IPCA15)


# =============================================================================
# IBGE / SIDRA – HELPER GENÉRICO PARA PMC / PMS
# =============================================================================

def _buscar_serie_sidra_valor(url: str) -> pd.DataFrame:
    """
    Helper genérico: busca uma série na API do SIDRA
    e devolve DataFrame ['data', 'valor'].
    """
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    dados = resp.json()

    if not dados:
        return pd.DataFrame(columns=["data", "valor"])

    header = dados[0]
    linhas = dados[1:]
    df = pd.DataFrame(linhas)

    col_periodo = None
    for col in df.columns:
        titulo = header.get(col, "")
        if any(p in titulo for p in ["Mês (Código)", "Mês", "Período"]):
            col_periodo = col
            break
    if col_periodo is None:
        if "D3C" in df.columns:
            col_periodo = "D3C"
        else:
            col_periodo = df.columns[0]

    df["data"] = df[col_periodo].apply(_parse_periodo)
    df["valor"] = pd.to_numeric(
        df["V"].astype(str).str.replace(",", "."),
        errors="coerce"
    )

    df = (
        df[["data", "valor"]]
        .dropna()
        .sort_values("data")
        .drop_duplicates(subset=["data"], keep="last")
        .reset_index(drop=True)
    )
    return df


# =============================================================================
# ATIVIDADE ECONÔMICA – PMC (VAREJO) – SÉRIES OFICIAIS
# =============================================================================

def buscar_pmc_var_mom_ajustada() -> pd.DataFrame:
    """
    Série oficial do IBGE:
    PMC - Variação mês/mês imediatamente anterior,
    COM ajuste sazonal (M/M-1), volume de vendas no
    comércio varejista (restrito), Brasil.

    Parâmetros para a API:
    https://apisidra.ibge.gov.br/values/
        t/8880/n1/all/v/11708/p/all/c11046/56734/d/v11708%201
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/8880/n1/all/v/11708/p/all/c11046/56734/d/v11708%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pmc_var_acum_ano() -> pd.DataFrame:
    """
    PMC - Variação acumulada no ano (em relação ao mesmo
    período do ano anterior), volume de vendas, varejo
    restrito, Brasil.
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/8880/n1/all/v/11710/p/all/c11046/56734/d/v11710%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pmc_var_acum_12m() -> pd.DataFrame:
    """
    PMC - Variação acumulada em 12 meses (em relação ao
    período anterior de 12 meses), volume de vendas,
    varejo restrito, Brasil.
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/8880/n1/all/v/11711/p/all/c11046/56734/d/v11711%201"
    )
    return _buscar_serie_sidra_valor(url)


def _resumo_triple_series(
    df_mom: pd.DataFrame,
    df_ano: pd.DataFrame,
    df_12: pd.DataFrame
) -> Dict[str, float]:
    """Helper reutilizado por PMC e PMS."""
    if df_mom.empty and df_ano.empty and df_12.empty:
        return {
            "referencia": "-",
            "var_mensal": float("nan"),
            "acum_ano": float("nan"),
            "acum_12m": float("nan"),
        }

    # data de referência: prioridade M/M-1, depois ano, depois 12m
    if not df_mom.empty:
        data_ref = df_mom["data"].max()
    elif not df_ano.empty:
        data_ref = df_ano["data"].max()
    else:
        data_ref = df_12["data"].max()

    ref_mes = _formata_mes(data_ref)

    def _pega_valor(df: pd.DataFrame) -> float:
        if df.empty:
            return float("nan")
        linha = df[df["data"] == data_ref]
        if linha.empty:
            linha = df.iloc[[-1]]
        return float(linha.iloc[0]["valor"])

    var_mensal = _pega_valor(df_mom)
    acum_ano = _pega_valor(df_ano)
    acum_12m = _pega_valor(df_12)

    return {
        "referencia": ref_mes,
        "var_mensal": var_mensal,
        "acum_ano": acum_ano,
        "acum_12m": acum_12m,
    }


def resumo_pmc_oficial() -> Dict[str, float]:
    """Resumo oficial do varejo (PMC) – volume."""
    df_mom = buscar_pmc_var_mom_ajustada()
    df_ano = buscar_pmc_var_acum_ano()
    df_12 = buscar_pmc_var_acum_12m()
    return _resumo_triple_series(df_mom, df_ano, df_12)


# =============================================================================
# ATIVIDADE ECONÔMICA – PMS (SERVIÇOS) – SÉRIES OFICIAIS
# =============================================================================

def buscar_pms_var_mom_ajustada() -> pd.DataFrame:
    """
    PMS - Variação mês/mês imediatamente anterior, com ajuste sazonal (M/M-1),
    índice de volume de serviços, Brasil.

    Link enviado:
    https://apisidra.ibge.gov.br/values/
        t/5906/n1/all/v/11623/p/all/c11046/56726/d/v11623%201
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/5906/n1/all/v/11623/p/all/c11046/56726/d/v11623%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pms_var_acum_ano() -> pd.DataFrame:
    """
    PMS - Variação acumulada no ano,
    índice de volume de serviços, Brasil.

    Link enviado:
    https://apisidra.ibge.gov.br/values/
        t/5906/n1/all/v/11625/p/all/c11046/56726/d/v11625%201
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/5906/n1/all/v/11625/p/all/c11046/56726/d/v11625%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pms_var_acum_12m() -> pd.DataFrame:
    """
    PMS - Variação acumulada em 12 meses,
    índice de volume de serviços, Brasil.

    Link enviado:
    https://apisidra.ibge.gov.br/values/
        t/5906/n1/all/v/11626/p/all/c11046/56726/d/v11626%201
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/5906/n1/all/v/11626/p/all/c11046/56726/d/v11626%201"
    )
    return _buscar_serie_sidra_valor(url)


def resumo_pms_oficial() -> Dict[str, float]:
    """Resumo oficial dos serviços (PMS) – volume."""
    df_mom = buscar_pms_var_mom_ajustada()
    df_ano = buscar_pms_var_acum_ano()
    df_12 = buscar_pms_var_acum_12m()
    return _resumo_triple_series(df_mom, df_ano, df_12)


# =============================================================================
# CÁLCULOS PARA INFLAÇÃO
# =============================================================================

def _acumula_percentuais(valores: pd.Series) -> float:
    """
    Recebe uma série de variações mensais em % (ex: 0.09, 0.18, ...)
    e retorna o acumulado composto em %.
    """
    if valores.empty:
        return float("nan")
    fator = (1 + valores / 100).prod()
    return (fator - 1) * 100.0


def resumo_inflacao(df: pd.DataFrame) -> Dict[str, float]:
    """
    A partir de um DataFrame de inflação mensal (%), calcula:
    - mês de referência
    - última variação mensal
    - acumulado no ano
    - acumulado em 12 meses
    """
    df = df.sort_values("data").reset_index(drop=True)
    ult = df.iloc[-1]
    ref_mes = _formata_mes(ult["data"])
    ultimo_valor = ult["valor"]  # já em %

    ano_ref = ult["data"].year
    df_ano = df[df["data"].dt.year == ano_ref]

    if not df_ano.empty:
        acum_ano = _acumula_percentuais(df_ano["valor"])
    else:
        acum_ano = float("nan")

    # últimos 12 meses (ou menos, se não houver histórico)
    if len(df) >= 2:
        df_12m = df.tail(12)
        acum_12m = _acumula_percentuais(df_12m["valor"])
    else:
        acum_12m = float("nan")

    return {
        "referencia": ref_mes,
        "mensal": ultimo_valor,
        "acum_ano": acum_ano,
        "acum_12m": acum_12m,
    }


# =============================================================================
# CÂMBIO – RESUMO
# =============================================================================

def resumo_cambio(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    """
    Para o câmbio (PTAX, em R$/US$), calcula:
    - último valor
    - variação no ano (%)
    - variação em 12 meses (%)
    """
    if df.empty:
        return {"ultimo": None, "var_ano": None, "var_12m": None, "ultima_data": None}

    df = df.sort_values("data").reset_index(drop=True)
    ult = df.iloc[-1]
    ultimo_valor = ult["valor"]
    ultima_data = ult["data"]

    ano_ref = ultima_data.year
    df_ano = df[df["data"].dt.year == ano_ref]
    if not df_ano.empty:
        inicio_ano = df_ano.iloc[0]["valor"]
        var_ano = (ultimo_valor / inicio_ano - 1) * 100.0
    else:
        var_ano = None

    corte_12m = ultima_data - relativedelta(years=1)
    df_12m = df[df["data"] >= corte_12m]
    if not df_12m.empty:
        inicio_12m = df_12m.iloc[0]["valor"]
        var_12m = (ultimo_valor / inicio_12m - 1) * 100.0
    else:
        var_12m = None

    return {
        "ultimo": ultimo_valor,
        "ultima_data": ultima_data,
        "var_ano": var_ano,
        "var_12m": var_12m,
    }


# =============================================================================
# FUNÇÕES PARA MONTAR TABELAS RESUMO
# =============================================================================

def montar_tabela_inflacao() -> pd.DataFrame:
    """
    Monta uma tabela com IPCA e IPCA-15:
    - Mês de referência
    - Valor (mensal)
    - Acumulado no ano
    - Acumulado em 12 meses
    """
    linhas: List[Dict[str, str]] = []

    # IPCA
    try:
        df_ipca = buscar_ipca_ibge()
        if not df_ipca.empty:
            r = resumo_inflacao(df_ipca)
            linhas.append({
                "Indicador": "IPCA (variação mensal)",
                "Mês ref.": r["referencia"],
                "Valor (mensal)": f"{r['mensal']:.2f}%",
                "Acum. no ano": (
                    f"{r['acum_ano']:.2f}%" if pd.notna(r["acum_ano"]) else "-"
                ),
                "Acum. 12 meses": (
                    f"{r['acum_12m']:.2f}%" if pd.notna(r["acum_12m"]) else "-"
                ),
                "Fonte": "IBGE / SIDRA (Tabela 1737)",
            })
        else:
            linhas.append({
                "Indicador": "IPCA (variação mensal)",
                "Mês ref.": "-",
                "Valor (mensal)": "sem dados",
                "Acum. no ano": "-",
                "Acum. 12 meses": "-",
                "Fonte": "IBGE / SIDRA (Tabela 1737)",
            })
    except Exception as e:
        linhas.append({
            "Indicador": "IPCA (variação mensal)",
            "Mês ref.": "-",
            "Valor (mensal)": f"Erro: {e}",
            "Acum. no ano": "-",
            "Acum. 12 meses": "-",
            "Fonte": "IBGE / SIDRA",
        })

    # IPCA-15
    try:
        df_ipca15 = buscar_ipca15_ibge()
        if not df_ipca15.empty:
            r = resumo_inflacao(df_ipca15)
            linhas.append({
                "Indicador": "IPCA-15 (variação mensal)",
                "Mês ref.": r["referencia"],
                "Valor (mensal)": f"{r['mensal']:.2f}%",
                "Acum. no ano": (
                    f"{r['acum_ano']:.2f}%" if pd.notna(r["acum_ano"]) else "-"
                ),
                "Acum. 12 meses": (
                    f"{r['acum_12m']:.2f}%" if pd.notna(r["acum_12m"]) else "-"
                ),
                "Fonte": "IBGE / SIDRA (Tabela 3065)",
            })
        else:
            linhas.append({
                "Indicador": "IPCA-15 (variação mensal)",
                "Mês ref.": "-",
                "Valor (mensal)": "sem dados",
                "Acum. no ano": "-",
                "Acum. 12 meses": "-",
                "Fonte": "IBGE / SIDRA (Tabela 3065)",
            })
    except Exception as e:
        linhas.append({
            "Indicador": "IPCA-15 (variação mensal)",
            "Mês ref.": "-",
            "Valor (mensal)": f"Erro: {e}",
            "Acum. no ano": "-",
            "Acum. 12 meses": "-",
            "Fonte": "IBGE / SIDRA",
        })

    return pd.DataFrame(linhas)


def montar_tabela_selic_meta() -> pd.DataFrame:
    """
    Tabela com Selic Meta:
    - Nível atual
    - Nível no início do ano
    - Nível há 12 meses
    """
    linhas: List[Dict[str, str]] = []

    try:
        df = buscar_selic_meta_aa()
        if not df.empty:
            df = df.sort_values("data").reset_index(drop=True)
            ult = df.iloc[-1]
            ultima_data = ult["data"]
            ultimo = ult["valor"]

            ano_ref = ultima_data.year
            df_ano = df[df["data"].dt.year == ano_ref]
            if not df_ano.empty:
                inicio_ano_val = df_ano.iloc[0]["valor"]
            else:
                inicio_ano_val = None

            corte_12m = ultima_data - relativedelta(years=1)
            df_12m = df[df["data"] >= corte_12m]
            if not df_12m.empty:
                nivel_12m_val = df_12m.iloc[0]["valor"]
            else:
                nivel_12m_val = None

            linhas.append({
                "Indicador": "Selic Meta",
                "Data": ultima_data.strftime("%d/%m/%Y"),
                "Nível atual": f"{ultimo:.2f}% a.a.",
                "Início do ano": (
                    f"{inicio_ano_val:.2f}% a.a."
                    if inicio_ano_val is not None else "-"
                ),
                "Há 12 meses": (
                    f"{nivel_12m_val:.2f}% a.a."
                    if nivel_12m_val is not None else "-"
                ),
                "Fonte": f"BCB / SGS ({SGS_SERIES['selic_meta_aa']})",
            })
        else:
            linhas.append({
                "Indicador": "Selic Meta",
                "Data": "-",
                "Nível atual": "sem dados",
                "Início do ano": "-",
                "Há 12 meses": "-",
                "Fonte": "BCB / SGS",
            })
    except Exception as e:
        linhas.append({
            "Indicador": "Selic Meta",
            "Data": "-",
            "Nível atual": f"Erro: {e}",
            "Início do ano": "-",
            "Há 12 meses": "-",
            "Fonte": "BCB / SGS",
        })

    return pd.DataFrame(linhas)


def montar_tabela_cdi() -> pd.DataFrame:
    """
    Tabela com CDI diário:
    - Nível diário (% a.d.)
    - Projeção de mês (21 dias úteis)
    - Projeção de ano (252 dias úteis)
    - CDI acumulado nos últimos 12 meses passados
    """
    linhas: List[Dict[str, str]] = []

    try:
        df = buscar_cdi_diario()
        if not df.empty:
            df = df.sort_values("data").reset_index(drop=True)
            ult = df.iloc[-1]
            ultima_data = ult["data"]
            taxa_dia = ult["valor"]  # % a.d.

            # Projeções mantendo a taxa de hoje
            fator_mes = (1 + taxa_dia / 100) ** 21 - 1
            fator_ano = (1 + taxa_dia / 100) ** 252 - 1

            # CDI acumulado nos ~últimos 12 meses (janela do DataFrame)
            fator_12m_real = (1 + df["valor"] / 100).prod() - 1

            linhas.append({
                "Indicador": "CDI (over) diário",
                "Data": ultima_data.strftime("%d/%m/%Y"),
                "Nível (a.d.)": f"{taxa_dia:.4f}% a.d.",
                "Proj. mês": f"{fator_mes * 100:.2f}%",
                "Proj. ano": f"{fator_ano * 100:.2f}%",
                "CDI 12m (passado)": f"{fator_12m_real * 100:.2f}%",
                "Fonte": f"BCB / SGS ({SGS_SERIES['cdi_diario']})",
            })
        else:
            linhas.append({
                "Indicador": "CDI (over) diário",
                "Data": "-",
                "Nível (a.d.)": "sem dados",
                "Proj. mês": "-",
                "Proj. ano": "-",
                "CDI 12m (passado)": "-",
                "Fonte": "BCB / SGS",
            })
    except Exception as e:
        linhas.append({
            "Indicador": "CDI (over) diário",
            "Data": "-",
            "Nível (a.d.)": f"Erro: {e}",
            "Proj. mês": "-",
            "Proj. ano": "-",
            "CDI 12m (passado)": "-",
            "Fonte": "BCB / SGS",
        })

    return pd.DataFrame(linhas)


def montar_tabela_ptax() -> pd.DataFrame:
    """
    Tabela com Dólar PTAX - venda:
    - Nível atual
    - Variação no ano (%)
    - Variação em 12 meses (%)
    """
    linhas: List[Dict[str, str]] = []

    try:
        df = buscar_ptax_venda()
        r = resumo_cambio(df)
        if r["ultimo"] is not None:
            ultima_data = r["ultima_data"].strftime("%d/%m/%Y")
            nivel = f"R$ {r['ultimo']:.4f}"
            var_ano = f"{r['var_ano']:+.2f}%" if r["var_ano"] is not None else "-"
            var_12m = f"{r['var_12m']:+.2f}%" if r["var_12m"] is not None else "-"
        else:
            ultima_data = "-"
            nivel = "sem dados"
            var_ano = "-"
            var_12m = "-"

        linhas.append({
            "Indicador": "Dólar PTAX - venda",
            "Data": ultima_data,
            "Nível": nivel,
            "Var. ano": var_ano,
            "Var. 12m": var_12m,
            "Fonte": f"BCB / SGS ({SGS_SERIES['ptax_venda']})",
        })
    except Exception as e:
        linhas.append({
            "Indicador": "Dólar PTAX - venda",
            "Data": "-",
            "Nível": f"Erro: {e}",
            "Var. ano": "-",
            "Var. 12m": "-",
            "Fonte": "BCB / SGS",
        })

    return pd.DataFrame(linhas)


def montar_tabela_atividade_economica() -> pd.DataFrame:
    """
    Bloco de Atividade Econômica (IBGE).

    - Varejo (PMC): usa as três séries oficiais do IBGE
      (M/M-1 ajustado, acumulado no ano, acumulado em 12 meses).
    - Serviços (PMS): idem, com tabela 5906.
    - Indústria (PIM-PF): placeholder.
    """
    linhas: List[Dict[str, str]] = []

    # VAREJO (PMC) – volume
    try:
        r_pmc = resumo_pmc_oficial()
        if r_pmc["referencia"] != "-":
            linhas.append({
                "Indicador": "Varejo (PMC) – volume",
                "Mês ref.": r_pmc["referencia"],
                "Var. mensal": (
                    f"{r_pmc['var_mensal']:.1f}%"
                    if pd.notna(r_pmc["var_mensal"]) else "-"
                ),
                "Acum. no ano": (
                    f"{r_pmc['acum_ano']:.1f}%"
                    if pd.notna(r_pmc["acum_ano"]) else "-"
                ),
                "Acum. 12 meses": (
                    f"{r_pmc['acum_12m']:.1f}%"
                    if pd.notna(r_pmc["acum_12m"]) else "-"
                ),
                "Fonte": "IBGE / PMC (SIDRA – Tabela 8880)",
            })
        else:
            linhas.append({
                "Indicador": "Varejo (PMC) – volume",
                "Mês ref.": "-",
                "Var. mensal": "sem dados",
                "Acum. no ano": "-",
                "Acum. 12 meses": "-",
                "Fonte": "IBGE / PMC (SIDRA – Tabela 8880)",
            })
    except Exception as e:
        linhas.append({
            "Indicador": "Varejo (PMC) – volume",
            "Mês ref.": "-",
            "Var. mensal": f"Erro: {e}",
            "Acum. no ano": "-",
            "Acum. 12 meses": "-",
            "Fonte": "IBGE / PMC (SIDRA – Tabela 8880)",
        })

    # SERVIÇOS (PMS) – volume
    try:
        r_pms = resumo_pms_oficial()
        if r_pms["referencia"] != "-":
            linhas.append({
                "Indicador": "Serviços (PMS) – volume",
                "Mês ref.": r_pms["referencia"],
                "Var. mensal": (
                    f"{r_pms['var_mensal']:.1f}%"
                    if pd.notna(r_pms["var_mensal"]) else "-"
                ),
                "Acum. no ano": (
                    f"{r_pms['acum_ano']:.1f}%"
                    if pd.notna(r_pms["acum_ano"]) else "-"
                ),
                "Acum. 12 meses": (
                    f"{r_pms['acum_12m']:.1f}%"
                    if pd.notna(r_pms["acum_12m"]) else "-"
                ),
                "Fonte": "IBGE / PMS (SIDRA – Tabela 5906)",
            })
        else:
            linhas.append({
                "Indicador": "Serviços (PMS) – volume",
                "Mês ref.": "-",
                "Var. mensal": "sem dados",
                "Acum. no ano": "-",
                "Acum. 12 meses": "-",
                "Fonte": "IBGE / PMS (SIDRA – Tabela 5906)",
            })
    except Exception as e:
        linhas.append({
            "Indicador": "Serviços (PMS) – volume",
            "Mês ref.": "-",
            "Var. mensal": f"Erro: {e}",
            "Acum. no ano": "-",
            "Acum. 12 meses": "-",
            "Fonte": "IBGE / PMS (SIDRA – Tabela 5906)",
        })

    # INDÚSTRIA (PIM-PF) – placeholder
    linhas.append({
        "Indicador": "Indústria (PIM-PF) – produção física",
        "Mês ref.": "-",
        "Var. mensal": "em construção",
        "Acum. no ano": "em construção",
        "Acum. 12 meses": "em construção",
        "Fonte": "IBGE / PIM-PF (SIDRA)",
    })

    return pd.DataFrame(linhas)


# =============================================================================
# STREAMLIT - INTERFACE
# =============================================================================

def main():
    st.set_page_config(
        page_title="Indicadores Macro Brasil",
        layout="wide",
    )

    st.title("Indicadores Macro Brasil")
    st.caption("Foco em exatidão dos dados - IBGE (SIDRA) e Banco Central (SGS).")

    st.write("---")

    with st.spinner("Buscando dados mais recentes..."):
        df_infla = montar_tabela_inflacao()
        df_ativ = montar_tabela_atividade_economica()
        df_selic = montar_tabela_selic_meta()
        df_cdi = montar_tabela_cdi()
        df_ptax = montar_tabela_ptax()

    # INFLAÇÃO
    st.subheader("📊 Inflação (IBGE)")
    st.dataframe(
        df_infla.set_index("Indicador"),
        width="stretch",
    )

    # ATIVIDADE ECONÔMICA
    st.subheader("🏭 Atividade Econômica (IBGE)")
    st.dataframe(
        df_ativ.set_index("Indicador"),
        width="stretch",
    )

    # JUROS E CÂMBIO
    st.subheader("💰 Juros e Câmbio (Banco Central)")

    st.markdown("**Taxa básica – Selic Meta**")
    st.dataframe(
        df_selic.set_index("Indicador"),
        width="stretch",
    )

    st.markdown("**CDI diário – projeções (mantida a taxa de hoje)**")
    st.dataframe(
        df_cdi.set_index("Indicador"),
        width="stretch",
    )

    st.markdown("**Câmbio – Dólar PTAX (venda)**")
    st.dataframe(
        df_ptax.set_index("Indicador"),
        width="stretch",
    )

    st.write("---")
    st.caption(
        "Atualize os dados recarregando a página ou rodando novamente "
        "`streamlit run indicadores_macro_br.py`."
    )


if __name__ == "__main__":
    main()
