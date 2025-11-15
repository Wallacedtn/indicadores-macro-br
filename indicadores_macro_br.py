# indicadores_macro_br.py
# -*- coding: utf-8 -*-

import requests
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import streamlit as st
from typing import Optional, Dict, List
from functools import lru_cache

# =============================================================================
# HELPER DE REDE COM RETRY
# =============================================================================

def _get_with_retry(
    url: str,
    max_attempts: int = 3,
    timeout: int = 30,
) -> requests.Response:
    """
    Faz GET com poucas tentativas e timeout configurável.
    - Retry só em Timeout / ConnectionError.
    - Erros 4xx/5xx não fazem retry (provavelmente problema de URL/servidor).
    """
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_exc = e
            if attempt == max_attempts:
                raise
        except requests.exceptions.RequestException:
            # 4xx/5xx ou outros erros: não adianta tentar de novo
            raise

    if last_exc:
        raise last_exc
    raise RuntimeError("Falha inesperada em _get_with_retry")


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
# - Indústria (PIM-PF) – volume (tabela 8888, Brasil, Indústria Geral):
#       v/11601 -> variação M/M-1 com ajuste sazonal
#       v/11603 -> variação acumulada no ano
#       v/11604 -> variação acumulada em 12 meses
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

# API OLINDA – EXPECTATIVAS FOCUS
FOCUS_BASE_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata"
)


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


def _dois_anos_atras_str() -> str:
    """Data de 2 anos atrás em dd/mm/aaaa."""
    dt = date.today() - relativedelta(years=2)
    return dt.strftime("%d/%m/%Y")


def _formata_mes(dt: pd.Timestamp) -> str:
    """Formata data mensal como mm/aaaa."""
    if pd.isna(dt):
        return "-"
    return dt.strftime("%m/%Y")


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


# =============================================================================
# BANCO CENTRAL (SGS) – FUNÇÃO GENÉRICA COM CACHE + RETRY
# =============================================================================

@lru_cache(maxsize=32)
def _buscar_serie_sgs_cached(
    codigo: int,
    data_inicial: Optional[str],
    data_final: Optional[str],
) -> pd.DataFrame:
    """
    Implementação interna com cache. Não chame diretamente;
    use buscar_serie_sgs().
    """
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
        f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
    )

    resp = _get_with_retry(url, max_attempts=3, timeout=30)
    dados = resp.json()

    if not dados:
        return pd.DataFrame(columns=["data", "valor"])

    df = pd.DataFrame(dados)
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["valor"] = pd.to_numeric(df["valor"].astype(str).str.replace(",", "."), errors="coerce")
    df = df.sort_values("data").reset_index(drop=True)
    return df


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
    return _buscar_serie_sgs_cached(codigo, data_inicial, data_final).copy()


def buscar_selic_meta_aa() -> pd.DataFrame:
    """Meta Selic (% a.a.). Último ano de dados."""
    return buscar_serie_sgs(SGS_SERIES["selic_meta_aa"])


def buscar_cdi_diario() -> pd.DataFrame:
    """CDI diário (% a.d.), último ano."""
    return buscar_serie_sgs(SGS_SERIES["cdi_diario"])


def buscar_ptax_venda() -> pd.DataFrame:
    """Dólar PTAX - venda (R$/US$). Usa janela de 2 anos para variações."""
    return buscar_serie_sgs(
        SGS_SERIES["ptax_venda"],
        data_inicial=_dois_anos_atras_str(),
        data_final=_hoje_str(),
    )


# =============================================================================
# IBGE / SIDRA GENÉRICO (IPCA, IPCA-15, etc.) COM CACHE + p/last60
# =============================================================================

@lru_cache(maxsize=64)
def _buscar_serie_mensal_ibge_cached(
    tabela: int,
    variavel: int,
    nivel: str,
) -> pd.DataFrame:
    """
    Implementação interna com cache. Não chame diretamente;
    use buscar_serie_mensal_ibge().

    IMPORTANTE:
    - Usa p/last60 (últimos 60 meses), e não p/all,
      para evitar respostas gigantes do SIDRA ao longo do tempo.
    """
    url = (
        f"https://apisidra.ibge.gov.br/values/"
        f"t/{tabela}/{nivel}/v/{variavel}/p/last60"
    )

    resp = _get_with_retry(url, max_attempts=3, timeout=30)
    dados = resp.json()

    if not dados:
        return pd.DataFrame(columns=["data", "valor"])

    header = dados[0]
    linhas = dados[1:]
    df = pd.DataFrame(linhas)

    # Descobre coluna de período (mais robusto)
    col_periodo = None
    for col in df.columns:
        titulo = str(header.get(col, "")).lower()
        if any(p in titulo for p in ["mês (código)", "mes (código)", "mês", "mes", "período", "periodo"]):
            col_periodo = col
            break

    if col_periodo is None:
        if "D3C" in df.columns:
            col_periodo = "D3C"
        elif "D2C" in df.columns:
            col_periodo = "D2C"
        else:
            col_periodo = df.columns[0]

    col_valor = "V"  # coluna padrão SIDRA

    df["data"] = df[col_periodo].apply(_parse_periodo)
    df["valor"] = pd.to_numeric(
        df[col_valor].astype(str).str.replace(",", "."),
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


def buscar_serie_mensal_ibge(
    tabela: int,
    variavel: int,
    nivel: str = IBGE_NIVEL_BRASIL
) -> pd.DataFrame:
    """
    Busca uma série mensal simples na API SIDRA do IBGE.
    Retorna DataFrame com ['data', 'valor'].
    """
    return _buscar_serie_mensal_ibge_cached(tabela, variavel, nivel).copy()


def buscar_ipca_ibge() -> pd.DataFrame:
    """IPCA - variação mensal (%)."""
    return buscar_serie_mensal_ibge(IBGE_TABELA_IPCA, IBGE_VARIAVEL_IPCA)


def buscar_ipca15_ibge() -> pd.DataFrame:
    """IPCA-15 - variação mensal (%)."""
    return buscar_serie_mensal_ibge(IBGE_TABELA_IPCA15, IBGE_VARIAVEL_IPCA15)


# =============================================================================
# IBGE / SIDRA – HELPER GENÉRICO PARA PMC / PMS / PIM (com retry)
# =============================================================================

@lru_cache(maxsize=128)
def _buscar_serie_sidra_valor_cached(url: str) -> pd.DataFrame:
    """
    Helper genérico: busca uma série na API do SIDRA
    e devolve DataFrame ['data', 'valor'].
    Implementação com cache.
    """
    resp = _get_with_retry(url, max_attempts=3, timeout=30)
    dados = resp.json()

    if not dados:
        return pd.DataFrame(columns=["data", "valor"])

    header = dados[0]
    linhas = dados[1:]
    df = pd.DataFrame(linhas)

    # Detecta coluna de período de forma robusta
    col_periodo = None
    for col in df.columns:
        titulo = str(header.get(col, "")).lower()
        if any(p in titulo for p in ["mês (código)", "mes (código)", "mês", "mes", "período", "periodo"]):
            col_periodo = col
            break

    if col_periodo is None:
        if "D3C" in df.columns:
            col_periodo = "D3C"
        elif "D2C" in df.columns:
            col_periodo = "D2C"
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


def _buscar_serie_sidra_valor(url: str) -> pd.DataFrame:
    """Wrapper sem cache mutável (retorna cópia)."""
    return _buscar_serie_sidra_valor_cached(url).copy()


# =============================================================================
# ATIVIDADE ECONÔMICA – PMC / PMS / PIM
# =============================================================================

def buscar_pmc_var_mom_ajustada() -> pd.DataFrame:
    """
    PMC - Variação mês/mês imediatamente anterior,
    com ajuste sazonal (M/M-1), volume de vendas no
    comércio varejista (restrito), Brasil.
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/8880/n1/all/v/11708/p/last60/c11046/56734/d/v11708%201"
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
        "t/8880/n1/all/v/11710/p/last60/c11046/56734/d/v11710%201"
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
        "t/8880/n1/all/v/11711/p/last60/c11046/56734/d/v11711%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pms_var_mom_ajustada() -> pd.DataFrame:
    """
    PMS - Variação mês/mês imediatamente anterior, com ajuste sazonal (M/M-1),
    índice de volume de serviços, Brasil.
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/5906/n1/all/v/11623/p/last60/c11046/56726/d/v11623%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pms_var_acum_ano() -> pd.DataFrame:
    """
    PMS - Variação acumulada no ano,
    índice de volume de serviços, Brasil.
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/5906/n1/all/v/11625/p/last60/c11046/56726/d/v11625%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pms_var_acum_12m() -> pd.DataFrame:
    """
    PMS - Variação acumulada em 12 meses,
    índice de volume de serviços, Brasil.
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/5906/n1/all/v/11626/p/last60/c11046/56726/d/v11626%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pim_var_mom_ajustada() -> pd.DataFrame:
    """
    PIM-PF – Variação mês/mês imediatamente anterior (%), com ajuste sazonal.
    Fonte oficial: tabela 8888, variável 11601, Brasil, Indústria Geral.
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/8888/n1/all/v/11601/p/last60/c544/129314/d/v11601%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pim_var_acum_ano() -> pd.DataFrame:
    """
    PIM-PF – Variação acumulada no ano (%).
    Fonte: tabela 8888, variável 11603.
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/8888/n1/all/v/11603/p/last60/c544/129314/d/v11603%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pim_var_acum_12m() -> pd.DataFrame:
    """
    PIM-PF – Variação acumulada em 12 meses (%).
    Fonte: tabela 8888, variável 11604.
    """
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/8888/n1/all/v/11604/p/last60/c544/129314/d/v11604%201"
    )
    return _buscar_serie_sidra_valor(url)


# =============================================================================
# RESUMOS PMC / PMS / PIM
# =============================================================================

def _resumo_triple_series(
    df_mom: pd.DataFrame,
    df_ano: pd.DataFrame,
    df_12: pd.DataFrame
) -> Dict[str, float]:
    """Helper reutilizado por PMC, PMS e PIM-PF."""
    if df_mom.empty and df_ano.empty and df_12.empty:
        return {
            "referencia": "-",
            "var_mensal": float("nan"),
            "acum_ano": float("nan"),
            "acum_12m": float("nan"),
        }

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


def resumo_pms_oficial() -> Dict[str, float]:
    """Resumo oficial dos serviços (PMS) – volume."""
    df_mom = buscar_pms_var_mom_ajustada()
    df_ano = buscar_pms_var_acum_ano()
    df_12 = buscar_pms_var_acum_12m()
    return _resumo_triple_series(df_mom, df_ano, df_12)


def resumo_pim_oficial() -> Dict[str, float]:
    """Resumo da indústria (PIM-PF) – produção física."""
    df_mom = buscar_pim_var_mom_ajustada()
    df_ano = buscar_pim_var_acum_ano()
    df_12 = buscar_pim_var_acum_12m()
    return _resumo_triple_series(df_mom, df_ano, df_12)


# =============================================================================
# INFLAÇÃO – CÁLCULOS
# =============================================================================

def _acumula_percentuais(valores: pd.Series) -> float:
    """
    Recebe uma série de variações mensais em % e retorna o acumulado composto.
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
    if df.empty:
        return {
            "referencia": "-",
            "mensal": float("nan"),
            "acum_ano": float("nan"),
            "acum_12m": float("nan"),
        }

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
# CÂMBIO – RESUMO (níveis + variações)
# =============================================================================

def resumo_cambio(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    """
    Para o câmbio (PTAX, em R$/US$), calcula:
    - último valor (cotação atual)
    - valor há 12 meses e 24 meses
    - variação no ano (%)
    - variação em 12 meses (%)
    - variação em 24 meses (%)
    """
    if df.empty:
        return {
            "ultimo": None,
            "ultima_data": None,
            "valor_12m": None,
            "data_12m": None,
            "valor_24m": None,
            "data_24m": None,
            "var_ano": None,
            "var_12m": None,
            "var_24m": None,
        }

    df = df.sort_values("data").reset_index(drop=True)
    ult = df.iloc[-1]
    ultima_data = ult["data"]
    ultimo_valor = ult["valor"]

    # Variação no ano (YTD)
    ano_ref = ultima_data.year
    df_ano = df[df["data"].dt.year == ano_ref]
    if not df_ano.empty:
        inicio_ano = df_ano.iloc[0]["valor"]
        var_ano = (ultimo_valor / inicio_ano - 1) * 100.0
    else:
        var_ano = None

    # Há 12 meses
    corte_12m = ultima_data - relativedelta(years=1)
    df_12m = df[df["data"] >= corte_12m]
    if not df_12m.empty:
        valor_12m = df_12m.iloc[0]["valor"]
        data_12m = df_12m.iloc[0]["data"]
        var_12m = (ultimo_valor / valor_12m - 1) * 100.0
    else:
        valor_12m = None
        data_12m = None
        var_12m = None

    # Há 24 meses
    corte_24m = ultima_data - relativedelta(years=2)
    df_24m = df[df["data"] >= corte_24m]
    if not df_24m.empty:
        valor_24m = df_24m.iloc[0]["valor"]
        data_24m = df_24m.iloc[0]["data"]
        var_24m = (ultimo_valor / valor_24m - 1) * 100.0
    else:
        valor_24m = None
        data_24m = None
        var_24m = None

    return {
        "ultimo": ultimo_valor,
        "ultima_data": ultima_data,
        "valor_12m": valor_12m,
        "data_12m": data_12m,
        "valor_24m": valor_24m,
        "data_24m": data_24m,
        "var_ano": var_ano,
        "var_12m": var_12m,
        "var_24m": var_24m,
    }


# =============================================================================
# EXPECTATIVAS FOCUS
# =============================================================================

def buscar_focus_expectativa_anual(indicador, detalhe, ano_desejado):
    """Busca a mediana das expectativas anuais do Focus para um indicador específico."""
    base_url = "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
    
    url = (
        base_url +
        "ExpectativasMercadoAnuais?"
        "$top=50&$format=json&"
        f"$filter=Indicador%20eq%20'{indicador}'%20and%20IndicadorDetalhe%20eq%20'{detalhe}'"
    )

    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        dados = r.json()["value"]

        # Filtra pelo ano desejado (ex: 2025)
        dados_ano = [item for item in dados if item.get("DataReferencia") == str(ano_desejado)]
        if not dados_ano:
            return "-"

        # Pega mediana
        mediana = dados_ano[0].get("Mediana")
        if mediana is None:
            return "-"

        return f"{mediana:.2f}"

    except Exception as e:
        return f"Erro: {e}"

def formatar_focus_valor(valor: Optional[float], tipo: str) -> str:
    """
    tipo:
      - 'percent' -> formata como 4.55%
      - 'cambio'  -> formata como R$ 5.40
    """
    if valor is None:
        return "-"

    if tipo == "cambio":
        return f"R$ {valor:.2f}"
    else:
        return f"{valor:.2f}%"


# =============================================================================
# TABELAS RESUMO
# =============================================================================

def montar_tabela_inflacao() -> pd.DataFrame:
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
                    f"{r['acum_ano']:.2f}%"
                    if pd.notna(r["acum_ano"]) else "-"
                ),
                "Acum. 12 meses": (
                    f"{r['acum_12m']:.2f}%"
                    if pd.notna(r["acum_12m"]) else "-"
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
                    f"{r['acum_ano']:.2f}%"
                    if pd.notna(r["acum_ano"]) else "-"
                ),
                "Acum. 12 meses": (
                    f"{r['acum_12m']:.2f}%"
                    if pd.notna(r["acum_12m"]) else "-"
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
    linhas: List[Dict[str, str]] = []

    try:
        df = buscar_cdi_diario()
        if df.empty:
            raise ValueError("Sem dados do CDI.")

        df = df.sort_values("data").reset_index(drop=True)

        ult = df.iloc[-1]
        data_ult = ult["data"]
        taxa_ult = ult["valor"]  # % a.d.

        ano_ref = data_ult.year
        mes_ref = data_ult.month

        df_mes = df[
            (df["data"].dt.year == ano_ref) &
            (df["data"].dt.month == mes_ref)
        ]
        if not df_mes.empty:
            fator_mes = (1 + df_mes["valor"] / 100).prod()
            cdi_mes = (fator_mes - 1) * 100.0
        else:
            cdi_mes = float("nan")

        df_ano = df[df["data"].dt.year == ano_ref]
        if not df_ano.empty:
            fator_ano = (1 + df_ano["valor"] / 100).prod()
            cdi_ano = (fator_ano - 1) * 100.0
        else:
            cdi_ano = float("nan")

        corte_12m = data_ult - relativedelta(years=1)
        df_12m = df[df["data"] > corte_12m]
        if not df_12m.empty:
            fator_12m = (1 + df_12m["valor"] / 100).prod()
            cdi_12m = (fator_12m - 1) * 100.0
        else:
            cdi_12m = float("nan")

        linhas.append({
            "Indicador": "CDI (over) diário",
            "Data ref.": data_ult.strftime("%d/%m/%Y"),
            "Nível diário": f"{taxa_ult:.4f}% a.d.",
            "CDI no mês": f"{cdi_mes:.2f}%" if pd.notna(cdi_mes) else "-",
            "CDI no ano": f"{cdi_ano:.2f}%" if pd.notna(cdi_ano) else "-",
            "CDI em 12 meses": f"{cdi_12m:.2f}%" if pd.notna(cdi_12m) else "-",
            "Fonte": f"BCB / SGS ({SGS_SERIES['cdi_diario']})",
        })

    except Exception as e:
        linhas.append({
            "Indicador": "CDI (over) diário",
            "Data ref.": "-",
            "Nível diário": f"Erro: {e}",
            "CDI no mês": "-",
            "CDI no ano": "-",
            "CDI em 12 meses": "-",
            "Fonte": "BCB / SGS",
        })

    return pd.DataFrame(linhas)


def montar_tabela_ptax() -> pd.DataFrame:
    linhas: List[Dict[str, str]] = []

    try:
        df = buscar_ptax_venda()
        r = resumo_cambio(df)

        if r["ultimo"] is not None:
            ultima_data_str = r["ultima_data"].strftime("%d/%m/%Y")
            nivel_atual = f"R$ {r['ultimo']:.4f}"

            if r["valor_12m"] is not None and r["data_12m"] is not None:
                nivel_12m = f"R$ {r['valor_12m']:.4f} ({r['data_12m'].strftime('%d/%m/%Y')})"
            else:
                nivel_12m = "-"

            if r["valor_24m"] is not None and r["data_24m"] is not None:
                nivel_24m = f"R$ {r['valor_24m']:.4f} ({r['data_24m'].strftime('%d/%m/%Y')})"
            else:
                nivel_24m = "-"

            var_ano = f"{r['var_ano']:+.2f}%" if r["var_ano"] is not None else "-"
            var_12m = f"{r['var_12m']:+.2f}%" if r["var_12m"] is not None else "-"
            var_24m = f"{r['var_24m']:+.2f}%" if r["var_24m"] is not None else "-"
        else:
            ultima_data_str = "-"
            nivel_atual = "sem dados"
            nivel_12m = "-"
            nivel_24m = "-"
            var_ano = "-"
            var_12m = "-"
            var_24m = "-"

        linhas.append({
            "Indicador": "Dólar PTAX - venda",
            "Data": ultima_data_str,
            "Nível atual": nivel_atual,
            "Nível há 12m": nivel_12m,
            "Nível há 24m": nivel_24m,
            "Var. ano": var_ano,
            "Var. 12m": var_12m,
            "Var. 24m": var_24m,
            "Fonte": f"BCB / SGS ({SGS_SERIES['ptax_venda']})",
        })

    except Exception as e:
        linhas.append({
            "Indicador": "Dólar PTAX - venda",
            "Data": "-",
            "Nível atual": f"Erro: {e}",
            "Nível há 12m": "-",
            "Nível há 24m": "-",
            "Var. ano": "-",
            "Var. 12m": "-",
            "Var. 24m": "-",
            "Fonte": "BCB / SGS",
        })

    return pd.DataFrame(linhas)


def montar_tabela_atividade_economica() -> pd.DataFrame:
    linhas: List[Dict[str, str]] = []

    # Varejo
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

    # Serviços
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

    # Indústria
    try:
        r_pim = resumo_pim_oficial()
        if r_pim["referencia"] != "-":
            linhas.append({
                "Indicador": "Indústria (PIM-PF) – produção física",
                "Mês ref.": r_pim["referencia"],
                "Var. mensal": (
                    f"{r_pim['var_mensal']:.1f}%"
                    if pd.notna(r_pim["var_mensal"]) else "-"
                ),
                "Acum. no ano": (
                    f"{r_pim['acum_ano']:.1f}%"
                    if pd.notna(r_pim["acum_ano"]) else "-"
                ),
                "Acum. 12 meses": (
                    f"{r_pim['acum_12m']:.1f}%"
                    if pd.notna(r_pim["acum_12m"]) else "-"
                ),
                "Fonte": "IBGE / PIM-PF (SIDRA – Tabela 8888)",
            })
        else:
            linhas.append({
                "Indicador": "Indústria (PIM-PF) – produção física",
                "Mês ref.": "-",
                "Var. mensal": "sem dados",
                "Acum. no ano": "-",
                "Acum. 12 meses": "-",
                "Fonte": "IBGE / PIM-PF (SIDRA – Tabela 8888)",
            })
    except Exception as e:
        linhas.append({
            "Indicador": "Indústria (PIM-PF) – produção física",
            "Mês ref.": "-",
            "Var. mensal": f"Erro: {e}",
            "Acum. no ano": "-",
            "Acum. 12 meses": "-",
            "Fonte": "IBGE / PIM-PF (SIDRA – Tabela 8888)",
        })

    return pd.DataFrame(linhas)


def montar_tabela_focus():
    ano_atual = datetime.now().year
    anos = [ano_atual, ano_atual + 1]

    indicadores = [
        ("IPCA (a.a.)", "IPCA", "Índice cheio"),
        ("PIB Total (var.%)", "PIB Total", "Var. %"),
        ("Selic fim do ano (a.a.)", "Selic Meta", "Fim de período"),
        ("Câmbio fim do ano (R$/US$)", "Câmbio", "R$/US$ - fim de período"),
    ]

    linhas = []
    for nome_exibicao, indicador, detalhe in indicadores:
        linha = {"Indicador": nome_exibicao}
        for ano in anos:
            valor = buscar_focus_expectativa_anual(indicador, detalhe, ano)
            linha[str(ano)] = valor
        linha["Fonte"] = "BCB / Focus – Expectativas de Mercado Anuais"
        linhas.append(linha)

    return linhas



# =============================================================================
# STREAMLIT - INTERFACE
# =============================================================================

def main():
    st.set_page_config(
        page_title="Indicadores Macro Brasil",
        layout="wide",
    )

    st.title("Indicadores Macro Brasil")
    st.caption("Dados oficiais – IBGE (SIDRA), Banco Central (SGS) e Focus (BCB).")

    st.write("---")

    with st.spinner("Buscando dados mais recentes..."):
        df_infla = montar_tabela_inflacao()
        df_ativ = montar_tabela_atividade_economica()
        df_focus = montar_tabela_focus()
        df_selic = montar_tabela_selic_meta()
        df_cdi = montar_tabela_cdi()
        df_ptax = montar_tabela_ptax()

    # INFLAÇÃO
    st.subheader("📊 Inflação (IBGE)")
    st.dataframe(
        df_infla.set_index("Indicador"),
        width="stretch",
    )

    st.write("---")

    # EXPECTATIVAS DE MERCADO (FOCUS)
    st.subheader("📈 Expectativas de Mercado (Focus)")
    st.caption(
        "Mediana das expectativas anuais para IPCA, PIB, Selic e câmbio – "
        "ano corrente e próximo (BCB / Focus)."
    )
    st.dataframe(
        df_focus.set_index("Indicador"),
        width="stretch",
    )

    st.write("---")

    # ATIVIDADE ECONÔMICA
    st.subheader("🏭 Atividade Econômica (IBGE)")
    st.dataframe(
        df_ativ.set_index("Indicador"),
        width="stretch",
    )

    st.write("---")

    # JUROS E CÂMBIO
    st.subheader("💰 Juros e Câmbio (Banco Central)")

    st.markdown("**Taxa básica – Selic Meta**")
    st.dataframe(
        df_selic.set_index("Indicador"),
        width="stretch",
    )

    st.markdown("**CDI – níveis e acumulados**")
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
