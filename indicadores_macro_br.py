# indicadores_macro_br.py
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import unicodedata
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import streamlit as st
from typing import Optional, Dict, List
from functools import lru_cache
from curvas_anbima import (
    atualizar_todas_as_curvas,
    montar_curva_anbima_hoje,
    montar_curva_anbima_variacoes,
)
from di_futuro_b3 import (
    atualizar_historico_di_futuro,
    carregar_historico_di_futuro,
)


# =============================================================================
# HELPER DE REDE COM RETRY
# =============================================================================


def atualizar_dados_externos():
    """
    Atualiza os dados que ficam salvos em CSV fora do app principal:
    - Curvas ANBIMA (prefixada, DI, IPCA+)
    - Histórico dos contratos DI Futuro (B3)
    """
    # Atualiza curvas ANBIMA
    try:
        atualizar_todas_as_curvas()
    except Exception as e:
        st.warning(f"Não foi possível atualizar curvas ANBIMA: {e}")

    # Atualiza histórico DI Futuro B3
    try:
        atualizar_historico_di_futuro()
    except Exception as e:
        st.warning(f"Não foi possível atualizar histórico DI Futuro B3: {e}")


def _get_with_retry(
    url: str,
    max_attempts: int = 2,
    timeout: int = 10,
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
# CONFIGURAÇÕES DE SÉRIES
# =============================================================================

SGS_SERIES = {
    "selic_meta_aa": 432,
    "cdi_diario": 12,
    "ptax_venda": 10813,
}

IBGE_TABELA_IPCA = 1737
IBGE_VARIAVEL_IPCA = 63  # variação mensal (%)

IBGE_TABELA_IPCA15 = 3065
IBGE_VARIAVEL_IPCA15 = 355

IBGE_NIVEL_BRASIL = "n1/all"  # nível Brasil

# FOCUS – endpoint definitivo (ExpectativasMercadoAnuais)
FOCUS_BASE_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/"
    "Expectativas/versao/v1/odata/ExpectativasMercadoAnuais"
)

FOCUS_TOP5_ANUAIS_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/"
    "Expectativas/versao/v1/odata/ExpectativasMercadoTop5Anuais"
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

    resp = _get_with_retry(url)  # usa os defaults: 2 tentativas, 10s
    dados = resp.json()

    if not dados:
        return pd.DataFrame(columns=["data", "valor"])

    df = pd.DataFrame(dados)
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["valor"] = pd.to_numeric(
        df["valor"].astype(str).str.replace(",", "."),
        errors="coerce",
    )
    df = df.sort_values("data").reset_index(drop=True)
    return df


def buscar_serie_sgs(
    codigo: int,
    data_inicial: Optional[str] = None,
    data_final: Optional[str] = None,
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

    resp = _get_with_retry(url)  # usa os defaults: 2 tentativas, 10s
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

    col_valor = "V"  # coluna padrão SIDRA

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


def buscar_serie_mensal_ibge(
    tabela: int,
    variavel: int,
    nivel: str = IBGE_NIVEL_BRASIL,
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
    resp = _get_with_retry(url)  # usa os defaults: 2 tentativas, 10s
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

    df["data"] = df[col_periodo].apply(_parse_periodo)
    df["valor"] = pd.to_numeric(
        df["V"].astype(str).str.replace(",", "."),
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


def _buscar_serie_sidra_valor(url: str) -> pd.DataFrame:
    """Wrapper sem cache mutável (retorna cópia)."""
    return _buscar_serie_sidra_valor_cached(url).copy()


# =============================================================================
# ATIVIDADE ECONÔMICA – PMC / PMS / PIM
# =============================================================================


def buscar_pmc_var_mom_ajustada() -> pd.DataFrame:
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/8880/n1/all/v/11708/p/last60/c11046/56734/d/v11708%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pmc_var_acum_ano() -> pd.DataFrame:
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/8880/n1/all/v/11710/p/last60/c11046/56734/d/v11710%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pmc_var_acum_12m() -> pd.DataFrame:
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/8880/n1/all/v/11711/p/last60/c11046/56734/d/v11711%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pms_var_mom_ajustada() -> pd.DataFrame:
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/5906/n1/all/v/11623/p/last60/c11046/56726/d/v11623%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pms_var_acum_ano() -> pd.DataFrame:
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/5906/n1/all/v/11625/p/last60/c11046/56726/d/v11625%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pms_var_acum_12m() -> pd.DataFrame:
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/5906/n1/all/v/11626/p/last60/c11046/56726/d/v11626%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pim_var_mom_ajustada() -> pd.DataFrame:
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/8888/n1/all/v/11601/p/last60/c544/129314/d/v11601%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pim_var_acum_ano() -> pd.DataFrame:
    url = (
        "https://apisidra.ibge.gov.br/values/"
        "t/8888/n1/all/v/11603/p/last60/c544/129314/d/v11603%201"
    )
    return _buscar_serie_sidra_valor(url)


def buscar_pim_var_acum_12m() -> pd.DataFrame:
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
    df_12: pd.DataFrame,
) -> Dict[str, float]:
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
    df_mom = buscar_pmc_var_mom_ajustada()
    df_ano = buscar_pmc_var_acum_ano()
    df_12 = buscar_pmc_var_acum_12m()
    return _resumo_triple_series(df_mom, df_ano, df_12)


def resumo_pms_oficial() -> Dict[str, float]:
    df_mom = buscar_pms_var_mom_ajustada()
    df_ano = buscar_pms_var_acum_ano()
    df_12 = buscar_pms_var_acum_12m()
    return _resumo_triple_series(df_mom, df_ano, df_12)


def resumo_pim_oficial() -> Dict[str, float]:
    df_mom = buscar_pim_var_mom_ajustada()
    df_ano = buscar_pim_var_acum_ano()
    df_12 = buscar_pim_var_acum_12m()
    return _resumo_triple_series(df_mom, df_ano, df_12)


# =============================================================================
# INFLAÇÃO – CÁLCULOS
# =============================================================================


def _acumula_percentuais(valores: pd.Series) -> float:
    if valores.empty:
        return float("nan")
    fator = (1 + valores / 100).prod()
    return (fator - 1) * 100.0


def resumo_inflacao(df: pd.DataFrame) -> Dict[str, float]:
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
    ultimo_valor = ult["valor"]

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
        valor_12m = df_12m.iloc[0]["valor"]
        data_12m = df_12m.iloc[0]["data"]
        var_12m = (ultimo_valor / valor_12m - 1) * 100.0
    else:
        valor_12m = None
        data_12m = None
        var_12m = None

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
# FOCUS – EXPECTATIVAS DE MERCADO (ANUAIS)
# =============================================================================


def _normalizar_str(s: str) -> str:
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()


@lru_cache(maxsize=1)
def _carregar_focus_raw() -> pd.DataFrame:
    """
    Carrega o dataset de Expectativas de Mercado Anuais (estatísticas)
    e prepara um DataFrame com:

      - Indicador
      - IndicadorDetalhe
      - Data           (quando a expectativa foi registrada)
      - DataReferencia (ano de referência, ex: 2025)
      - ano_ref        (int)
      - Mediana        (float)
    """
    url = (
        f"{FOCUS_BASE_URL}"
        "?$top=5000"
        "&$orderby=Data%20desc"
        "&$format=json"
        "&$select=Indicador,IndicadorDetalhe,Data,DataReferencia,Mediana"
    )

    try:
        resp = _get_with_retry(url)  # usa os defaults: 2 tentativas, 10s
        dados = resp.json().get("value", [])
    except Exception:
        return pd.DataFrame()

    if not dados:
        return pd.DataFrame()

    df = pd.DataFrame(dados)

    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")

    df["ano_ref"] = df["DataReferencia"].astype(str).str[:4]
    df = df[df["ano_ref"].str.isdigit()].copy()
    df["ano_ref"] = df["ano_ref"].astype(int)

    df["indicador_norm"] = df["Indicador"].apply(_normalizar_str)
    if "IndicadorDetalhe" in df.columns:
        df["detalhe_norm"] = df["IndicadorDetalhe"].apply(_normalizar_str)
    else:
        df["detalhe_norm"] = ""

    return df


@lru_cache(maxsize=1)
def _carregar_focus_top5_raw() -> pd.DataFrame:
    url = (
        f"{FOCUS_TOP5_ANUAIS_URL}"
        "?$top=5000"
        "&$orderby=Data%20desc"
        "&$format=json"
        "&$select=Indicador,Data,DataReferencia,Mediana"
    )

    try:
        resp = _get_with_retry(url)  # usa os defaults: 2 tentativas, 10s
        dados = resp.json().get("value", [])
    except Exception:
        return pd.DataFrame()

    if not dados:
        return pd.DataFrame()

    df = pd.DataFrame(dados)

    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")

    df["ano_ref"] = df["DataReferencia"].astype(str).str[:4]
    df = df[df["ano_ref"].str.isdigit()].copy()
    df["ano_ref"] = df["ano_ref"].astype(int)

    df["indicador_norm"] = df["Indicador"].apply(_normalizar_str)
    # esse endpoint não tem IndicadorDetalhe, então deixamos vazio
    df["detalhe_norm"] = ""

    return df


def buscar_focus_expectativa_anual(
    indicador_substr: str,
    ano_desejado: int,
    detalhe_substr: Optional[str] = None,
):
    """
    Busca a mediana mais recente do Focus para um dado indicador e ano.
    Ex.:
      indicador_substr = "ipca"
      indicador_substr = "pib total"
      indicador_substr = "selic"
      indicador_substr = "cambio"
    """
    df = _carregar_focus_raw().copy()
    if df.empty:
        return "-"

    mask = df["ano_ref"] == ano_desejado

    ind_norm = _normalizar_str(indicador_substr)
    mask &= df["indicador_norm"].str.contains(ind_norm, na=False)

    if detalhe_substr:
        det_norm = _normalizar_str(detalhe_substr)
        mask &= df["detalhe_norm"].str.contains(det_norm, na=False)

    df_f = df[mask]
    if df_f.empty:
        return "-"

    df_f = df_f.sort_values("Data", ascending=False)
    med = df_f.iloc[0].get("Mediana", None)

    try:
        return float(med)
    except Exception:
        return "-"


def buscar_focus_top5_expectativa_anual(
    indicador_substr: str,
    ano_desejado: int,
    detalhe_substr: Optional[str] = None,
):
    df = _carregar_focus_top5_raw().copy()
    if df.empty:
        return "-"

    mask = df["ano_ref"] == ano_desejado

    ind_norm = _normalizar_str(indicador_substr)
    mask &= df["indicador_norm"].str.contains(ind_norm, na=False)

    if detalhe_substr:
        det_norm = _normalizar_str(detalhe_substr)
        mask &= df["detalhe_norm"].str.contains(det_norm, na=False)

    df_f = df[mask]
    if df_f.empty:
        return "-"

    df_f = df_f.sort_values("Data", ascending=False)
    med = df_f.iloc[0].get("Mediana", None)

    try:
        return float(med)
    except Exception:
        return "-"


def montar_tabela_focus() -> pd.DataFrame:
    """
    Monta a tabela de expectativas anuais para IPCA, PIB, Selic e câmbio
    usando o recurso ExpectativasMercadoAnuais.
    """
    ano_atual = datetime.now().year
    anos = [ano_atual, ano_atual + 1]

    configs = [
        ("IPCA (a.a.)", "ipca", None, True),
        ("PIB Total (var.% a.a.)", "pib total", None, True),
        ("Selic (a.a.)", "selic", None, False),
        ("Câmbio (R$/US$)", "cambio", None, False),
    ]

    linhas: List[Dict[str, str]] = []

    for nome_exibicao, indicador_sub, detalhe_sub, eh_percentual in configs:
        linha: Dict[str, str] = {"Indicador": nome_exibicao}

        for ano in anos:
            valor = buscar_focus_expectativa_anual(indicador_sub, ano, detalhe_sub)

            if isinstance(valor, (int, float)):
                if eh_percentual:
                    linha[str(ano)] = f"{valor:.2f}%"
                else:
                    linha[str(ano)] = f"{valor:.2f}"
            else:
                linha[str(ano)] = valor

        linha["Fonte"] = "BCB / Focus – Expectativas de Mercado Anuais"
        linhas.append(linha)

    return pd.DataFrame(linhas)


def montar_tabela_focus_top5() -> pd.DataFrame:
    """
    Monta a tabela de expectativas anuais para IPCA, PIB, Selic e câmbio
    usando o recurso ExpectativasMercadoAnuaisTop5 (Focus Top5).

    Top5 = mediana das 5 instituições que mais acertam as projeções.
    """
    ano_atual = datetime.now().year
    anos = [ano_atual, ano_atual + 1]

    # mesmo conjunto de indicadores, mas com rótulo deixando claro que é Top5
    configs = [
        ("IPCA (a.a., Top5)", "ipca", None, True),
        ("PIB Total (var.% a.a., Top5)", "pib total", None, True),
        ("Selic (a.a., Top5)", "selic", None, False),
        ("Câmbio (R$/US$, Top5)", "cambio", None, False),
    ]

    linhas: List[Dict[str, str]] = []

    for nome_exibicao, indicador_sub, detalhe_sub, eh_percentual in configs:
        linha: Dict[str, str] = {"Indicador": nome_exibicao}

        for ano in anos:
            valor = buscar_focus_top5_expectativa_anual(
                indicador_sub, ano, detalhe_sub
            )

            if isinstance(valor, (int, float)):
                if eh_percentual:
                    linha[str(ano)] = f"{valor:.2f}%"
                else:
                    linha[str(ano)] = f"{valor:.2f}"
            else:
                linha[str(ano)] = valor

        linha["Fonte"] = "BCB / Focus – Anuais Top5 (estatísticas)"
        linhas.append(linha)

    return pd.DataFrame(linhas)


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
            linhas.append(
                {
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
                }
            )
        else:
            linhas.append(
                {
                    "Indicador": "IPCA (variação mensal)",
                    "Mês ref.": "-",
                    "Valor (mensal)": "sem dados",
                    "Acum. no ano": "-",
                    "Acum. 12 meses": "-",
                    "Fonte": "IBGE / SIDRA (Tabela 1737)",
                }
            )
    except Exception as e:
        linhas.append(
            {
                "Indicador": "IPCA (variação mensal)",
                "Mês ref.": "-",
                "Valor (mensal)": f"Erro: {e}",
                "Acum. no ano": "-",
                "Acum. 12 meses": "-",
                "Fonte": "IBGE / SIDRA",
            }
        )

    # IPCA-15
    try:
        df_ipca15 = buscar_ipca15_ibge()
        if not df_ipca15.empty:
            r = resumo_inflacao(df_ipca15)
            linhas.append(
                {
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
                }
            )
        else:
            linhas.append(
                {
                    "Indicador": "IPCA-15 (variação mensal)",
                    "Mês ref.": "-",
                    "Valor (mensal)": "sem dados",
                    "Acum. no ano": "-",
                    "Acum. 12 meses": "-",
                    "Fonte": "IBGE / SIDRA (Tabela 3065)",
                }
            )
    except Exception as e:
        linhas.append(
            {
                "Indicador": "IPCA-15 (variação mensal)",
                "Mês ref.": "-",
                "Valor (mensal)": f"Erro: {e}",
                "Acum. no ano": "-",
                "Acum. 12 meses": "-",
                "Fonte": "IBGE / SIDRA",
            }
        )

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

            linhas.append(
                {
                    "Indicador": "Selic Meta",
                    "Data": ultima_data.strftime("%d/%m/%Y"),
                    "Nível atual": f"{ultimo:.2f}% a.a.",
                    "Início do ano": (
                        f"{inicio_ano_val:.2f}% a.a."
                        if inicio_ano_val is not None
                        else "-"
                    ),
                    "Há 12 meses": (
                        f"{nivel_12m_val:.2f}% a.a."
                        if nivel_12m_val is not None
                        else "-"
                    ),
                    "Fonte": f"BCB / SGS ({SGS_SERIES['selic_meta_aa']})",
                }
            )
        else:
            linhas.append(
                {
                    "Indicador": "Selic Meta",
                    "Data": "-",
                    "Nível atual": "sem dados",
                    "Início do ano": "-",
                    "Há 12 meses": "-",
                    "Fonte": "BCB / SGS",
                }
            )
    except Exception as e:
        linhas.append(
            {
                "Indicador": "Selic Meta",
                "Data": "-",
                "Nível atual": f"Erro: {e}",
                "Início do ano": "-",
                "Há 12 meses": "-",
                "Fonte": "BCB / SGS",
            }
        )

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
            (df["data"].dt.year == ano_ref) & (df["data"].dt.month == mes_ref)
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

        linhas.append(
            {
                "Indicador": "CDI (over) diário",
                "Data ref.": data_ult.strftime("%d/%m/%Y"),
                "Nível diário": f"{taxa_ult:.4f}% a.d.",
                "CDI no mês": f"{cdi_mes:.2f}%" if pd.notna(cdi_mes) else "-",
                "CDI no ano": f"{cdi_ano:.2f}%" if pd.notna(cdi_ano) else "-",
                "CDI em 12 meses": f"{cdi_12m:.2f}%" if pd.notna(cdi_12m) else "-",
                "Fonte": f"BCB / SGS ({SGS_SERIES['cdi_diario']})",
            }
        )

    except Exception as e:
        linhas.append(
            {
                "Indicador": "CDI (over) diário",
                "Data ref.": "-",
                "Nível diário": f"Erro: {e}",
                "CDI no mês": "-",
                "CDI no ano": "-",
                "CDI em 12 meses": "-",
                "Fonte": "BCB / SGS",
            }
        )

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
                nivel_12m = (
                    f"R$ {r['valor_12m']:.4f} "
                    f"({r['data_12m'].strftime('%d/%m/%Y')})"
                )
            else:
                nivel_12m = "-"

            if r["valor_24m"] is not None and r["data_24m"] is not None:
                nivel_24m = (
                    f"R$ {r['valor_24m']:.4f} "
                    f"({r['data_24m'].strftime('%d/%m/%Y')})"
                )
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

        linhas.append(
            {
                "Indicador": "Dólar PTAX - venda",
                "Data": ultima_data_str,
                "Nível atual": nivel_atual,
                "Nível há 12m": nivel_12m,
                "Nível há 24m": nivel_24m,
                "Var. ano": var_ano,
                "Var. 12m": var_12m,
                "Var. 24m": var_24m,
                "Fonte": f"BCB / SGS ({SGS_SERIES['ptax_venda']})",
            }
        )

    except Exception as e:
        linhas.append(
            {
                "Indicador": "Dólar PTAX - venda",
                "Data": "-",
                "Nível atual": f"Erro: {e}",
                "Nível há 12m": "-",
                "Nível há 24m": "-",
                "Var. ano": "-",
                "Var. 12m": "-",
                "Var. 24m": "-",
                "Fonte": "BCB / SGS",
            }
        )

    return pd.DataFrame(linhas)


def montar_tabela_di_futuro() -> pd.DataFrame:
    """
    Curva de juros – DI Futuro (contrato DI1 na B3).

    Usa a API pública leve da B3:
        https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation/DI1

    Retorna uma tabela com os principais vencimentos e as taxas
    (sem a coluna de contratos em aberto).
    """
    linhas: List[Dict[str, str]] = []

    try:
        url = "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation/DI1"
        resp = _get_with_retry(url, timeout=30)
        data = resp.json()

        scty_list = data.get("Scty", [])

        if not scty_list:
            raise ValueError("Resposta da B3 sem lista 'Scty'.")

        # -------------------------------------------------------------
        # Montagem das linhas com os principais campos da B3
        # -------------------------------------------------------------
        for item in scty_list:
            # Exemplo de símbolo: DI1Z25, DI1F26 etc.
            symb = (item.get("symb") or "").strip()
            if not symb.startswith("DI1"):
                continue

            asset = item.get("asset") or {}
            asst_summary = asset.get("AsstSummry") or {}
            scty_qtn = item.get("SctyQtn") or {}

            # Vencimento
            mtrty_str = asst_summary.get("mtrtyCode")
            try:
                if mtrty_str:
                    mtrty_dt = datetime.strptime(mtrty_str, "%Y-%m-%d").date()
                    vencimento_fmt = mtrty_dt.strftime("%d/%m/%Y")
                else:
                    vencimento_fmt = "-"
            except Exception:
                vencimento_fmt = "-"

            # Taxas e variação
            taxa_atual = scty_qtn.get("curPrc")
            taxa_ant = scty_qtn.get("prvsDayAdjstmntPric")
            variacao_bps = scty_qtn.get("prcFlcn")

            # Se a B3 não enviar a variação, tenta calcular manualmente
            if variacao_bps is None and taxa_atual is not None and taxa_ant is not None:
                try:
                    variacao_bps = (float(taxa_atual) - float(taxa_ant)) * 100.0
                except Exception:
                    variacao_bps = None

            def fmt_taxa(x) -> str:
                if x is None:
                    return "-"
                try:
                    return f"{float(x):.4f}%"
                except Exception:
                    return "-"

            def fmt_bps(x) -> str:
                if x is None:
                    return "-"
                try:
                    return f"{float(x):+.1f}"
                except Exception:
                    return "-"

            linhas.append(
                {
                    "Contrato": symb,
                    "Vencimento": vencimento_fmt,
                    "Taxa (%)": fmt_taxa(taxa_atual),
                    "Taxa dia ant. (%)": fmt_taxa(taxa_ant),
                    "Variação (bps)": fmt_bps(variacao_bps),
                }
            )

        if not linhas:
            raise ValueError("Nenhum contrato DI1 encontrado na resposta da B3.")

        df = pd.DataFrame(linhas)

        # -------------------------------------------------------------
        # Ordena por vencimento (convertendo a string de volta para data)
        # -------------------------------------------------------------
        def parse_venc(x: str):
            try:
                return datetime.strptime(x, "%d/%m/%Y").date()
            except Exception:
                # empurra valores inválidos para o fim
                return datetime.max.date()

        df = df.sort_values(by="Vencimento", key=lambda s: s.apply(parse_venc)).reset_index(
            drop=True
        )

        return df

    except Exception as e:
        # Fallback amigável se der erro na API da B3
        print(f"Erro ao montar curva DI Futuro (B3): {e}")
        linhas.append(
            {
                "Contrato": "DI1 – curva",
                "Vencimento": "-",
                "Taxa (%)": "-",
                "Taxa dia ant. (%)": "-",
                "Variação (bps)": "-",
            }
        )
        return pd.DataFrame(linhas)


def montar_tabela_atividade_economica() -> pd.DataFrame:
    linhas: List[Dict[str, str]] = []

    # Varejo (PMC) – COINCIDENTE
    try:
        r_pmc = resumo_pmc_oficial()
        if r_pmc["referencia"] != "-":
            linhas.append(
                {
                    "Indicador": "Varejo (PMC) – volume",
                    "Classificação": "🟡 Coincidente",
                    "Mês ref.": r_pmc["referencia"],
                    "Var. mensal": (
                        f"{r_pmc['var_mensal']:.1f}%"
                        if pd.notna(r_pmc["var_mensal"])
                        else "-"
                    ),
                    "Acum. no ano": (
                        f"{r_pmc['acum_ano']:.1f}%"
                        if pd.notna(r_pmc["acum_ano"])
                        else "-"
                    ),
                    "Acum. 12 meses": (
                        f"{r_pmc['acum_12m']:.1f}%"
                        if pd.notna(r_pmc["acum_12m"])
                        else "-"
                    ),
                    "Fonte": "IBGE / PMC (SIDRA – Tabela 8880)",
                }
            )
        else:
            linhas.append(
                {
                    "Indicador": "Varejo (PMC) – volume",
                    "Classificação": "🟡 Coincidente",
                    "Mês ref.": "-",
                    "Var. mensal": "sem dados",
                    "Acum. no ano": "-",
                    "Acum. 12 meses": "-",
                    "Fonte": "IBGE / PMC (SIDRA – Tabela 8880)",
                }
            )
    except Exception as e:
        linhas.append(
            {
                "Indicador": "Varejo (PMC) – volume",
                "Classificação": "🟡 Coincidente",
                "Mês ref.": "-",
                "Var. mensal": f"Erro: {e}",
                "Acum. no ano": "-",
                "Acum. 12 meses": "-",
                "Fonte": "IBGE / PMC (SIDRA – Tabela 8880)",
            }
        )

    # Serviços (PMS) – COINCIDENTE
    try:
        r_pms = resumo_pms_oficial()
        if r_pms["referencia"] != "-":
            linhas.append(
                {
                    "Indicador": "Serviços (PMS) – volume",
                    "Classificação": "🟡 Coincidente",
                    "Mês ref.": r_pms["referencia"],
                    "Var. mensal": (
                        f"{r_pms['var_mensal']:.1f}%"
                        if pd.notna(r_pms["var_mensal"])
                        else "-"
                    ),
                    "Acum. no ano": (
                        f"{r_pms['acum_ano']:.1f}%"
                        if pd.notna(r_pms["acum_ano"])
                        else "-"
                    ),
                    "Acum. 12 meses": (
                        f"{r_pms['acum_12m']:.1f}%"
                        if pd.notna(r_pms["acum_12m"])
                        else "-"
                    ),
                    "Fonte": "IBGE / PMS (SIDRA – Tabela 5906)",
                }
            )
        else:
            linhas.append(
                {
                    "Indicador": "Serviços (PMS) – volume",
                    "Classificação": "🟡 Coincidente",
                    "Mês ref.": "-",
                    "Var. mensal": "sem dados",
                    "Acum. no ano": "-",
                    "Acum. 12 meses": "-",
                    "Fonte": "IBGE / PMS (SIDRA – Tabela 5906)",
                }
            )
    except Exception as e:
        linhas.append(
            {
                "Indicador": "Serviços (PMS) – volume",
                "Classificação": "🟡 Coincidente",
                "Mês ref.": "-",
                "Var. mensal": f"Erro: {e}",
                "Acum. no ano": "-",
                "Acum. 12 meses": "-",
                "Fonte": "IBGE / PMS (SIDRA – Tabela 5906)",
            }
        )

    # Indústria (PIM-PF) – COINCIDENTE
    try:
        r_pim = resumo_pim_oficial()
        if r_pim["referencia"] != "-":
            linhas.append(
                {
                    "Indicador": "Indústria (PIM-PF) – produção física",
                    "Classificação": "🟡 Coincidente",
                    "Mês ref.": r_pim["referencia"],
                    "Var. mensal": (
                        f"{r_pim['var_mensal']:.1f}%"
                        if pd.notna(r_pim["var_mensal"])
                        else "-"
                    ),
                    "Acum. no ano": (
                        f"{r_pim['acum_ano']:.1f}%"
                        if pd.notna(r_pim["acum_ano"])
                        else "-"
                    ),
                    "Acum. 12 meses": (
                        f"{r_pim['acum_12m']:.1f}%"
                        if pd.notna(r_pim["acum_12m"])
                        else "-"
                    ),
                    "Fonte": "IBGE / PIM-PF (SIDRA – Tabela 8888)",
                }
            )
        else:
            linhas.append(
                {
                    "Indicador": "Indústria (PIM-PF) – produção física",
                    "Classificação": "🟡 Coincidente",
                    "Mês ref.": "-",
                    "Var. mensal": "sem dados",
                    "Acum. no ano": "-",
                    "Acum. 12 meses": "-",
                    "Fonte": "IBGE / PIM-PF (SIDRA – Tabela 8888)",
                }
            )
    except Exception as e:
        linhas.append(
            {
                "Indicador": "Indústria (PIM-PF) – produção física",
                "Classificação": "🟡 Coincidente",
                "Mês ref.": "-",
                "Var. mensal": f"Erro: {e}",
                "Acum. no ano": "-",
                "Acum. 12 meses": "-",
                "Fonte": "IBGE / PIM-PF (SIDRA – Tabela 8888)",
            }
        )

    return pd.DataFrame(linhas)


def render_bloco1_observatorio_mercado(
    df_focus,
    df_focus_top5,
    df_selic,
    df_cdi,
    df_ptax,
    df_di_fut,
    df_hist_di,
):
    """
    Estrutura:
    - Aba "Brasil"
        - Sub-aba "Curto prazo":
            - Selic Meta, CDI acumulado e câmbio PTAX
            - Curva de juros – DI Futuro (B3)
            - Histórico DI Futuro (B3)
            - Curva de juros – ANBIMA (prefixado x IPCA+)
        - Sub-aba "Expectativas":
            - Focus – Mediana (consenso do mercado)
            - Focus – Top 5 (instituições mais assertivas)
    - Aba "Mundo"
        - Indicadores globais de curto prazo — em construção
        - Expectativas de mercado – Global — em construção
    """

    tab_br, tab_mundo = st.tabs(["Brasil", "Mundo"])

    # ==========================
    # ABA BRASIL
    # ==========================
    with tab_br:
        subtab_indic_br, subtab_exp_br = st.tabs(["Curto prazo", "Expectativas"])

        # -------- Indicadores BR --------
        with subtab_indic_br:
            st.markdown("### Indicadores de curto prazo – Brasil")
            st.caption(
                "Selic Meta, CDI acumulado, câmbio PTAX e curvas de juros "
                "(DI Futuro na B3 e ANBIMA prefixado x IPCA+), "
                "com foco em leitura de curto e médio prazo."
            )

            # Selic
            st.markdown("**Taxa básica – Selic Meta**")
            st.dataframe(
                df_selic.set_index("Indicador"),
                width="stretch",
            )

            # CDI
            st.markdown("**CDI – Retorno acumulado**")
            st.dataframe(
                df_cdi.set_index("Indicador"),
                width="stretch",
            )

            # Câmbio
            st.markdown("**Câmbio – Dólar PTAX (venda)**")
            st.dataframe(
                df_ptax.set_index("Indicador"),
                width="stretch",
            )

            # Curva DI Futuro (snapshot de hoje)
            st.markdown("**Curva de juros – DI Futuro (B3)**")
            st.caption(
                "Principais vencimentos do contrato DI1, com taxa implícita anualizada "
                "e variação em basis points."
            )
            st.dataframe(
                df_di_fut.set_index("Contrato"),
                width="stretch",
            )

            # -------------------------------
            # Histórico – DI Futuro (B3) – opcional, em expander
            # -------------------------------
            st.markdown("**Histórico – DI Futuro (B3)**")
            with st.expander(
                "Ver evolução histórica da taxa por contrato (opcional)"
            ):
                if df_hist_di is None or df_hist_di.empty:
                    st.info(
                        "Ainda não há histórico salvo de DI Futuro. "
                        "Certifique-se de rodar o app em dias úteis para ir "
                        "acumulando as observações no arquivo "
                        "`data/di_futuro/di1_historico.csv`."
                    )
                    df_sel = None
                else:
                    # garante ordenação por data
                    df_hist = df_hist_di.copy()
                    df_hist["data"] = pd.to_datetime(df_hist["data"])
                    df_hist = df_hist.sort_values("data")

                    # lista de tickers disponíveis
                    tickers = sorted(df_hist["ticker"].unique())

                    # tenta deixar DI1Z25 como padrão, se existir
                    idx_default = (
                        tickers.index("DI1Z25") if "DI1Z25" in tickers else 0
                    )

                    contrato_sel = st.selectbox(
                        "Selecione o contrato DI1 para análise histórica",
                        options=tickers,
                        index=idx_default,
                    )

                    df_sel = df_hist[df_hist["ticker"] == contrato_sel].copy()
                    df_sel = df_sel.set_index("data")

                    # gráfico da taxa
                    st.line_chart(
                        df_sel[["taxa"]],
                        width="stretch",
                    )

                    st.caption(
                        "Taxa implícita do contrato selecionado (% a.a.), "
                        "com base no histórico salvo em CSV."
                    )

                    # -----------------------
                    # Últimas observações
                    # -----------------------
                    st.markdown("Últimas observações:")

                    colunas_base = ["taxa", "variacao_bps", "volume"]
                    colunas_existentes = [
                        c for c in colunas_base if c in df_sel.columns
                    ]

                    if colunas_existentes:
                        mapa_renome = {
                            "taxa": "Taxa (%)",
                            "variacao_bps": "Var. (bps)",
                            "volume": "Volume",
                        }

                        df_ultimas = (
                            df_sel[colunas_existentes]
                            .tail(10)
                            .rename(
                                columns={
                                    c: mapa_renome[c]
                                    for c in colunas_existentes
                                }
                            )
                        )

                        st.dataframe(
                            df_ultimas,
                            width="stretch",
                        )
                    else:
                        st.info(
                            "Ainda não há colunas suficientes no histórico para montar a "
                            "tabela de últimas observações (ex.: 'variacao_bps' ou "
                            "'volume')."
                        )

            # -------------------------------
            # Curva de juros – ANBIMA
            # -------------------------------
            st.markdown("### Curva de juros – ANBIMA (Prefixado x IPCA+)")
            st.caption(
                "Juro nominal, juro real e breakeven para vértices selecionados "
                "com base nas curvas da ANBIMA (DI, prefixada e IPCA+), usando "
                "histórico salvo localmente em CSV."
            )

            df_curva_hoje = montar_curva_anbima_hoje()

            if df_curva_hoje.empty:
                st.info(
                    "Ainda não há dados locais das curvas ANBIMA para hoje. "
                    "Certifique-se de rodar o app em dia útil, após a "
                    "divulgação das curvas pela ANBIMA, para começar a "
                    "popular o histórico (arquivos em data/curvas_anbima)."
                )
            else:
                st.markdown(
                    "**Níveis atuais por vértice (nominal, real e breakeven)**"
                )
                st.dataframe(
                    df_curva_hoje.set_index("Vértice (anos)"),
                    width="stretch",
                )

                st.markdown(
                    "**Abertura/fechamento por vértice – visão resumida**"
                )
                vertice = st.selectbox(
                    "Selecione o vértice para análise de abertura/fechamento",
                    options=[2, 5, 10, 20],
                    index=2,
                    format_func=lambda x: f"{x} anos",
                )

                df_var = montar_curva_anbima_variacoes(anos=vertice)

                if df_var.empty:
                    st.info(
                        "Ainda não há histórico suficiente para esse vértice. "
                        "Conforme o tempo passar, o painel vai acumulando "
                        "observações diárias das curvas ANBIMA."
                    )
                else:
                    st.dataframe(
                        df_var.set_index("Data"),
                        width="stretch",
                    )
                    st.caption(
                        "Os níveis estão em % ao ano. A diferença entre as datas "
                        "indica se a curva abriu ou fechou em cada horizonte "
                        "(D-1, 1 semana, 1 mês, início do ano, 12 meses)."
                    )

        # -------- Expectativas BR --------
        with subtab_exp_br:
            st.markdown("### Expectativas de mercado – Brasil (Focus)")
            st.caption(
                "Projeções anuais do Focus, com comparação entre o consenso (Mediana) "
                "e o grupo das instituições mais assertivas (Top 5)."
            )

            st.markdown("**Focus – Mediana (consenso do mercado)**")
            st.caption(
                "Mediana das projeções de todas as instituições participantes do boletim Focus."
            )
            st.dataframe(
                df_focus.set_index("Indicador"),
                width="stretch",
            )

            st.markdown("**Focus – Top 5 (instituições mais assertivas)**")
            st.caption(
                "Mediana das projeções das 5 instituições com melhor desempenho histórico no Focus."
            )
            st.dataframe(
                df_focus_top5.set_index("Indicador"),
                width="stretch",
            )

    # ==========================
    # ABA MUNDO
    # ==========================
    with tab_mundo:
        subtab_indic_world, subtab_exp_world = st.tabs(
            ["Curto prazo", "Expectativas"]
        )

        # -------- Indicadores MUNDO --------
        with subtab_indic_world:
            st.markdown("### Indicadores de curto prazo – Global")
            st.caption(
                "Em construção: bolsas (EUA, Europa, Ásia), VIX, DXY, Treasuries, "
                "commodities e CDS Brasil."
            )
            st.info(
                "Aqui vamos adicionar: S&P, Nasdaq, Stoxx 600, índices asiáticos, "
                "VIX, DXY, Treasuries 2y/5y/10y/30y, petróleo, minério, ouro e CDS Brasil."
            )

        # -------- Expectativas MUNDO --------
        with subtab_exp_world:
            st.markdown("### Expectativas de mercado – Global")
            st.caption(
                "Em construção: projeções de crescimento, inflação e juros em economias "
                "avançadas e emergentes."
            )
            st.info(
                "Aqui futuramente entram projeções do FMI/OCDE, Fed funds implícito, "
                "inflação esperada nos EUA/Europa etc."
            )


def render_bloco2_fiscal():
    st.info(
        "Em construção: resultado primário (12m), resultado nominal, juros nominais, "
        "DBGG (% do PIB), DLSP (% do PIB) e NFSP (Tesouro / BCB)."
    )


def render_bloco3_setor_externo():
    st.info(
        "Em construção: exportações, importações, balança comercial, transações correntes, "
        "conta financeira, renda primária/secundária e reservas internacionais."
    )


def render_bloco4_mercado_trabalho():
    st.info(
        "Em construção: PNAD Contínua (desemprego, ocupados, renda), CAGED e desemprego nos EUA."
    )


def render_bloco5_atividade(df_ativ: pd.DataFrame):
    st.markdown("### Atividade econômica – IBGE")
    st.caption(
        "Indicadores de volume de Varejo (PMC), Serviços (PMS) e Indústria (PIM-PF), "
        "classificados como indicadores coincidentes do ciclo econômico."
    )

    # Filtro de classificação cíclica
    filtro = st.selectbox(
        "Classificação cíclica dos indicadores",
        ["Todos", "Coincidentes"],
        index=1,
    )

    df_exibir = df_ativ.copy()

    if filtro == "Coincidentes":
        df_exibir = df_exibir[
            df_exibir["Classificação"].str.contains("Coincidente")
        ]

    st.dataframe(
        df_exibir.set_index(["Indicador", "Classificação"]),
        width="stretch",
    )

    st.info(
        "⚙️ Em construção (parte avançada): inclusão de indicadores antecedentes "
        "(PMI, confiança FGV) e defasados (desemprego, massa salarial), "
        "todos com a mesma lógica de classificação cíclica."
    )


def render_bloco6_inflacao(df_infla: pd.DataFrame):
    st.markdown("### IPCA e IPCA-15 – visão consolidada")
    st.caption("Inflação cheia e IPCA-15: mensal, acumulado no ano e em 12 meses.")
    st.dataframe(
        df_infla.set_index("Indicador"),
        width="stretch",
    )

    st.info(
        "⚙️ Em construção: núcleos, difusão, IGPs, INCC e inflação internacional."
    )


def render_bloco7_credito_condicoes():
    st.info(
        "Em construção: inadimplência PF/PJ, concessões, spreads, estoque total, "
        "crédito/PIB e índice de condições financeiras."
    )


# =============================================================================
# WRAPPERS CACHEADOS (Streamlit) PARA AS TABELAS
# =============================================================================


@st.cache_data(ttl=60 * 30)  # 30 minutos
def get_tabela_inflacao():
    return montar_tabela_inflacao()


@st.cache_data(ttl=60 * 30)
def get_tabela_atividade():
    return montar_tabela_atividade_economica()


@st.cache_data(ttl=60 * 30)
def get_tabela_focus():
    return montar_tabela_focus()


@st.cache_data(ttl=60 * 30)
def get_tabela_focus_top5():
    return montar_tabela_focus_top5()


@st.cache_data(ttl=60 * 30)
def get_tabela_selic():
    return montar_tabela_selic_meta()


@st.cache_data(ttl=60 * 30)
def get_tabela_cdi():
    return montar_tabela_cdi()


@st.cache_data(ttl=60 * 30)
def get_tabela_ptax():
    return montar_tabela_ptax()


@st.cache_data(ttl=60 * 10)
def get_tabela_di_futuro():
    return montar_tabela_di_futuro()


@st.cache_data(ttl=60 * 10)
def get_historico_di_futuro():
    """
    Lê o CSV de histórico de DI Futuro (data/di_futuro/di1_historico.csv).
    Se ainda não existir, retorna DataFrame vazio.
    """
    try:
        df = carregar_historico_di_futuro()
        return df
    except FileNotFoundError:
        return pd.DataFrame()


# =============================================================================
# STREAMLIT - INTERFACE
# =============================================================================


def atualizar_dados_externos():
    """
    Atualiza os dados que ficam salvos em CSV fora do app principal:
    - Curvas ANBIMA (prefixada, DI, IPCA+)
    - Histórico dos contratos DI Futuro (B3)
    """
    # Atualiza curvas ANBIMA
    try:
        atualizar_todas_as_curvas()
    except Exception as e:
        st.warning(f"Não foi possível atualizar curvas ANBIMA: {e}")

    # Atualiza histórico DI Futuro B3
    try:
        atualizar_historico_di_futuro()
    except Exception as e:
        st.warning(f"Não foi possível atualizar histórico DI Futuro B3: {e}")


def main():
    st.set_page_config(
        page_title="Observatório Macro",
        layout="wide",
    )

    # 🔄 Atualiza ANBIMA + DI Futuro B3 logo que o app inicia
    with st.spinner("Atualizando curvas ANBIMA e histórico de DI Futuro B3..."):
        atualizar_dados_externos()

    st.title("Observatório Macro")
    st.caption(
        "Painel de conjuntura e inteligência macroeconômica – dados oficiais do IBGE, "
        "BCB e fontes internacionais."
    )

    st.write("---")

    with st.spinner("Buscando dados mais recentes..."):
        df_infla = get_tabela_inflacao()
        df_ativ = get_tabela_atividade()
        df_focus = get_tabela_focus()
        df_focus_top5 = get_tabela_focus_top5()
        df_selic = get_tabela_selic()
        df_cdi = get_tabela_cdi()
        df_ptax = get_tabela_ptax()
        df_di_fut = get_tabela_di_futuro()
        df_hist_di = get_historico_di_futuro()

    # ==========
    # LAYOUT PRINCIPAL COM TABS
    # ==========
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
        [
            "📊 Termômetros de Mercado",
            "🏛 Fiscal",
            "🌍 Setor Externo",
            "👷 Mercado de Trabalho",
            "🏭 Atividade Real",
            "📈 Inflação",
            "💳 Crédito & Condições",
        ]
    )

    with tab1:
        render_bloco1_observatorio_mercado(
            df_focus=df_focus,
            df_focus_top5=df_focus_top5,
            df_selic=df_selic,
            df_cdi=df_cdi,
            df_ptax=df_ptax,
            df_di_fut=df_di_fut,
            df_hist_di=df_hist_di,
        )

    with tab2:
        render_bloco2_fiscal()

    with tab3:
        render_bloco3_setor_externo()

    with tab4:
        render_bloco4_mercado_trabalho()

    with tab5:
        render_bloco5_atividade(df_ativ=df_ativ)

    with tab6:
        render_bloco6_inflacao(df_infla=df_infla)

    with tab7:
        render_bloco7_credito_condicoes()

    st.write("---")
    st.caption(
        "Atualize os dados recarregando a página ou rodando novamente "
        "`streamlit run indicadores_macro_br.py`."
    )


if __name__ == "__main__":
    main()
