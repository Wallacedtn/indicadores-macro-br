# indicadores_macro_br.py
# -*- coding: utf-8 -*-

import streamlit_shadcn_ui as ui
import altair as alt
import requests
import pandas as pd
import unicodedata
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import streamlit as st
from typing import Optional, Dict, List, Tuple
from functools import lru_cache
from pathlib import Path
from curvas_anbima import (
    atualizar_todas_as_curvas,
    montar_curva_anbima_hoje,
    montar_curva_anbima_variacoes,
)
from di_futuro_b3 import (
    atualizar_historico_di_futuro,
    carregar_historico_di_futuro,
)

from ibovespa_ipea import (
    atualizar_historico_ibovespa,
    carregar_historico_ibovespa,
)

from bloco_curto_prazo_br import render_bloco_curto_prazo_br
from analise_tesouro_vs_curva import (
    comparar_tesouro_pre_vs_curva,
    comparar_tesouro_ipca_vs_curva,
)
from tesouro_direto import carregar_tesouro_ultimo_dia
import logging


# =============================================================================
# TEMA GLOBAL / CSS EXTERNO (theme_ion.css)
# =============================================================================


def load_theme_css() -> None:
    """
    Carrega o arquivo css/theme_ion.css (tema estilo Íon) e injeta no app.

    IMPORTANTE:
    - Não usamos mais session_state aqui.
      O Streamlit reconstrói o DOM a cada rerun, então precisamos
      injetar o <style> em TODA execução do script.
    """
    css_path = Path(__file__).resolve().parent / "css" / "theme_ion.css"
    try:
        css = css_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        st.warning(
            "Arquivo de tema CSS não encontrado em 'css/theme_ion.css'. "
            "Verifique se ele foi criado corretamente."
        )
        return

    # injeta o CSS inteiro dentro de uma tag <style>
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


# =============================================================================
# HELPER DE REDE COM RETRY
# =============================================================================


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

def _quatro_anos_atras_str() -> str:
    """Data de 4 anos atrás em dd/mm/aaaa."""
    dt = date.today() - relativedelta(years=4)
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
    """Meta Selic (% a.a.). Últimos 4 anos de dados."""
    return buscar_serie_sgs(
        SGS_SERIES["selic_meta_aa"],
        data_inicial=_quatro_anos_atras_str(),
        data_final=_hoje_str(),
    )


def buscar_cdi_diario() -> pd.DataFrame:
    """CDI diário (% a.d.), últimos 2 anos de dados."""
    return buscar_serie_sgs(
        SGS_SERIES["cdi_diario"],
        data_inicial=_dois_anos_atras_str(),
        data_final=_hoje_str(),
    )


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
    """
    Calcula resumo do câmbio (ou qualquer série diária):
    - último valor
    - variação no ano
    - variação no mês
    - variação em 12 meses
    - variação em 24 meses
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
            "var_mes": None,
            "var_12m": None,
            "var_24m": None,
        }

    df = df.sort_values("data").reset_index(drop=True)

    ult = df.iloc[-1]
    ultima_data = ult["data"]
    ultimo_valor = ult["valor"]

    # ---------- Variação no ano ----------
    ano_ref = ultima_data.year
    df_ano = df[df["data"].dt.year == ano_ref]
    if not df_ano.empty:
        inicio_ano = df_ano.iloc[0]["valor"]
        var_ano = (ultimo_valor / inicio_ano - 1.0) * 100.0
    else:
        var_ano = None

    # ---------- Variação no mês ----------
    mes_ref = ultima_data.month
    df_mes = df[
        (df["data"].dt.year == ano_ref) & (df["data"].dt.month == mes_ref)
    ]
    if not df_mes.empty:
        inicio_mes = df_mes.iloc[0]["valor"]
        var_mes = (ultimo_valor / inicio_mes - 1.0) * 100.0
    else:
        var_mes = None

    # ---------- Variação em 12 meses ----------
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

    # ---------- Variação em 24 meses ----------
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
        "var_mes": var_mes,
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
        "?$top=50000"
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
    """
    Carrega o dataset de Expectativas Anuais Top5.
    """
    url = (
        f"{FOCUS_TOP5_ANUAIS_URL}"
        "?$top=50000"
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

    # nome do indicador normalizado (IPCA, PIB, Balança comercial, etc.)
    df["indicador_norm"] = df["Indicador"].apply(_normalizar_str)

    # aqui agora usamos o IndicadorDetalhe (Saldo, Exportações, Importações, etc.)
    if "IndicadorDetalhe" in df.columns:
        df["detalhe_norm"] = (
            df["IndicadorDetalhe"]
            .fillna("")
            .apply(_normalizar_str)
        )
    else:
        # fallback defensivo (não é o caso desse endpoint, mas deixa robusto)
        df["detalhe_norm"] = ""

    return df


def buscar_focus_expectativa_anual(
    indicador_substr: str,
    ano_desejado: int,
    detalhe_substr: Optional[str] = None,
):
    """
    Busca a mediana MAIS RECENTE do Focus para um dado indicador e ano.

    - Tenta match EXATO do nome do indicador (em vez de só .contains),
      pra não misturar IPCA com IPCA Administrados etc.
    - Agrupa por Data para ficar com um valor por boletim Focus.
    """
    df = _carregar_focus_raw().copy()
    if df.empty:
        return "-"

    # filtra ano de referência
    mask = df["ano_ref"] == ano_desejado

    # -------- filtro do indicador (IPCA, PIB, Selic, Câmbio...) --------
    ind_norm = _normalizar_str(indicador_substr)
    col_ind = df["indicador_norm"]

    # tenta primeiro match EXATO
    mask_ind = col_ind == ind_norm
    if not mask_ind.any():
        # se não achar nada exato, cai pro comportamento antigo (.contains)
        mask_ind = col_ind.str.contains(ind_norm, na=False)

    mask &= mask_ind

    # -------- filtro de detalhe, se usado (em alguns indicadores) --------
    if detalhe_substr:
        det_norm = _normalizar_str(detalhe_substr)
        col_det = df["detalhe_norm"]

        mask_det = col_det == det_norm
        if not mask_det.any():
            mask_det = col_det.str.contains(det_norm, na=False)

        mask &= mask_det

    df_f = df[mask].copy()
    if df_f.empty:
        return "-"

    # garante Data válida e ordena
    df_f["Data"] = pd.to_datetime(df_f["Data"], errors="coerce")
    df_f = df_f.dropna(subset=["Data"])
    if df_f.empty:
        return "-"

    df_f = df_f.sort_values("Data")

    # um valor por boletim (Data): pega a última Mediana de cada Data
    df_grp = df_f.groupby("Data", as_index=False)["Mediana"].last()

    med = df_grp.iloc[-1]["Mediana"]

    try:
        return float(med)
    except Exception:
        return "-"

def _resumo_semanal_expectativa_anual(
    indicador_substr: str,
    ano_desejado: int,
    detalhe_substr: Optional[str] = None,
) -> Dict[str, Optional[float]]:
    """
    Calcula um resumo semanal para a mediana do Focus de um indicador/ano:

      - hoje:     valor da ÚLTIMA semana (semana que contém a última data)
      - semana_4: valor de 4 semanas atrás (4 semanas Focus)
      - comp:     seta + nº de semanas desde o último movimento de alta/baixa
                  (ex.: "▲ (3)", "▼ (1)", "= (2)").

    A semana Focus é definida como período "W-FRI" (sábado a sexta).
    Dentro de cada semana, usamos o ÚLTIMO valor disponível:

      - se sexta não for feriado → usamos sexta
      - se sexta for feriado → usamos o último dia útil da semana
        (quinta, quarta, etc.)

    Para o comportamento semanal ("comp"), seguimos a lógica usada no
    relatório Focus do BC:

      1) Arredondamos a mediana em 2 casas decimais;
      2) Calculamos os deltas semana a semana;
      3) Procuramos, de trás pra frente, o ÚLTIMO delta diferente de zero;
      4) A seta é o sinal desse delta;
      5) O número entre parênteses é quantas semanas se passaram desde esse
         movimento até a semana atual (incluindo semanas estáveis).
    """

    df = _carregar_focus_raw().copy()
    if df.empty:
        return {}

    # 1) filtro pelo ano de referência
    mask = df["ano_ref"] == ano_desejado

    # 2) filtro pelo indicador (IPCA, PIB, Selic, câmbio, etc.)
    ind_norm = _normalizar_str(indicador_substr)
    col_ind = df["indicador_norm"]

    mask_ind = col_ind == ind_norm
    if not mask_ind.any():
        mask_ind = col_ind.str.contains(ind_norm, na=False)

    mask &= mask_ind

    # 3) filtro por detalhe (Saldo da balança, Administrados, etc.)
    if detalhe_substr:
        det_norm = _normalizar_str(detalhe_substr)
        col_det = df["detalhe_norm"]

        mask_det = col_det == det_norm
        if not mask_det.any():
            mask_det = col_det.str.contains(det_norm, na=False)

        mask &= mask_det

    df_f = df[mask].copy()
    if df_f.empty:
        return {}

    # 4) garantir datas válidas
    df_f["Data"] = pd.to_datetime(df_f["Data"], errors="coerce")
    df_f = df_f.dropna(subset=["Data"])
    if df_f.empty:
        return {}

    # 5) definir a semana Focus: termina na sexta (W-FRI)
    df_f["semana_focus"] = df_f["Data"].dt.to_period("W-FRI")

    # Dentro de cada semana, pegamos o ÚLTIMO valor (mais próximo da sexta)
    df_sem = (
        df_f.sort_values("Data")
        .groupby("semana_focus", as_index=False)
        .last()
    )

    if df_sem.empty:
        return {}

    # --- níveis (usamos o valor cheio, como vem da API) ---
    valores_raw = df_sem["Mediana"].astype(float).tolist()
    n = len(valores_raw)
    if n == 0:
        return {}

    val_hoje = valores_raw[-1]
    val_4 = valores_raw[-5] if n >= 5 else None  # 4 semanas atrás

    # --- comportamento semanal (setas) ---
    # Se só tem uma semana, não há histórico para comparar
    if n == 1:
        comp_txt = "-"
    else:
        # usamos a mediana arredondada em 2 casas, como o BC
        valores = [round(v, 2) for v in valores_raw]

        # deltas semana a semana
        diffs = [valores[i] - valores[i - 1] for i in range(1, n)]
        eps = 1e-9

        # 1) procuramos, de trás pra frente, o ÚLTIMO delta diferente de zero
        last_nonzero_idx = None  # índice (1-based) da semana do último movimento
        last_nonzero_delta = None

        for idx in range(len(diffs) - 1, -1, -1):
            d = diffs[idx]
            if abs(d) > eps:
                last_nonzero_idx = idx + 1  # diffs[0] = 2ª semana
                last_nonzero_delta = d
                break

        if last_nonzero_idx is None:
            # série toda estável → "=" + nº de semanas estáveis
            repeticoes = len(diffs)
            comp_txt = f"= ({repeticoes})" if repeticoes > 0 else "-"
        else:
            seta = "▲" if last_nonzero_delta > 0 else "▼"
            # nº de semanas desde esse movimento até a semana atual
            repeticoes = n - last_nonzero_idx
            comp_txt = f"{seta} ({repeticoes})" if repeticoes > 0 else seta

    return {
        "hoje": val_hoje,
        "semana_4": val_4,
        "comp": comp_txt,
    }


def buscar_focus_top5_expectativa_anual(
    indicador_substr: str,
    ano_desejado: int,
    detalhe_substr: Optional[str] = None,
):
    """
    Busca a mediana mais recente das expectativas anuais Top5 para um indicador.

    OBS.: o endpoint Top5 não traz "IndicadorDetalhe", então `detalhe_substr`
    é ignorado (mantido só para compatibilidade de assinatura).
    """
    df = _carregar_focus_top5_raw().copy()
    if df.empty:
        return "-"

    # filtra pelo ano desejado
    mask = df["ano_ref"] == ano_desejado

    # filtra pelo indicador (IPCA, PIB, Selic, câmbio...)
    ind_norm = _normalizar_str(indicador_substr)
    mask &= df["indicador_norm"].str.contains(ind_norm, na=False)

    df_f = df[mask]
    if df_f.empty:
        return "-"

    # pega a mediana mais recente
    df_f = df_f.sort_values("Data", ascending=False)
    med = df_f.iloc[0].get("Mediana", None)

    try:
        return float(med)
    except (TypeError, ValueError):
        return "-"


def montar_tabela_focus() -> pd.DataFrame:
    """
    Monta a tabela consolidada de expectativas Focus por ano,
    no formato:

        2025: [Há 4 sem., Hoje, Comp. sem.]
        2026: [Há 4 sem., Hoje, Comp. sem.]
        ...

    Ou seja, NÃO teremos mais a coluna "Há 1 semana" para reduzir a largura.
    As colunas usam o resumo semanal calculado em
    _resumo_semanal_expectativa_anual (semana Focus W-FRI).

    """

    anos = [2025, 2026, 2027, 2028]

    # (nome exibido, substring indicador, detalhe, é percentual?)
    configs: List[Tuple[str, str, Optional[str], bool]] = [
        ("IPCA (variação %)",                   "IPCA",                         None, True),
        ("PIB Total (variação %)",              "PIB Total",                    None, True),
        ("Câmbio (R\\$/US\\$)",                     "Câmbio",                      None, False),
        ("Selic (% a.a)",                       "Selic",                        None, True),
        ("IGP-M (variação %)",                  "IGP-M",                        None, True),
        ("IPCA Administrados (variação %)",     "IPCA Administrados",           None, True),
        ("Conta corrente (US$ bilhões)",        "Conta corrente",              None, False),
        ("Balança comercial (US$ bilhões)",     "Balança comercial",           "Saldo", False),
        ("Investimento direto no país (US$ bi)","Investimento direto",         None, False),
        ("Dívida líquida do setor público (% do PIB)",
                                              "Dívida líquida do setor público", None, True),
        ("Resultado primário (% do PIB)",       "Resultado primário",            None, True),
        ("Resultado nominal (% do PIB)",        "Resultado nominal",             None, True),
    ]

    # AGORA SÓ 3 subcolunas por ano
    subcolunas = ["Há 4 sem.", "Hoje", "Comp. sem."]

    linhas: List[List[str]] = []

    for nome_exibicao, indicador_sub, detalhe_sub, eh_percentual in configs:
        linha: List[str] = [nome_exibicao]

        for ano in anos:
            resumo = _resumo_semanal_expectativa_anual(
                indicador_sub,
                ano,
                detalhe_sub,
            )

            if not resumo:
                linha.extend(["-"] * len(subcolunas))
                continue

            def _fmt_val(v: Optional[float]) -> str:
                if v is None:
                    return "-"
                if eh_percentual:
                    return f"{v:.2f}%"
                return f"{v:.2f}"

            linha.append(_fmt_val(resumo.get("semana_4")))
            linha.append(_fmt_val(resumo.get("hoje")))
            linha.append(resumo.get("comp", "-") or "-")

        linhas.append(linha)

    # Cabeçalho em dois níveis (Ano x Janela), igual ao que você já está usando
    primeira_coluna = [("Indicador", "")]
    demais_colunas = []
    for ano in anos:
        for label in subcolunas:
            demais_colunas.append((str(ano), label))

    colunas = primeira_coluna + demais_colunas
    multi_cols = pd.MultiIndex.from_tuples(colunas, names=["Ano", "Janela"])

    df_focus = pd.DataFrame(linhas, columns=multi_cols)
    return df_focus


def montar_tabela_focus_top5() -> pd.DataFrame:
    """
    Tabela resumida com as expectativas Top5 (IPCA, PIB, Selic, câmbio)
    para o ano corrente e o próximo.
    """
    ano_atual = datetime.now().year
    anos = [ano_atual, ano_atual + 1]

    configs = [
        ("IPCA (a.a., Top5)",                 "ipca",       None, True),
        ("PIB Total (var.% a.a., Top5)",      "pib total",  None, True),
        ("Selic (a.a., Top5)",                "selic",      None, True),
        ("Câmbio (R\\$/US\\$, Top5)",             "cambio",     None, False),
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
                    texto = f"{valor:.2f}%"
                else:
                    texto = f"{valor:.2f}"
            else:
                texto = valor

            linha[str(ano)] = texto

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
    """
    Tabela da Selic Meta focada em níveis de política monetária:

    - Nível atual
    - Início do ano
    - Há 12 meses
    - Há 24 meses
    - Há 36 meses
    - Há 48 meses
    """
    linhas: List[Dict[str, str]] = []

    try:
        df = buscar_selic_meta_aa()
        if df.empty:
            raise ValueError("Sem dados da Selic Meta.")

        df = df.sort_values("data").reset_index(drop=True)

        # Última observação (nível atual)
        ult = df.iloc[-1]
        data_ult = ult["data"]
        nivel_atual = float(ult["valor"])

        # ---------- Início do ano ----------
        ano_ref = data_ult.year
        df_ano = df[df["data"].dt.year == ano_ref]
        if not df_ano.empty:
            inicio_ano_val = float(df_ano.iloc[0]["valor"])
        else:
            inicio_ano_val = None

        # ---------- função auxiliar p/ pegar nível <= data alvo ----------
        def _nivel_ate(df_local: pd.DataFrame, data_alvo: pd.Timestamp) -> Optional[float]:
            df_aux = df_local[df_local["data"] <= data_alvo]
            if df_aux.empty:
                return None
            return float(df_aux.iloc[-1]["valor"])

        # ---------- níveis há 12, 24, 36 e 48 meses ----------
        nivel_12m = _nivel_ate(df, data_ult - relativedelta(years=1))
        nivel_24m = _nivel_ate(df, data_ult - relativedelta(years=2))
        nivel_36m = _nivel_ate(df, data_ult - relativedelta(years=3))
        nivel_48m = _nivel_ate(df, data_ult - relativedelta(years=4))

        def _fmt(v: Optional[float]) -> str:
            return f"{v:.2f}% a.a." if v is not None else "-"

        linhas.append(
            {
                "Indicador": "Selic Meta",
                "Data": data_ult.strftime("%d/%m/%Y"),
                "Nível atual": _fmt(nivel_atual),
                "Início do ano": _fmt(inicio_ano_val),
                "Há 12 meses": _fmt(nivel_12m),
                "Há 24 meses": _fmt(nivel_24m),
                "Há 36 meses": _fmt(nivel_36m),
                "Há 48 meses": _fmt(nivel_48m),
                "Fonte": f"BCB / SGS ({SGS_SERIES['selic_meta_aa']})",
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
                "Há 24 meses": "-",
                "Há 36 meses": "-",
                "Há 48 meses": "-",
                "Fonte": "BCB / SGS",
            }
        )

    # Garante ordem das colunas
    df_out = pd.DataFrame(linhas)
    df_out = df_out[
        [
            "Indicador",
            "Data",
            "Nível atual",
            "Início do ano",
            "Há 12 meses",
            "Há 24 meses",
            "Há 36 meses",
            "Há 48 meses",
            "Fonte",
        ]
    ]
    return df_out


def montar_tabela_cdi() -> pd.DataFrame:
    """
    Tabela do CDI (over) diário com retornos acumulados:
    mês, ano, 12m e 24m.
    """
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

        # ---------- CDI no mês ----------
        df_mes = df[
            (df["data"].dt.year == ano_ref) & (df["data"].dt.month == mes_ref)
        ]
        if not df_mes.empty:
            fator_mes = (1 + df_mes["valor"] / 100).prod()
            cdi_mes = (fator_mes - 1) * 100.0
        else:
            cdi_mes = float("nan")

        # ---------- CDI no ano ----------
        df_ano = df[df["data"].dt.year == ano_ref]
        if not df_ano.empty:
            fator_ano = (1 + df_ano["valor"] / 100).prod()
            cdi_ano = (fator_ano - 1) * 100.0
        else:
            cdi_ano = float("nan")

        # ---------- CDI em 12 meses ----------
        corte_12m = data_ult - relativedelta(years=1)
        df_12m = df[df["data"] >= corte_12m]
        if not df_12m.empty:
            fator_12m = (1 + df_12m["valor"] / 100).prod()
            cdi_12m = (fator_12m - 1) * 100.0
        else:
            cdi_12m = float("nan")

        # ---------- CDI em 24 meses ----------
        corte_24m = data_ult - relativedelta(years=2)
        df_24m = df[df["data"] >= corte_24m]
        if not df_24m.empty:
            fator_24m = (1 + df_24m["valor"] / 100).prod()
            cdi_24m = (fator_24m - 1) * 100.0
        else:
            cdi_24m = float("nan")

        linhas.append(
            {
                "Indicador": "CDI (over) diário",
                "Data ref.": data_ult.strftime("%d/%m/%Y"),
                "Nível diário": f"{taxa_ult:.4f}% a.d.",
                "CDI no mês": f"{cdi_mes:.2f}%" if pd.notna(cdi_mes) else "-",
                "CDI no ano": f"{cdi_ano:.2f}%" if pd.notna(cdi_ano) else "-",
                "CDI em 12 meses": f"{cdi_12m:.2f}%" if pd.notna(cdi_12m) else "-",
                "CDI em 24 meses": f"{cdi_24m:.2f}%" if pd.notna(cdi_24m) else "-",
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
                "CDI em 24 meses": "-",
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
            var_mes = f"{r['var_mes']:+.2f}%" if r["var_mes"] is not None else "-"
            var_12m = f"{r['var_12m']:+.2f}%" if r["var_12m"] is not None else "-"
            var_24m = f"{r['var_24m']:+.2f}%" if r["var_24m"] is not None else "-"
        else:
            ultima_data_str = "-"
            nivel_atual = "sem dados"
            nivel_12m = "-"
            nivel_24m = "-"
            var_ano = "-"
            var_mes = "-"
            var_12m = "-"
            var_24m = "-"

        # 👉 Aqui a ordem das colunas já vem com Var. mês antes de Var. ano
        linhas.append(
            {
                "Indicador": "Dólar PTAX - venda",
                "Data": ultima_data_str,
                "Nível atual": nivel_atual,
                "Nível há 12m": nivel_12m,
                "Nível há 24m": nivel_24m,
                "Var. mês": var_mes,
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
                "Var. mês": "-",
                "Var. ano": "-",
                "Var. 12m": "-",
                "Var. 24m": "-",
                "Fonte": "BCB / SGS",
            }
        )

    # 🔽 força a ordem das colunas na tabela
    df = pd.DataFrame(linhas)
    ordem_colunas = [
        "Indicador",
        "Data",
        "Nível atual",
        "Nível há 12m",
        "Nível há 24m",
        "Var. mês",   # primeiro
        "Var. ano",
        "Var. 12m",
        "Var. 24m",
        "Fonte",
    ]
    df = df[ordem_colunas]
    return df



def _format_br_number(valor: float | None, casas: int = 2) -> str:
    """
    Formata número em padrão brasileiro, ex: 155.381,00
    """
    if valor is None:
        return "-"
    s = f"{valor:,.{casas}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

@st.cache_data(ttl=86400)  # cache por até 1 dia (24h)
def obter_historico_ibovespa_inteligente(chave_dia: str) -> pd.DataFrame:
    """
    Atualiza o histórico do Ibovespa a partir do Ipeadata.

    IMPORTANTE (comportamento):
    - A função é cacheada por dia (usando 'chave_dia' como parte da chave).
    - Se a atualização ONLINE der certo uma vez no dia,
      as próximas chamadas no MESMO dia reaproveitam o mesmo DataFrame
      (não chamam o Ipeadata de novo).
    - Se a chamada der erro (timeout, etc.), nenhuma atualização é cacheada
      e as próximas execuções podem tentar novamente no mesmo dia.
    """
    # Aqui chamamos a função que fala com o Ipeadata e atualiza o CSV.
    # Se der erro, a exceção sobe para quem chamou (montar_tabela_ibovespa).
    return atualizar_historico_ibovespa()


def montar_tabela_ibovespa() -> pd.DataFrame:
    """
    Monta quadro do Ibovespa (fechamento) no padrão dos demais:
    1 linha com ano, mês, 12m e 24m.

    Estratégia:
    - Tenta atualizar o histórico local a partir do Ipeadata.
    - Se der erro (timeout, etc.), cai para a base local em CSV.
    - Só mostra mensagem de erro se não houver nem dado online nem base local.
    """
    linhas: List[Dict[str, str]] = []

    try:
        origem_dados = "online"

        # chave por dia: garante, junto com o cache, no máximo 1 atualização
        # bem-sucedida por dia a partir do Ipeadata
        chave_dia = date.today().strftime("%Y-%m-%d")

        try:
            # 1) Tenta atualizar histórico (API Ipeadata) de forma cacheada por dia
            df_hist = obter_historico_ibovespa_inteligente(chave_dia)
        except Exception as e_online:
            # 2) Se falhar (timeout, erro de rede, etc.), tenta usar apenas o CSV já salvo
            try:
                df_hist = carregar_historico_ibovespa()
                origem_dados = "offline"
            except Exception:
                # 3) Sem base local → propaga o erro original
                raise e_online

        if df_hist is None or df_hist.empty:
            raise ValueError("Histórico do Ibovespa vazio.")

        # Garante tipos e ordenação
        df = df_hist.copy()
        df["data"] = pd.to_datetime(df["data"])
        df = df.sort_values("data").set_index("data")
        close = df["valor"]

        ultimo = float(close.iloc[-1])
        data_ult = close.index[-1]

        # ---------- variação no ano ----------
        mask_ano = close.index.year == data_ult.year
        serie_ano = close[mask_ano]
        if not serie_ano.empty:
            base_ano = float(serie_ano.iloc[0])
            var_ano_val = (ultimo / base_ano - 1.0) * 100.0
        else:
            var_ano_val = None

        # ---------- variação no mês ----------
        mask_mes = (close.index.year == data_ult.year) & (
            close.index.month == data_ult.month
        )
        serie_mes = close[mask_mes]
        if not serie_mes.empty:
            base_mes = float(serie_mes.iloc[0])
            var_mes_val = (ultimo / base_mes - 1.0) * 100.0
        else:
            var_mes_val = None

        # ---------- 12m e 24m ----------
        def _pega_base_ate(data_limite):
            serie = close[close.index <= data_limite]
            if serie.empty:
                return None, None
            return float(serie.iloc[-1]), serie.index[-1]

        base_12m, data_12m = _pega_base_ate(data_ult - relativedelta(years=1))
        base_24m, data_24m = _pega_base_ate(data_ult - relativedelta(years=2))

        var_12m_val = (
            (ultimo / base_12m - 1.0) * 100.0 if base_12m is not None else None
        )
        var_24m_val = (
            (ultimo / base_24m - 1.0) * 100.0 if base_24m is not None else None
        )

        # ---------- formatações em string ----------
        data_str = data_ult.strftime("%d/%m/%Y")
        nivel_atual = f"{_format_br_number(ultimo, 2)} pts"

        if base_12m is not None and data_12m is not None:
            nivel_12m = f"{_format_br_number(base_12m, 2)} pts"
        else:
            nivel_12m = "-"

        if base_24m is not None and data_24m is not None:
            nivel_24m = f"{_format_br_number(base_24m, 2)} pts"
        else:
            nivel_24m = "-"

        var_ano = f"{var_ano_val:+.2f}%" if var_ano_val is not None else "-"
        var_mes = f"{var_mes_val:+.2f}%" if var_mes_val is not None else "-"
        var_12m = f"{var_12m_val:+.2f}%" if var_12m_val is not None else "-"
        var_24m = f"{var_24m_val:+.2f}%" if var_24m_val is not None else "-"

        if origem_dados == "online":
            fonte = "Ipeadata (GM366_IBVSP366)"
        else:
            fonte = "Ipeadata (GM366_IBVSP366) – base local"

        linhas.append(
            {
                "Indicador": "Ibovespa - fechamento",
                "Data": data_str,
                "Nível atual": nivel_atual,
                "Nível há 12m": nivel_12m,
                "Nível há 24m": nivel_24m,
                "Var. mês": var_mes,
                "Var. ano": var_ano,
                "Var. 12m": var_12m,
                "Var. 24m": var_24m,
                "Fonte": fonte,
            }
        )

    except Exception:
        linhas.append(
            {
                "Indicador": "Ibovespa - fechamento",
                "Data": "-",
                "Nível atual": "Indisponível (falha ao obter dados)",
                "Nível há 12m": "-",
                "Nível há 24m": "-",
                "Var. mês": "-",
                "Var. ano": "-",
                "Var. 12m": "-",
                "Var. 24m": "-",
                "Fonte": "Ipeadata",
            }
        )

    # força a ordem das colunas na tabela
    df_saida = pd.DataFrame(linhas)
    ordem_colunas = [
        "Indicador",
        "Data",
        "Nível atual",
        "Nível há 12m",
        "Nível há 24m",
        "Var. mês",
        "Var. ano",
        "Var. 12m",
        "Var. 24m",
        "Fonte",
    ]
    df_saida = df_saida[ordem_colunas]
    return df_saida


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
    df_ibov_curto,
    df_di_fut,   # ainda passo, mas não uso mais a tabela diária
    df_hist_di,
):
    """
    Estrutura:
    - Aba "Brasil"
        - Sub-aba "Curto prazo":
            - Selic Meta, CDI acumulado, câmbio PTAX e Ibovespa
        - Sub-aba "Curvas & Tesouro":
            - Curva de juros – ANBIMA (prefixado x IPCA+ x breakeven)
            - Histórico DI Futuro (B3) com tabela resumida (1 contrato por ano)
            - Oportunidades na curva – Tesouro vs ANBIMA
        - Sub-aba "Expectativas":
            - Focus – Mediana (consenso do mercado)
            - Focus – Top 5 (instituições mais assertivas)
    """

    tab_br, tab_mundo = st.tabs(["Brasil", "Mundo"])

    # ==========================
    # ABA BRASIL
    # ==========================
    with tab_br:
        subtab_indic_br, subtab_exp_br, subtab_curvas_tesouro = st.tabs(
            ["Curto prazo", "Expectativas", "Curvas & Tesouro"]
        )

        # -------- Indicadores BR --------
        with subtab_indic_br:
            # Bloco de cards / visão rápida (já vem com título próprio)
            render_bloco_curto_prazo_br()

            # Linha separadora opcional
            st.markdown("---")

            # Título só para os QUADROS abaixo (tabelas)
            st.markdown("### Outros indicadores de curto prazo – Brasil")
            st.caption(
                "Quadros detalhados com Selic meta, CDI acumulado, câmbio PTAX e "
                "Ibovespa, complementando os cards acima."
            )


            # Selic
            st.markdown("**Taxa básica – Selic Meta**")
            st.table(df_selic.set_index("Indicador"))

            # CDI
            st.markdown("**CDI – Retorno acumulado**")
            st.table(df_cdi.set_index("Indicador"))

            # Câmbio
            st.markdown("**Câmbio – Dólar PTAX (venda)**")
            st.table(df_ptax.set_index("Indicador"))

            # Bolsa
            st.markdown("**Bolsa – Ibovespa (fechamento)**")
            st.table(df_ibov_curto.set_index("Indicador"))           
            

        # -------- Curvas & Tesouro BR --------
        with subtab_curvas_tesouro:
            # -------------------------------
            # Curva de juros – ANBIMA
            # -------------------------------

            vertices_anos = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30]

            vertice = st.radio(
                "Vértice (anos)",
                options=vertices_anos,
                horizontal=True,
                index=1,  # 2 anos
                key="vertice_anbima",
            )

            df_var = montar_curva_anbima_variacoes(anos=vertice)

            if df_var.empty:
                st.info(
                    "Ainda não há histórico suficiente para esse vértice. "
                    "Conforme o tempo passar, o painel vai acumulando "
                    "observações diárias das curvas ANBIMA."
                )
            else:
                col_pref, col_ipca, col_breakeven = st.columns(3)

                def montar_tabela_curva(
                    df_base: pd.DataFrame,
                    nome_coluna: str,
                    titulo: str,
                ) -> pd.DataFrame:
                    df_show = (
                        df_base[["Data", nome_coluna]]
                        .rename(columns={nome_coluna: titulo})
                        .set_index("Data")
                    )

                    df_show[titulo] = df_show[titulo].apply(
                        lambda x: "-"
                        if pd.isna(x)
                        else f"{float(x):.3f}".replace(".", ",")
                    )
                    return df_show

                # Tabela 1 – Prefixada (juro nominal)
                with col_pref:
                    st.markdown("**Curva prefixada (juro nominal)**")
                    df_pref = montar_tabela_curva(
                        df_var,
                        "Juro Nominal (%)",
                        "Taxa (% a.a.)",
                    )
                    st.table(df_pref)

                # Tabela 2 – IPCA+ (juro real)
                with col_ipca:
                    st.markdown("**Curva IPCA+ (juro real)**")
                    df_ipca = montar_tabela_curva(
                        df_var,
                        "Juro Real (%)",
                        "Taxa (% a.a.)",
                    )
                    st.table(df_ipca)

                # Tabela 3 – Breakeven
                with col_breakeven:
                    st.markdown("**Breakeven (inflação implícita)**")
                    df_be = montar_tabela_curva(
                        df_var,
                        "Breakeven (%)",
                        "Taxa (% a.a.)",
                    )
                    st.table(df_be)

            # -------------------------------
            # Oportunidades na curva – Tesouro vs ANBIMA
            # -------------------------------
            st.markdown("### Oportunidades na curva – Tesouro vs ANBIMA")
            st.caption(
                "Títulos do Tesouro Direto comparados com a curva limpa da dívida pública "
                "(ANBIMA). Spreads positivos (Barato) indicam que o Tesouro paga acima "
                "da curva para o mesmo prazo (anos)."
            )

            with st.spinner("Carregando oportunidades em Tesouro Direto..."):
                try:
                    df_pre = get_comparacao_tesouro_pre_vs_curva()
                    df_ipca = get_comparacao_tesouro_ipca_vs_curva()

                    # Datas de referência
                    data_tesouro_ref = None
                    data_curva_ref = None
                    for df_tmp in (df_pre, df_ipca):
                        if df_tmp is not None and not df_tmp.empty:
                            if (
                                data_tesouro_ref is None
                                and "data_base_tesouro" in df_tmp.columns
                            ):
                                data_tesouro_ref = df_tmp["data_base_tesouro"].iloc[0]
                            if (
                                data_curva_ref is None
                                and "data_curva_anbima" in df_tmp.columns
                            ):
                                data_curva_ref = df_tmp["data_curva_anbima"].iloc[0]

                    if (df_pre is None or df_pre.empty) and (
                        df_ipca is None or df_ipca.empty
                    ):
                        st.info(
                            "Não foi possível carregar oportunidades em Tesouro Direto hoje."
                        )
                    else:
                        # Família (prefixado x IPCA+)
                        familia_escolhida = st.radio(
                            "Família de título",
                            ["Tesouro Prefixado", "Tesouro IPCA+"],
                            key="familia_tesouro_curva",
                            horizontal=True,
                        )

                        if familia_escolhida == "Tesouro Prefixado":
                            df_base = df_pre.copy()
                            col_taxa_curva = "taxa_pre_anbima"
                            label_taxa_curva = "Curva pré ANBIMA (% a.a.)"
                        else:
                            df_base = df_ipca.copy()
                            col_taxa_curva = "taxa_ipca_anbima"
                            label_taxa_curva = "Curva real ANBIMA (% a.a.)"

                        if df_base is None or df_base.empty:
                            st.info("Sem dados disponíveis para essa família de títulos.")
                        else:
                            # Filtro por sinal
                            sinais_disponiveis = ["Barato", "No preço", "Caro"]
                            padrao = {"Barato": True, "No preço": True, "Caro": False}

                            col_s1, col_s2, col_s3 = st.columns(3)
                            flags = {}
                            with col_s1:
                                flags["Barato"] = st.checkbox(
                                    "Barato",
                                    value=padrao["Barato"],
                                    key="chk_barato_curva",
                                )
                            with col_s2:
                                flags["No preço"] = st.checkbox(
                                    "No preço",
                                    value=padrao["No preço"],
                                    key="chk_nopreco_curva",
                                )
                            with col_s3:
                                flags["Caro"] = st.checkbox(
                                    "Caro",
                                    value=padrao["Caro"],
                                    key="chk_caro_curva",
                                )

                            sinais_ativos = [
                                s for s in sinais_disponiveis if flags.get(s, False)
                            ]
                            if not sinais_ativos:
                                st.info(
                                    "Nenhum sinal selecionado. Marque ao menos uma opção "
                                    "(Barato, No preço ou Caro)."
                                )
                            else:
                                df_filtrado = df_base[df_base["Sinal"].isin(sinais_ativos)]

                                if df_filtrado.empty:
                                    st.info(
                                        "Não há títulos com os sinais selecionados "
                                        "nas condições atuais de mercado."
                                    )
                                else:
                                    df_show = df_filtrado.copy()

                                    # Taxas em % com 2 casas
                                    for col in ["taxa_compra", col_taxa_curva]:
                                        if col in df_show.columns:
                                            df_show[col] = df_show[col].map(
                                                lambda x: f"{x:.2f}"
                                                if pd.notna(x)
                                                else "-"
                                            )

                                    # Spread em bps sem casa decimal
                                    if "spread_bps" in df_show.columns:
                                        df_show["spread_bps"] = df_show[
                                            "spread_bps"
                                        ].map(
                                            lambda x: f"{x:.0f}"
                                            if pd.notna(x)
                                            else "-"
                                        )

                                    renomear = {
                                        "nome_titulo": "Título",
                                        "data_vencimento": "Vencimento",
                                        "prazo_anos": "Prazo (anos)",
                                        "taxa_compra": "Tesouro (% a.a.)",
                                        col_taxa_curva: label_taxa_curva,
                                        "spread_bps": "Spread x curva (bps)",
                                    }
                                    df_show = df_show.rename(columns=renomear)

                                    colunas_ordem = [
                                        "Título",
                                        "Vencimento",
                                        "Prazo (anos)",
                                        "Tesouro (% a.a.)",
                                        label_taxa_curva,
                                        "Spread x curva (bps)",
                                        "Sinal",
                                    ]
                                    colunas_existentes = [
                                        c for c in colunas_ordem if c in df_show.columns
                                    ]
                                    df_show = df_show[colunas_existentes]

                                    tabela_out = df_show.copy()
                                    if "Título" in tabela_out.columns:
                                        tabela_out = tabela_out.set_index("Título")
                                    st.table(tabela_out)

                                    legenda_partes = []
                                    if data_tesouro_ref is not None:
                                        try:
                                            dt_ref = pd.to_datetime(
                                                data_tesouro_ref
                                            ).strftime("%d/%m/%Y")
                                        except Exception:
                                            dt_ref = str(data_tesouro_ref)
                                        legenda_partes.append(
                                            f"Tesouro Direto – dados de {dt_ref}."
                                        )
                                    if data_curva_ref is not None:
                                        try:
                                            dc_ref = pd.to_datetime(
                                                data_curva_ref
                                            ).strftime("%d/%m/%Y")
                                        except Exception:
                                            dc_ref = str(data_curva_ref)
                                        legenda_partes.append(
                                            f"Curva ANBIMA (ETTJ soberana) – dados de {dc_ref}."
                                        )

                                    if legenda_partes:
                                        st.caption("Fontes: " + " ".join(legenda_partes))

                except Exception as e:
                    st.warning(
                        f"Não foi possível carregar a comparação Tesouro x Curva ANBIMA: {e}"
                    )
            # ---------------------------------------------
            # Histórico – DI Futuro (B3) – 1 contrato por ano, próximos 10 anos
            # ---------------------------------------------
            st.markdown("**Histórico – DI Futuro (B3)**")
            with st.expander(
                "Ver curva DI Futuro (1 contrato por ano, próximos 10 anos)"
            ):
                if df_hist_di is None or df_hist_di.empty:
                    st.info(
                        "Ainda não há histórico salvo de DI Futuro. "
                        "Certifique-se de rodar o app em dias úteis para ir "
                        "acumulando as observações no arquivo "
                        "`data/di_futuro/di1_historico.csv`."
                    )
                else:
                    # cópia ordenada por data
                    df_hist = df_hist_di.copy()
                    df_hist["data"] = pd.to_datetime(df_hist["data"])
                    df_hist = df_hist.sort_values("data")

                    # garante coluna de volume numérica (se existir)
                    if "volume" in df_hist.columns:
                        df_hist["volume"] = pd.to_numeric(
                            df_hist["volume"], errors="coerce"
                        )
                    else:
                        df_hist["volume"] = pd.NA

                    # Trata taxa / ajuste → cria 'taxa_final'
                    df_hist["taxa"] = pd.to_numeric(
                        df_hist.get("taxa"), errors="coerce"
                    )
                    if "ajuste" in df_hist.columns:
                        df_hist["ajuste"] = pd.to_numeric(
                            df_hist.get("ajuste"), errors="coerce"
                        )
                        df_hist["taxa_final"] = df_hist["taxa"].fillna(
                            df_hist["ajuste"]
                        )
                    else:
                        df_hist["taxa_final"] = df_hist["taxa"]

                    # Extrai o ano de vencimento do ticker (ex.: DI1F26 -> 2026)
                    def _extrair_ano(ticker: str) -> Optional[int]:
                        if not isinstance(ticker, str) or len(ticker) < 2:
                            return None
                        sufixo = ticker[-2:]
                        if sufixo.isdigit():
                            return 2000 + int(sufixo)
                        return None

                    df_hist["ano_venc"] = df_hist["ticker"].apply(_extrair_ano)

                    # Ano de referência = ano da última data observada
                    ano_ref = int(df_hist["data"].max().year)
                    # próximos 10 anos (ano_ref, ano_ref+1, ..., ano_ref+9)
                    anos_desejados = [ano_ref + i for i in range(10)]

                    # Ordem dos meses da B3 (pra fallback de liquidez)
                    ordem_meses = "FGHJKMNQUVXZ"

                    # Para cada ano desejado, escolhe um contrato representativo
                    contratos_escolhidos = []
                    for ano in anos_desejados:
                        subset = df_hist[df_hist["ano_venc"] == ano]
                        if subset.empty:
                            continue

                        # Pega só a última data de cada contrato
                        subset = subset.sort_values(["ticker", "data"])
                        subset_ult = subset.groupby("ticker").tail(1)

                        # Escolhe o contrato com maior volume; se empate, usa ordem_meses
                        subset_ult = subset_ult.copy()
                        subset_ult["volume"] = subset_ult["volume"].fillna(0)

                        # separa mês-letra
                        subset_ult["mes_letra"] = subset_ult["ticker"].str[-3:-2]

                        def _ordem_mes(letra: str) -> int:
                            try:
                                return ordem_meses.index(letra)
                            except ValueError:
                                return len(ordem_meses)

                        subset_ult["ordem_mes"] = subset_ult["mes_letra"].apply(_ordem_mes)

                        subset_ult = subset_ult.sort_values(
                            ["volume", "ordem_mes"], ascending=[False, True]
                        )

                        contrato_escolhido = subset_ult.iloc[0]
                        contratos_escolhidos.append(contrato_escolhido)

                    if not contratos_escolhidos:
                        st.info(
                            "Não foi possível selecionar contratos representativos de DI Futuro."
                        )
                    else:
                        df_curva_hoje = pd.DataFrame(contratos_escolhidos)

                        # Ordena por ano de vencimento
                        df_curva_hoje = df_curva_hoje.sort_values("ano_venc")

                        st.markdown(
                            "Tabela – 1 contrato de DI Futuro por ano (próximos 10 anos)"
                        )
                        df_resumo_curva = (
                            df_curva_hoje[["ticker", "ano_venc", "data", "taxa_final"]]
                            .assign(
                                Ano_venc=lambda d: d["ano_venc"].astype(int).astype(str),
                                Data=lambda d: d["data"].dt.strftime("%d/%m/%Y"),
                                Taxa=lambda d: d["taxa_final"].map(
                                    lambda v: f"{v:.4f}%"
                                ),
                            )[["ticker", "Ano_venc", "Data", "Taxa"]]
                            .rename(
                                columns={
                                    "ticker": "Contrato",
                                    "Ano_venc": "Ano venc.",
                                }
                            )
                            .set_index("Contrato")
                        )
                        st.table(df_resumo_curva)

        # -------- Expectativas BR --------
        with subtab_exp_br:
            st.markdown("### Expectativas de mercado – Brasil (Focus)")

            # descobre a data mais recente nas bases do Focus (Mediana e Top5)
            try:
                df_raw_focus = _carregar_focus_raw()
                data_mediana = (
                    df_raw_focus["Data"].max()
                    if not df_raw_focus.empty
                    else None
                )
            except Exception:
                data_mediana = None

            try:
                df_raw_top5 = _carregar_focus_top5_raw()
                data_top5 = (
                    df_raw_top5["Data"].max()
                    if not df_raw_top5.empty
                    else None
                )
            except Exception:
                data_top5 = None

            # funçãozinha auxiliar para formatar a data em texto
            def _fmt_data(d):
                if d is None or pd.isna(d):
                    return "sem data disponível"
                try:
                    return pd.to_datetime(d).strftime("%d/%m/%Y")
                except Exception:
                    return str(d)

            data_mediana_txt = _fmt_data(data_mediana)
            data_top5_txt = _fmt_data(data_top5)

            st.markdown("**Focus – Mediana (consenso do mercado)**")
            st.caption(
                f"Mediana das projeções de todas as instituições participantes "
                f"do boletim Focus. Dados de {data_mediana_txt}."
            )
            st.table(df_focus.set_index("Indicador"))

            st.markdown("**Focus – Top 5 (instituições mais assertivas)**")
            st.caption(
                f"Mediana das projeções das 5 instituições com melhor "
                f"desempenho histórico no Focus. Dados de {data_top5_txt}."
            )
            st.table(df_focus_top5.set_index("Indicador"))


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
    # Se vier vazio, mostra aviso amigável
    if df_ativ is None or df_ativ.empty:
        st.info("Ainda não há dados de atividade econômica disponíveis.")
        return


    # ---------------- TÍTULO + DESCRIÇÃO (fora do card) ----------------
    st.markdown("### Atividade econômica – IBGE")
    st.caption(
        "Indicadores de volume de Varejo (PMC), Serviços (PMS) e Indústria (PIM-PF), "
        "classificados como indicadores coincidentes do ciclo econômico."
    )

    # ---------------- CARD ION (igual espírito dos outros blocos) ----------------
    # Tudo que é “conteúdo” do bloco (título pequeno + filtro + tabela)
    # fica dentro desse container, que o theme_ion estiliza como card.
    with st.container(border=True):

        # Linha do subtítulo + filtro (2 colunas, estilo Ion)
        col_label, col_filtro = st.columns([3, 1])

        with col_label:
            st.markdown("##### Classificação cíclica dos indicadores")

        with col_filtro:
            filtro_classif = st.radio(
                "Classificação",
                ["Coincidente", "Todos"],
                index=0,  # Coincidente como padrão
                key="filtro_atividade_ibge",
                horizontal=True,  # fica lado a lado, menos poluição visual
            )

        # --------- LÓGICA DO FILTRO (igual você já tinha) ---------
        df_exibir = df_ativ.copy()

        if filtro_classif != "Todos":
            df_exibir = df_exibir[
                df_exibir["Classificação"]
                .astype(str)
                .str.contains(filtro_classif, case=False, na=False)
            ]

        # --------- TABELA NO PADRÃO ION ---------
        st.table(
        df_exibir.set_index(["Indicador", "Classificação"])
    )

    # ---------------- AVISO EMBAIXO (fora do card, igual outros blocos) ----------------
    st.info(
        "⚙️ Em construção (parte avançada): inclusão de indicadores antecedentes "
        "(PMI, confiança FGV) e defasados (desemprego, massa salarial), "
        "todos com a mesma lógica de classificação cíclica."
    )


def render_bloco6_inflacao(df_infla: pd.DataFrame):
    """Bloco 6 – Inflação (IPCA e IPCA-15) em layout Ion-like."""
    if df_infla is None or df_infla.empty:
        st.markdown("### IPCA e IPCA-15 – visão consolidada")
        st.caption(
            "Inflação cheia e IPCA-15: mensal, acumulado no ano e em 12 meses."
        )
        st.info(
            "Ainda não há dados de inflação montados (DataFrame vazio). "
            "Verifique a rotina de carregamento dos dados."
        )
        return

    # Deixa o DataFrame com um índice mais bonitinho
    df_view = df_infla.copy()
    df_view = df_view.set_index("Indicador")

    st.markdown("### IPCA e IPCA-15 – visão consolidada")
    st.caption(
        "Inflação cheia e IPCA-15: mensal, acumulado no ano e em 12 meses."
    )

    col_label, _ = st.columns([3, 1])
    with col_label:
        st.markdown("##### Indicadores de inflação – IBGE / SIDRA")

    # AQUI é a mudança: usar st.table para pegar o CSS Íon,
    # em vez de st.dataframe (que fica preto).
    st.table(df_view)

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

@st.cache_data(ttl=86400)  # 1 dia
def get_comparacao_tesouro_pre_vs_curva():
    """
    Calcula a comparação Tesouro Prefixado x Curva Pré ANBIMA
    e deixa o resultado em cache por 1 dia.
    """
    return comparar_tesouro_pre_vs_curva()


@st.cache_data(ttl=86400)  # 1 dia
def get_comparacao_tesouro_ipca_vs_curva():
    """
    Calcula a comparação Tesouro IPCA+ x Curva Real ANBIMA
    e deixa o resultado em cache por 1 dia.
    """
    return comparar_tesouro_ipca_vs_curva()


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


@st.cache_data(ttl=60 * 30)
def get_tabela_ibovespa_curto():
    return montar_tabela_ibovespa()


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

    Se alguma chamada der erro, a exceção sobe para quem chamou.
    """
    # Se ANBIMA ou DI Futuro falharem, vamos deixar a exceção subir.
    # O tratamento (warning) será feito na camada de cache.
    atualizar_todas_as_curvas()
    atualizar_historico_di_futuro()

@st.cache_data(ttl=86400)  # 86400 segundos = 24 horas
def atualizar_dados_externos_cache(chave_dia: str) -> bool:
    """
    Executa a atualização das curvas ANBIMA e do histórico de DI Futuro B3
    no máximo UMA vez por dia (por servidor).

    Regras:
    - Se ANBIMA + DI Futuro atualizarem com sucesso, a função retorna True
      e esse resultado fica cacheado para o 'chave_dia' informado.
      => Próximas chamadas no mesmo dia NÃO batem de novo nas APIs.
    - Se alguma chamada lançar exceção, nada é cacheado, e a exceção sobe.
      => Próximas chamadas no mesmo dia podem tentar atualizar de novo.
    """
    atualizar_dados_externos()
    return True


def main():
    st.set_page_config(
        page_title="Observatório Macro",
        layout="wide",
    )

    # aplica tema visual global (CSS externo)
    load_theme_css()

       # 🔧 Forçar comportamento “normal” das colunas
    st.markdown(
        """
        <style>
        /* Garante que as colunas não “quebrem” sozinhas para 100% */
        div[data-testid="column"] {
            flex: 1 1 0 !important;
            min-width: 0 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # 🔄 Atualiza ANBIMA + DI Futuro B3 no máximo 1 vez por dia (por servidor)
    chave_dia = date.today().strftime("%Y-%m-%d")
    with st.spinner("Atualizando curvas ANBIMA e histórico de DI Futuro B3..."):
        try:
            atualizar_dados_externos_cache(chave_dia)
        except Exception as e:
            st.warning(
                "Não foi possível atualizar curvas ANBIMA / histórico de DI Futuro B3 "
                "neste momento. Serão usados os últimos dados salvos em disco."
            )
            logging.exception(
                "Erro ao atualizar dados externos (ANBIMA / DI Futuro B3): %s", e
            )

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
        df_ibov_curto = get_tabela_ibovespa_curto()
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
        with st.container():
            render_bloco1_observatorio_mercado(
                df_focus=df_focus,
                df_focus_top5=df_focus_top5,
                df_selic=df_selic,
                df_cdi=df_cdi,
                df_ptax=df_ptax,
                df_ibov_curto=df_ibov_curto,
                df_di_fut=df_di_fut,
                df_hist_di=df_hist_di,
            )

    with tab2:
        with st.container():
            render_bloco2_fiscal()

    with tab3:
        with st.container():
            render_bloco3_setor_externo()

    with tab4:
        with st.container():
            render_bloco4_mercado_trabalho()

    with tab5:
        with st.container():
            render_bloco5_atividade(df_ativ=df_ativ)

    with tab6:
        with st.container():
            render_bloco6_inflacao(df_infla=df_infla)

    with tab7:
        with st.container():
            render_bloco7_credito_condicoes()

    st.write("---")
    st.caption(
        "Atualize os dados recarregando a página ou rodando novamente "
        "`streamlit run indicadores_macro_br.py`."
    )


if __name__ == "__main__":
    main()
