# dados_curto_prazo_br.py
# -*- coding: utf-8 -*-

import pandas as pd
import requests
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
from functools import lru_cache
from dateutil.relativedelta import relativedelta

# Caminho para o CSV de curvas ANBIMA (já usado no bloco de Curvas)
BASE_DIR = Path(__file__).parent
CAMINHO_CURVAS_ANBIMA = BASE_DIR / "data" / "curvas_anbima_full.csv"

# =============================================================================
# DATACLASSES – ESTRUTURA DE DADOS DO BLOCO CURTO PRAZO
# =============================================================================


@dataclass
class MoedaJurosCurtoPrazo:
    # Selic / CDI / PTAX – cards principais
    selic_meta: Optional[float]
    cdi_acumulado_mes: Optional[float]
    cdi_variacao_dia: Optional[float]  # var do CDI (em p.p.) vs dia anterior
    ptax_fechamento: Optional[float]
    ptax_variacao_dia: Optional[float]  # var % no dia

    # Observações (para textos / morning call)
    selic_obs: str = ""
    cdi_obs: str = ""
    ptax_obs: str = ""

    # Complementares / histórico (para textos e possíveis tooltips)
    cdi_dia: Optional[float] = None  # taxa do CDI no dia (%)
    selic_12m: Optional[float] = None
    selic_24m: Optional[float] = None
    selic_ultima_decisao: Optional[float] = None  # nível ANTES do último Copom
    cdi_no_ano: Optional[float] = None
    cdi_em_12_meses: Optional[float] = None

    # PTAX – níveis e variações 12m / 24m
    ptax_nivel_12m: Optional[float] = None
    ptax_nivel_24m: Optional[float] = None
    ptax_var_12m: Optional[float] = None
    ptax_var_24m: Optional[float] = None


@dataclass
class AtivosDomesticosCurtoPrazo:
    ibov_nivel: float
    ibov_var_dia: float
    ibov_var_mes: float
    ibov_var_ano: float

    data_curva_anbima: Optional[date]
    pre_2_anos: Optional[float]
    pre_5_anos: Optional[float]
    curva_obs: str

    # DI Futuro – aproximando 2 anos e 5 anos
    di_2_anos_taxa: Optional[float] = None
    di_2_anos_delta: Optional[float] = None  # var. em p.p. no dia
    di_5_anos_taxa: Optional[float] = None
    di_5_anos_delta: Optional[float] = None  # var. em p.p. no dia


@dataclass
class DadosCurtoPrazoBR:
    moeda_juros: MoedaJurosCurtoPrazo
    ativos_domesticos: AtivosDomesticosCurtoPrazo


# =============================================================================
# HELPERS – BCB / SGS (Selic, CDI, PTAX)
# =============================================================================

SGS_SERIES = {
    "selic_meta_aa": 432,   # Selic Meta – ao ano
    "cdi_diario": 12,       # CDI – taxa diária (%)
    "ptax_venda": 10813,    # Dólar PTAX – venda (R$/US$)
}


def _hoje_str() -> str:
    return date.today().strftime("%d/%m/%Y")


def _um_ano_atras_str() -> str:
    dt = date.today() - relativedelta(years=1)
    return dt.strftime("%d/%m/%Y")


def _dois_anos_atras_str() -> str:
    dt = date.today() - relativedelta(years=2)
    return dt.strftime("%d/%m/%Y")


@lru_cache(maxsize=32)
def _buscar_serie_sgs_cached(
    codigo: int,
    data_inicial: str,
    data_final: str,
) -> pd.DataFrame:
    """
    Versão simplificada de busca na API do BCB (SGS), com cache em memória.
    Retorna DataFrame com colunas: data (datetime64) e valor (float).
    """
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
        f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
    )
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
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
    if data_inicial is None:
        data_inicial = _um_ano_atras_str()
    if data_final is None:
        data_final = _hoje_str()
    return _buscar_serie_sgs_cached(codigo, data_inicial, data_final).copy()


def buscar_selic_meta_aa(
    data_inicial: Optional[str] = None,
    data_final: Optional[str] = None,
) -> pd.DataFrame:
    return buscar_serie_sgs(
        SGS_SERIES["selic_meta_aa"],
        data_inicial=data_inicial,
        data_final=data_final,
    )


def buscar_cdi_diario(
    data_inicial: Optional[str] = None,
    data_final: Optional[str] = None,
) -> pd.DataFrame:
    return buscar_serie_sgs(
        SGS_SERIES["cdi_diario"],
        data_inicial=data_inicial,
        data_final=data_final,
    )


def buscar_ptax_venda() -> pd.DataFrame:
    """
    Últimos 2 anos de PTAX venda (você usa isso também no bloco de tabelas).
    """
    return buscar_serie_sgs(
        SGS_SERIES["ptax_venda"],
        data_inicial=_dois_anos_atras_str(),
        data_final=_hoje_str(),
    )


def resumo_cambio(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    """
    Mesma lógica conceitual da função que você já usa em indicadores_macro_br.py:
    - último nível
    - variação no ano
    - nível e variação em 12m / 24m
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
        "ultimo": float(ultimo_valor),
        "ultima_data": ultima_data,
        "valor_12m": float(valor_12m) if valor_12m is not None else None,
        "data_12m": data_12m,
        "valor_24m": float(valor_24m) if valor_24m is not None else None,
        "data_24m": data_24m,
        "var_ano": float(var_ano) if var_ano is not None else None,
        "var_12m": float(var_12m) if var_12m is not None else None,
        "var_24m": float(var_24m) if var_24m is not None else None,
    }


# =============================================================================
# CURVA ANBIMA – CONTINUA IGUAL (PRÉ 2 ANOS / 5 ANOS)
# =============================================================================


def _carregar_curvas_anbima_full() -> Optional[pd.DataFrame]:
    if not CAMINHO_CURVAS_ANBIMA.exists():
        return None

    try:
        df = pd.read_csv(CAMINHO_CURVAS_ANBIMA)
    except Exception:
        return None

    cols_minimas = {"data_curva", "PRAZO_DU", "TAXA_PREF"}
    if not cols_minimas.issubset(df.columns):
        return None

    df["data_curva"] = pd.to_datetime(df["data_curva"])
    df = df.sort_values(["data_curva", "PRAZO_DU"])
    return df


def _obter_taxas_pref_2e5_anos() -> Tuple[Optional[date], Optional[float], Optional[float]]:
    df = _carregar_curvas_anbima_full()
    if df is None or df.empty:
        return None, None, None

    ultima_data = df["data_curva"].max()
    df_ult = df[df["data_curva"] == ultima_data].copy()

    prazo_2a = 252 * 2
    prazo_5a = 252 * 5

    taxa_2a = None
    taxa_5a = None

    linha_2a = df_ult.loc[df_ult["PRAZO_DU"] == prazo_2a]
    if not linha_2a.empty:
        taxa_2a = float(linha_2a["TAXA_PREF"].iloc[0])

    linha_5a = df_ult.loc[df_ult["PRAZO_DU"] == prazo_5a]
    if not linha_5a.empty:
        taxa_5a = float(linha_5a["TAXA_PREF"].iloc[0])

    return ultima_data.date(), taxa_2a, taxa_5a


# =============================================================================
# FUNÇÃO PRINCIPAL – CARREGA TUDO
# =============================================================================


def carregar_dados_curto_prazo_br() -> DadosCurtoPrazoBR:
    """
    Carrega todos os dados usados no bloco 'Indicadores de Curto Prazo – Brasil'.

    Agora:
      • Selic, CDI e PTAX vêm de dados reais da API do BCB (SGS).
      • Ibovespa e DI Futuro ainda são placeholders (vamos plugar B3 depois).
    """

    # ---------------------
    # Moeda & Juros
    # ---------------------

    # Defaults (caso alguma API quebre, não derruba o app)
    selic_meta = 10.75
    selic_12m = None
    selic_24m = None
    selic_ultima_decisao = None

    cdi_dia = None
    cdi_acumulado_mes = None
    cdi_variacao_dia = None
    cdi_no_ano = None
    cdi_em_12_meses = None

    ptax_fechamento = None
    ptax_variacao_dia = None
    ptax_nivel_12m = None
    ptax_nivel_24m = None
    ptax_var_12m = None
    ptax_var_24m = None

    # -------- SELIC META --------
    try:
        # Pega 2 anos para conseguir 12m e 24m
        df_selic = buscar_selic_meta_aa(
            data_inicial=_dois_anos_atras_str(),
            data_final=_hoje_str(),
        )
        if not df_selic.empty:
            df_selic = df_selic.sort_values("data").reset_index(drop=True)
            selic_meta = float(df_selic["valor"].iloc[-1])

            ultima_data = df_selic["data"].iloc[-1]
            corte_12m = ultima_data - relativedelta(years=1)
            corte_24m = ultima_data - relativedelta(years=2)

            df_24m = df_selic[df_selic["data"] >= corte_24m]
            df_12m = df_selic[df_selic["data"] >= corte_12m]

            if not df_24m.empty:
                selic_24m = float(df_24m["valor"].mean())
            if not df_12m.empty:
                selic_12m = float(df_12m["valor"].mean())

            # "Última decisão" = último nível diferente do atual (aprox. pré-Copom)
            df_antes = df_selic[df_selic["valor"] != selic_meta]
            if not df_antes.empty:
                selic_ultima_decisao = float(df_antes["valor"].iloc[-1])
            else:
                selic_ultima_decisao = selic_meta
    except Exception:
        # Deixa nos defaults
        pass

    # -------- CDI DIÁRIO --------
    try:
        df_cdi = buscar_cdi_diario()  # por padrão usa ~1 ano
        if not df_cdi.empty:
            df_cdi = df_cdi.sort_values("data").reset_index(drop=True)
            ult = df_cdi.iloc[-1]
            cdi_dia = float(ult["valor"])
            data_ult = ult["data"]

            if len(df_cdi) >= 2:
                penult = df_cdi.iloc[-2]
                cdi_variacao_dia = float(cdi_dia - penult["valor"])
            else:
                cdi_variacao_dia = 0.0

            ano_ref = data_ult.year
            mes_ref = data_ult.month

            # Mês atual
            df_mes = df_cdi[
                (df_cdi["data"].dt.year == ano_ref)
                & (df_cdi["data"].dt.month == mes_ref)
            ]
            if not df_mes.empty:
                fator_mes = (1 + df_mes["valor"] / 100.0).prod()
                cdi_acumulado_mes = (fator_mes - 1) * 100.0

            # Ano corrente
            df_ano = df_cdi[df_cdi["data"].dt.year == ano_ref]
            if not df_ano.empty:
                fator_ano = (1 + df_ano["valor"] / 100.0).prod()
                cdi_no_ano = (fator_ano - 1) * 100.0

            # Últimos 12 meses
            corte_12m_cdi = data_ult - relativedelta(years=1)
            df_12m_cdi = df_cdi[df_cdi["data"] >= corte_12m_cdi]
            if not df_12m_cdi.empty:
                fator_12m = (1 + df_12m_cdi["valor"] / 100.0).prod()
                cdi_em_12_meses = (fator_12m - 1) * 100.0
    except Exception:
        pass

    # -------- PTAX – DÓLAR --------
    try:
        df_ptax = buscar_ptax_venda()
        if not df_ptax.empty:
            df_ptax = df_ptax.sort_values("data").reset_index(drop=True)
            ult = df_ptax.iloc[-1]
            ptax_fechamento = float(ult["valor"])

            if len(df_ptax) >= 2:
                penult = df_ptax.iloc[-2]
                ptax_variacao_dia = (
                    (ptax_fechamento / float(penult["valor"]) - 1) * 100.0
                )
            else:
                ptax_variacao_dia = 0.0

            resumo_fx = resumo_cambio(df_ptax)
            ptax_nivel_12m = resumo_fx["valor_12m"]
            ptax_nivel_24m = resumo_fx["valor_24m"]
            ptax_var_12m = resumo_fx["var_12m"]
            ptax_var_24m = resumo_fx["var_24m"]
    except Exception:
        pass

    moeda_juros = MoedaJurosCurtoPrazo(
        selic_meta=selic_meta,
        cdi_acumulado_mes=cdi_acumulado_mes,
        cdi_variacao_dia=cdi_variacao_dia,
        ptax_fechamento=ptax_fechamento,
        ptax_variacao_dia=ptax_variacao_dia,
        selic_obs="",
        cdi_obs="",
        ptax_obs="",
        cdi_dia=cdi_dia,
        selic_12m=selic_12m,
        selic_24m=selic_24m,
        selic_ultima_decisao=selic_ultima_decisao,
        cdi_no_ano=cdi_no_ano,
        cdi_em_12_meses=cdi_em_12_meses,
        ptax_nivel_12m=ptax_nivel_12m,
        ptax_nivel_24m=ptax_nivel_24m,
        ptax_var_12m=ptax_var_12m,
        ptax_var_24m=ptax_var_24m,
    )

    # ---------------------
    # Ativos domésticos
    # ---------------------
    # Aqui eu mantenho:
    #   - Curva ANBIMA real (pré 2a / 5a)
    #   - Ibovespa e DI Futuro ainda como placeholders
    # No próximo passo a gente pluga B3 (yfinance / API) para Ibov, DI2/DI5 etc.

    data_curva, taxa_2a, taxa_5a = _obter_taxas_pref_2e5_anos()

    curva_obs = (
        "Curva ANBIMA consolidada (pré-fixada). "
        "Se algum vértice vier como '—', significa que o prazo exato "
        "não foi encontrado na última curva salva."
    )

    ativos_domesticos = AtivosDomesticosCurtoPrazo(
        ibov_nivel=128_500.0,   # TODO: plugar dado real (B3 / yfinance)
        ibov_var_dia=0.45,      # % no dia
        ibov_var_mes=1.95,
        ibov_var_ano=6.12,
        data_curva_anbima=data_curva,
        pre_2_anos=taxa_2a,
        pre_5_anos=taxa_5a,
        curva_obs=curva_obs,
        # TODO: DI Futuro 2a / 5a reais (usando di_futuro_b3.py)
        di_2_anos_taxa=12.80,
        di_2_anos_delta=-0.05,  # p.p. no dia
        di_5_anos_taxa=12.95,
        di_5_anos_delta=0.07,   # p.p. no dia
    )

    return DadosCurtoPrazoBR(
        moeda_juros=moeda_juros,
        ativos_domesticos=ativos_domesticos,
    )


def carregar_dados_curto_prazo_br_dict() -> Dict[str, Any]:
    dados = carregar_dados_curto_prazo_br()
    return {
        "moeda_juros": dados.moeda_juros,
        "ativos_domesticos": dados.ativos_domesticos,
    }
