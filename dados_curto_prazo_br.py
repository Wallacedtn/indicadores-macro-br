# dados_curto_prazo_br.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
CAMINHO_CURVAS_ANBIMA = BASE_DIR / "data" / "curvas_anbima" / "curvas_anbima_full.csv"


# ---------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------


@dataclass
class MoedaJurosCurtoPrazo:
    # --- Campos obrigatórios (sem default) ---
    selic_meta: float

    cdi_acumulado_mes: float          # CDI no mês (%)
    cdi_variacao_dia: float           # var do CDI (em p.p.) vs dia anterior

    ptax_fechamento: float            # PTAX atual
    ptax_variacao_dia: float          # var PTAX no dia (%)

    # --- A partir daqui, tudo com default ---
    selic_obs: str = ""
    cdi_obs: str = ""
    ptax_obs: str = ""

    # CDI diário
    cdi_dia: Optional[float] = None   # (% a.d.)

    # Selic – histórico e referência
    selic_12m: Optional[float] = None
    selic_24m: Optional[float] = None
    selic_ultima_decisao: Optional[float] = None  # Selic na última decisão do Copom

    # CDI – janelas
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


# ---------------------------------------------------------------------
# Curva ANBIMA (continua igual, usada só para ter referência de pré)
# ---------------------------------------------------------------------


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


# ---------------------------------------------------------------------
# Função principal – carrega tudo
# ---------------------------------------------------------------------


def carregar_dados_curto_prazo_br() -> DadosCurtoPrazoBR:
    """
    Carrega todos os dados usados no bloco 'Indicadores de Curto Prazo – Brasil'.

    Por enquanto:
      • Selic, CDI, PTAX, Ibovespa e DI Futuro ainda como placeholders,
        mas centralizados aqui para ficar fácil plugar dados reais depois.
    """

    # ---------------------
    # Moeda & Juros
    # ---------------------
    moeda_juros = MoedaJurosCurtoPrazo(
        selic_meta=10.75,
        cdi_acumulado_mes=0.55,
        cdi_variacao_dia=0.05,      # var do CDI (em p.p.) vs dia anterior
        ptax_fechamento=5.42,
        ptax_variacao_dia=0.32,     # var % no dia

        # Observações (podem ficar vazias)
        selic_obs="",
        cdi_obs="",
        ptax_obs="",

        # Complementares / histórico
        cdi_dia=0.05510,            # ~0,0551% a.d. (placeholder)
        selic_12m=11.25,
        selic_24m=13.75,
        selic_ultima_decisao=10.75,  # igual à meta atual => Δ 0,00 p.p. (neutro)
        cdi_no_ano=12.45,
        cdi_em_12_meses=13.88,
        ptax_nivel_12m=5.7597,
        ptax_nivel_24m=4.8717,
        ptax_var_12m=-7.36,
        ptax_var_24m=9.52,
    )

    # ---------------------
    # Ativos domésticos
    # ---------------------
    data_curva, taxa_2a, taxa_5a = _obter_taxas_pref_2e5_anos()

    curva_obs = (
        "Curva ANBIMA consolidada (pré-fixada). "
        "Se algum vértice vier como '—', significa que o prazo exato "
        "não foi encontrado na última curva salva."
    )

    ativos_domesticos = AtivosDomesticosCurtoPrazo(
        ibov_nivel=128_500.0,
        ibov_var_dia=0.45,        # % no dia
        ibov_var_mes=1.95,
        ibov_var_ano=6.12,
        data_curva_anbima=data_curva,
        pre_2_anos=taxa_2a,
        pre_5_anos=taxa_5a,
        curva_obs=curva_obs,
        # Placeholders para DI Futuro 2a / 5a
        di_2_anos_taxa=12.80,
        di_2_anos_delta=-0.05,    # p.p. no dia
        di_5_anos_taxa=12.95,
        di_5_anos_delta=0.07,     # p.p. no dia
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
