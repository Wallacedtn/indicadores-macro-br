from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging
import datetime as dt

import pandas as pd
import requests


logger = logging.getLogger(__name__)


# =============================================================================
# Dataclass principal – aqui vão morar IBC-Br, desemprego, dívida, etc.
# Por enquanto só vamos preencher IBC-Br.
# =============================================================================
@dataclass
class DadosMacroFiscalBr:
    # ----- Atividade -----
    ibcbr_nivel: Optional[float] = None           # nível atual (série SA)
    ibcbr_var_mom: Optional[float] = None         # var. m/m dessaz. (%)
    ibcbr_var_aa: Optional[float] = None          # var. a/a (%), série sem ajuste
    ibcbr_referencia: Optional[str] = None        # "mm/aaaa"
    ibcbr_var_3m_dessaz: Optional[float] = None   # var. 3m acumulada, série SA (%)

    # ----- Confiança / mercado de trabalho (placeholders por enquanto) -----
    confianca_industria: Optional[float] = None
    confianca_industria_delta: Optional[float] = None

    desemprego_pnad: Optional[float] = None
    desemprego_delta_pp_12m: Optional[float] = None

    # ----- Risco país -----
    cds_5y: Optional[float] = None               # pontos-base
    cds_5y_delta_pb_12m: Optional[float] = None  # variação em 12m

    # ----- Fiscal / setor externo -----
    divida_bruta_pct_pib: Optional[float] = None           # nível atual (% PIB)
    divida_bruta_delta_pp_12m: Optional[float] = None      # (agora) variação m/m em p.p.
    divida_bruta_pct_pib_12m_atras: Optional[float] = None # nível há 12m
    divida_bruta_pct_pib_24m_atras: Optional[float] = None # nível há 24m
    divida_bruta_referencia: Optional[str] = None          # "mm/aaaa"

    # ----- Resultado Primário – Governo Central (valores reais) -----
    primario_mes_real_bi: Optional[float] = None            # mês, R$ bi reais
    primario_mes_delta_real_bi_aa: Optional[float] = None   # delta vs mesmo mês a/a, R$ bi
    receita_real_var_aa_pct: Optional[float] = None         # aqui usamos como: var real a/a (%) do primário do mês
    despesa_real_var_aa_pct: Optional[float] = None         # reservado p/ futuro (receita/despesa)
    primario_ano_real_bi: Optional[float] = None            # aqui usamos como: saldo 12m real (R$ bi)
    primario_ano_real_bi_prev: Optional[float] = None       # saldo 12m real 12m atrás (R$ bi)



    balanca_12m_usd_bi: Optional[float] = None
    balanca_delta_usd_bi_12m: Optional[float] = None


# =============================================================================
# Helpers para BCB / SGS
# =============================================================================
def _baixar_serie_sgs_json(codigo: int, n_ultimos: int = 24) -> pd.DataFrame:
    """
    Baixa a série SGS em JSON e devolve apenas os N últimos registros.

    Usa o endpoint padrão:
      https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json
    e faz o "tail" no pandas.
    """
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"

    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(data)
    if df.empty:
        raise ValueError(f"Série SGS {codigo} retornou vazio.")

    # data vem em dd/mm/aaaa, valor vem como string com vírgula
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["valor"] = df["valor"].str.replace(",", ".", regex=False).astype(float)

    # ordena cronologicamente e pega só os N últimos
    df = df.sort_values("data").tail(n_ultimos).reset_index(drop=True)
    return df


def _carregar_ibcbr() -> tuple[
    Optional[float],
    Optional[float],
    Optional[str],
    Optional[float],
    Optional[float],
]:
    """
    IBC-Br:

    - nível atual + m/m  -> série COM ajuste sazonal (SGS 24364)
    - variação 3m (dessaz.) -> também na série SA
    - variação a/a -> série SEM ajuste sazonal (SGS 24363)
    """
    codigo_sa = 24364   # IBC-Br dessazonalizado
    codigo_nsa = 24363  # IBC-Br sem ajuste sazonal

    # --- Série dessazonalizada: nível + m/m + 3m ---
    try:
        df_sa = _baixar_serie_sgs_json(codigo_sa, n_ultimos=36)
    except Exception as exc:  # noqa: BLE001
        logger.error("Erro ao baixar IBC-Br SA (24364): %s", exc)
        return None, None, None, None, None

    if len(df_sa) < 2:
        return None, None, None, None, None

    df_sa = df_sa.sort_values("data").reset_index(drop=True)
    ultimo_sa = df_sa.iloc[-1]
    penultimo_sa = df_sa.iloc[-2]

    nivel_sa = float(ultimo_sa["valor"])
    var_mom = (nivel_sa / float(penultimo_sa["valor"]) - 1.0) * 100.0
    data_ref = ultimo_sa["data"]
    ref_str = data_ref.strftime("%m/%Y")

    # variação 3m dessaz. (acumulada nos últimos 3 dados mensais)
    var_3m: Optional[float]
    # tentamos achar o valor da série SA de 3 meses atrás (mesmo mês/ano)
    data_3m = data_ref - pd.DateOffset(months=3)
    mask_3m = (df_sa["data"].dt.year == data_3m.year) & (
        df_sa["data"].dt.month == data_3m.month
    )
    df_3m = df_sa.loc[mask_3m]
    if df_3m.empty:
        var_3m = None
    else:
        valor_3m = float(df_3m.iloc[-1]["valor"])
        var_3m = (nivel_sa / valor_3m - 1.0) * 100.0

    # --- Série sem ajuste: variação a/a ---
    try:
        df_nsa = _baixar_serie_sgs_json(codigo_nsa, n_ultimos=120)
    except Exception as exc:  # noqa: BLE001
        logger.error("Erro ao baixar IBC-Br sem ajuste (24363): %s", exc)
        return nivel_sa, var_mom, ref_str, None, var_3m

    df_nsa = df_nsa.sort_values("data").reset_index(drop=True)

    # valor atual na série sem ajuste (mesmo mês/ano da ref)
    mask_atual = (df_nsa["data"].dt.year == data_ref.year) & (
        df_nsa["data"].dt.month == data_ref.month
    )
    df_atual = df_nsa.loc[mask_atual]

    # valor do mesmo mês do ano anterior
    mask_aa = (df_nsa["data"].dt.year == data_ref.year - 1) & (
        df_nsa["data"].dt.month == data_ref.month
    )
    df_aa = df_nsa.loc[mask_aa]

    if df_atual.empty or df_aa.empty:
        var_aa = None
    else:
        valor_atual = float(df_atual.iloc[-1]["valor"])
        valor_aa = float(df_aa.iloc[-1]["valor"])
        var_aa = (valor_atual / valor_aa - 1.0) * 100.0

    return nivel_sa, var_mom, ref_str, var_aa, var_3m


def _carregar_divida_bruta() -> tuple[
    Optional[float], Optional[float], Optional[float], Optional[float], Optional[str]
]:
    """
    Dívida Bruta do Governo Geral (% do PIB):

    - nível (último dado disponível)
    - variação m/m em p.p.  (mês contra mês anterior)
    - nível há 12 meses
    - nível há 24 meses
    - referência 'mm/aaaa'
    """
    codigo_divida = 13762  # DBGG (% PIB)

    try:
        df = _baixar_serie_sgs_json(codigo_divida, n_ultimos=240)
    except Exception as exc:  # noqa: BLE001
        logger.error("Erro ao baixar Dívida Bruta GG (SGS %s): %s", codigo_divida, exc)
        return None, None, None, None, None

    if len(df) < 2:
        return None, None, None, None, None

    df = df.sort_values("data").reset_index(drop=True)

    ultimo = df.iloc[-1]
    penultimo = df.iloc[-2]

    data_ult = ultimo["data"]
    nivel = float(ultimo["valor"])
    ref_str = data_ult.strftime("%m/%Y")

    # variação m/m em p.p. (mês contra mês anterior)
    delta_mom = nivel - float(penultimo["valor"])

    ano_ref = data_ult.year
    mes_ref = data_ult.month

    # mesmo mês de 12 meses atrás
    mask_12m = (df["data"].dt.year == ano_ref - 1) & (df["data"].dt.month == mes_ref)
    df_12m = df.loc[mask_12m]

    # mesmo mês de 24 meses atrás
    mask_24m = (df["data"].dt.year == ano_ref - 2) & (df["data"].dt.month == mes_ref)
    df_24m = df.loc[mask_24m]

    nivel_12m = float(df_12m.iloc[-1]["valor"]) if not df_12m.empty else None
    nivel_24m = float(df_24m.iloc[-1]["valor"]) if not df_24m.empty else None

    return nivel, delta_mom, nivel_12m, nivel_24m, ref_str

from io import BytesIO
from typing import Optional, Tuple

import pandas as pd
import requests
import logging

logger = logging.getLogger(__name__)

URL_TESOURO_RESULTADO_PRIMARIO = (
    "https://series-temporais-externo-frontend.tesouro.gov.br/"
    "backend-series-temporais/rest/Public/SerieGrafico/Download/8055"
)


def _carregar_resultado_primario_real_ipea_style() -> Tuple[
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
]:
    """
    Versão simplificada: Resultado Primário do Governo Central em
    valores NOMINAIS (R$ bi), usando a série 10.04.1 do Tesouro.

    Retorna:
      0) primário do mês em R$ bi (corrente)
      1) delta em R$ bi vs mesmo mês do ano anterior
      2) variação nominal a/a (%) do primário do mês
      3) (reservado para futuro) -> None
      4) saldo 12m (R$ bi, nominal)
      5) saldo 12m 12m atrás (R$ bi, nominal)
    """
    # ---------------------------
    # 1) Baixa a série do Tesouro (CSV ; em latin-1)
    # ---------------------------
    try:
        resp = requests.get(URL_TESOURO_RESULTADO_PRIMARIO, timeout=30)
        resp.raise_for_status()
    except Exception:
        logger.exception(
            "Erro ao baixar série de resultado primário do Tesouro."
        )
        return (None, None, None, None, None, None)

    try:
        df_prim = pd.read_csv(
            BytesIO(resp.content),
            sep=";",
            decimal=",",
            encoding="latin-1",
            engine="python",
        )
    except Exception:
        logger.exception(
            "Erro ao ler arquivo de resultado primário (CSV Tesouro)."
        )
        return (None, None, None, None, None, None)

    if df_prim.empty or df_prim.shape[1] < 2:
        return (None, None, None, None, None, None)

    # normalmente 1ª coluna = data, 2ª = valor
    col_data = df_prim.columns[0]
    col_val = df_prim.columns[1]

    df_prim = (
        df_prim[[col_data, col_val]]
        .rename(columns={col_data: "data", col_val: "valor_milhoes"})
    )

    # tenta data com dia primeiro; se der tudo NaT, tenta outros formatos simples
    raw_data = df_prim["data"].astype(str).str.strip()

    data_parsed = pd.to_datetime(raw_data, dayfirst=True, errors="coerce")
    if data_parsed.isna().all():
        # tenta formato ISO (yyyy-mm-dd)
        data_parsed = pd.to_datetime(raw_data, errors="coerce")

    df_prim["data"] = data_parsed
    df_prim["valor_milhoes"] = pd.to_numeric(
        df_prim["valor_milhoes"], errors="coerce"
    )

    df_prim = df_prim.dropna(subset=["data", "valor_milhoes"]).sort_values(
        "data"
    )
    if df_prim.empty:
        return (None, None, None, None, None, None)

    # converte para R$ bi NOMINAIS
    df_prim["valor_bi"] = df_prim["valor_milhoes"] / 1000.0

    # ---------------------------
    # 2) Calcula métricas nominais
    # ---------------------------
    ult = df_prim.iloc[-1]
    data_ult = ult["data"]
    prim_mes_bi = float(ult["valor_bi"])

    # mesmo mês do ano anterior
    mask_aa = (
        (df_prim["data"].dt.month == data_ult.month)
        & (df_prim["data"].dt.year == data_ult.year - 1)
    )
    df_aa = df_prim.loc[mask_aa]

    if df_aa.empty:
        prim_mes_bi_aa = None
    else:
        prim_mes_bi_aa = float(df_aa["valor_bi"].iloc[-1])

    if prim_mes_bi_aa is not None and prim_mes_bi_aa != 0:
        delta_bi_aa = prim_mes_bi - prim_mes_bi_aa
        var_aa_pct = (prim_mes_bi / prim_mes_bi_aa - 1.0) * 100.0
    else:
        delta_bi_aa = None
        var_aa_pct = None

    # saldo 12m (rolling de 12 meses)
    df_prim["valor_bi_12m"] = df_prim["valor_bi"].rolling(window=12).sum()
    serie_12m = df_prim["valor_bi_12m"].dropna()

    if serie_12m.empty:
        prim_12m_bi = None
        prim_12m_bi_prev = None
    else:
        prim_12m_bi = float(serie_12m.iloc[-1])
        prim_12m_bi_prev = (
            float(serie_12m.iloc[-13]) if len(serie_12m) > 12 else None
        )

    # mapeia para os 6 campos do dataclass
    return (
        prim_mes_bi,        # primario_mes_real_bi  (na prática: mês, R$ bi NOMINAL)
        delta_bi_aa,        # primario_mes_delta_real_bi_aa  (delta R$ bi vs mesmo mês a/a)
        var_aa_pct,         # receita_real_var_aa_pct  (aqui: var nominal a/a do mês)
        None,               # despesa_real_var_aa_pct  (reservado p/ futuro)
        prim_12m_bi,        # primario_ano_real_bi -> saldo 12m (nominal)
        prim_12m_bi_prev,   # primario_ano_real_bi_prev -> saldo 12m 12m atrás
    )


# =============================================================================
# Função pública principal
# =============================================================================
def carregar_dados_macro_fiscal_br() -> DadosMacroFiscalBr:
    """
    Ponto único de acesso aos dados macro/fiscais.
    IBC-Br + Dívida Bruta GG + (futuro) Primário, CDS, etc.
    """
    ibc_nivel, ibc_var_mom, ibc_ref, ibc_var_aa, ibc_var_3m = _carregar_ibcbr()

    (
        div_nivel,
        div_delta_mom,
        div_12m,
        div_24m,
        div_ref,
    ) = _carregar_divida_bruta()

    # resultado primário
    (
        primario_mes_real_bi,
        primario_mes_delta_real_bi_aa,
        receita_real_var_aa_pct,
        despesa_real_var_aa_pct,
        primario_ano_real_bi,
        primario_ano_real_bi_prev,
    ) = _carregar_resultado_primario_real_ipea_style()

    return DadosMacroFiscalBr(
        # IBC-Br
        ibcbr_nivel=ibc_nivel,
        ibcbr_var_mom=ibc_var_mom,
        ibcbr_var_aa=ibc_var_aa,
        ibcbr_referencia=ibc_ref,
        ibcbr_var_3m_dessaz=ibc_var_3m,

        # Dívida Bruta GG
        divida_bruta_pct_pib=div_nivel,
        
        # apesar do nome *_12m, AGORA esse campo guarda Δ m/m em p.p.
        divida_bruta_delta_pp_12m=div_delta_mom,
        divida_bruta_pct_pib_12m_atras=div_12m,
        divida_bruta_pct_pib_24m_atras=div_24m,
        divida_bruta_referencia=div_ref,

        # Resultado Primário – Governo Central (mês, preços reais)
        primario_mes_real_bi=primario_mes_real_bi,
        primario_mes_delta_real_bi_aa=primario_mes_delta_real_bi_aa,
        receita_real_var_aa_pct=receita_real_var_aa_pct,
        despesa_real_var_aa_pct=despesa_real_var_aa_pct,
        primario_ano_real_bi=primario_ano_real_bi,
        primario_ano_real_bi_prev=primario_ano_real_bi_prev,
    )


