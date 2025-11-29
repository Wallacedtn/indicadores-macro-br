# dados_curto_prazo_br.py
# -*- coding: utf-8 -*-

import pandas as pd
import requests
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
from functools import lru_cache
from dateutil.relativedelta import relativedelta
def _to_float_scalar(val: Any) -> float:
    """
    Converte de forma segura um valor vindo de pandas para float nativo.

    - Se for Series (mesmo de 1 elemento), usa .iloc[0], como o pandas recomenda.
    - Se já for escalar (int, float, numpy scalar), só aplica float() normal.
    """
    if isinstance(val, pd.Series):
        # pega o primeiro elemento da Series
        return float(val.iloc[0])
    return float(val)

from di_futuro_b3 import (
    baixar_snapshot_di_futuro,   # snapshot do dia
    carregar_historico_di_futuro # di1_historico.csv
)  # DI Futuro B


# Caminho para o CSV de curvas ANBIMA (já usado no bloco de Curvas)
BASE_DIR = Path(__file__).parent
CAMINHO_CURVAS_ANBIMA = (
    BASE_DIR / "data" / "curvas_tesouro" / "curvas_anbima" / "curvas_anbima_full.csv"
)

# Diretório de cache para séries SGS usadas no curto prazo (Selic/CDI/PTAX)
SGS_DIR = BASE_DIR / "data" / "curto_prazo"
CAMINHO_SGS_SELIC = SGS_DIR / "selic_meta_aa.csv"
CAMINHO_SGS_CDI = SGS_DIR / "cdi_diario.csv"
CAMINHO_SGS_PTAX = SGS_DIR / "ptax_venda.csv"

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

    # NOVO: variação desde o início do ano (mesmo ticker / mesmo prazo)
    di_2_anos_delta_ano: Optional[float] = None
    di_5_anos_delta_ano: Optional[float] = None

    # NOVO: tickers que estão sendo usados em cada card
    di_2_anos_ticker: Optional[str] = None
    di_5_anos_ticker: Optional[str] = None

    # fonte da variação (intraday = API B3 / D-1 = histórico csv)
    di_2_anos_fonte_delta: Optional[str] = None
    di_5_anos_fonte_delta: Optional[str] = None


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

def _quatro_anos_atras_str() -> str:
    dt = date.today() - relativedelta(years=4)
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
    """
    Meta Selic (% a.a.), vinda da API SGS.

    Esta função é usada pelo script de atualização de cache
    (dados_curto_prazo_br.py / atualiza_dados_pesados.py).

    O comportamento "offline-first" para o SITE fica em
    carregar_dados_curto_prazo_br(), que lê os CSVs em data/curto_prazo.
    """
    return buscar_serie_sgs(
        SGS_SERIES["selic_meta_aa"],
        data_inicial=data_inicial,
        data_final=data_final,
    )


def buscar_cdi_diario(
    data_inicial: Optional[str] = None,
    data_final: Optional[str] = None,
) -> pd.DataFrame:
    """
    CDI diário (% a.d.), vindo da API SGS.

    Também é usado pelos scripts de atualização de cache.
    """
    return buscar_serie_sgs(
        SGS_SERIES["cdi_diario"],
        data_inicial=data_inicial,
        data_final=data_final,
    )


def buscar_ptax_venda(
    data_inicial: Optional[str] = None,
    data_final: Optional[str] = None,
) -> pd.DataFrame:
    """
    Dólar PTAX venda (R$/US$), vindo da API SGS.

    Por padrão, usamos os últimos 2 anos se datas não forem passadas.
    """
    if data_inicial is None:
        data_inicial = _dois_anos_atras_str()
    if data_final is None:
        data_final = _hoje_str()

    return buscar_serie_sgs(
        SGS_SERIES["ptax_venda"],
        data_inicial=data_inicial,
        data_final=data_final,
    )



def resumo_cambio(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    """
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
# CURVA ANBIMA – PRÉ 2 ANOS / 5 ANOS
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
# HELPERS – IBOVESPA E DI FUTURO
# =============================================================================

@lru_cache(maxsize=366)
def _obter_serie_ibov(hoje_str: str) -> Optional[pd.Series]:
    """Carrega a série histórica do Ibovespa a partir do CSV local.

    100% off-line:
      - Lê sempre o CSV (carregar_historico_ibovespa)
      - Não faz nenhuma requisição on-line
    """
    try:
        from ibovespa_ipea import carregar_historico_ibovespa

        df_hist = carregar_historico_ibovespa()
        if df_hist is None or df_hist.empty:
            return None

        df_hist = df_hist.copy()
        df_hist["data"] = pd.to_datetime(df_hist["data"])
        df_hist = df_hist.set_index("data").sort_index()

        serie = df_hist["valor"].astype(float)
        if serie.empty:
            return None

        return serie
    except Exception:
        # Qualquer erro -> não derruba o app, só retorna None
        return None

def _carregar_ibovespa_curto() -> Tuple[float, float, float, float]:
    """Resumo do Ibovespa para o bloco de curto prazo (nível, dia, mês, ano).

    Usa SOMENTE a série carregada do CSV local via _obter_serie_ibov.
    """
    hoje = date.today()
    chave_dia = hoje.strftime("%Y-%m-%d")

    serie = _obter_serie_ibov(chave_dia)
    if serie is None or serie.empty:
        return 0.0, 0.0, 0.0, 0.0

    serie = serie.dropna()
    if serie.empty:
        return 0.0, 0.0, 0.0, 0.0

    ultimo_dia = serie.index.max()
    nivel_atual = float(serie.loc[ultimo_dia])

    # Variação no dia (vs último fechamento anterior)
    serie_antes = serie[serie.index < ultimo_dia]
    if serie_antes.empty:
        var_dia_pct = 0.0
    else:
        nivel_anterior = float(serie_antes.iloc[-1])
        var_dia_pct = (nivel_atual / nivel_anterior - 1.0) * 100.0

    # Variação no mês
    serie_mes = serie[
        (serie.index.year == ultimo_dia.year)
        & (serie.index.month == ultimo_dia.month)
    ]
    if serie_mes.empty:
        var_mes_pct = 0.0
    else:
        nivel_inicio_mes = float(serie_mes.iloc[0])
        var_mes_pct = (nivel_atual / nivel_inicio_mes - 1.0) * 100.0

    # Variação no ano
    serie_ano = serie[serie.index.year == ultimo_dia.year]
    if serie_ano.empty:
        var_ano_pct = 0.0
    else:
        nivel_inicio_ano = float(serie_ano.iloc[0])
        var_ano_pct = (nivel_atual / nivel_inicio_ano - 1.0) * 100.0

    return nivel_atual, var_dia_pct, var_mes_pct, var_ano_pct


def montar_resumo_ibovespa_tabela() -> pd.DataFrame:
    """
    Monta um pequeno resumo de desempenho do Ibovespa
    para ser exibido em tabela no bloco de curto prazo.

    Retorna um DataFrame com colunas:
      - "Período"
      - "Nível base (pts)"
      - "Nível atual (pts)"
      - "Variação (%)"

    Usa a mesma série do helper _obter_serie_ibov, que por sua vez:
      - tenta atualizar o CSV via Ipeadata;
      - lê sempre o histórico local (ibovespa_ipea.carregar_historico_ibovespa);
      - cacheia por dia para não repetir trabalho.
    """
    try:
        chave_dia = date.today().strftime("%Y-%m-%d")
        serie = _obter_serie_ibov(chave_dia)

        if serie is None or serie.empty:
            raise RuntimeError("Série de Ibovespa vazia.")

        # Garante ordenação crescente por data
        close = serie.sort_index()

        # Nível atual (último fechamento)
        ultimo_close = _to_float_scalar(close.iloc[-1])
        data_ult = close.index[-1]

        # ---------- No ano ----------
        mask_ano = close.index.year == data_ult.year
        serie_ano = close[mask_ano]
        if not serie_ano.empty:
            base_ano = _to_float_scalar(serie_ano.iloc[0])
        else:
            base_ano = ultimo_close
        var_ano = (ultimo_close / base_ano - 1.0) * 100.0 if base_ano != 0 else 0.0

        # ---------- Em 12 meses ----------
        data_12m = data_ult - relativedelta(years=1)
        serie_12m = close[close.index <= data_12m]
        if not serie_12m.empty:
            base_12m = _to_float_scalar(serie_12m.iloc[-1])
        else:
            base_12m = ultimo_close
        var_12m = (ultimo_close / base_12m - 1.0) * 100.0 if base_12m != 0 else 0.0

        # ---------- Em 24 meses ----------
        data_24m = data_ult - relativedelta(years=2)
        serie_24m = close[close.index <= data_24m]
        if not serie_24m.empty:
            base_24m = _to_float_scalar(serie_24m.iloc[-1])
        else:
            base_24m = ultimo_close
        var_24m = (ultimo_close / base_24m - 1.0) * 100.0 if base_24m != 0 else 0.0

        dados_tabela = [
            {
                "Período": "No ano",
                "Nível base (pts)": base_ano,
                "Nível atual (pts)": ultimo_close,
                "Variação (%)": var_ano,
            },
            {
                "Período": "Em 12 meses",
                "Nível base (pts)": base_12m,
                "Nível atual (pts)": ultimo_close,
                "Variação (%)": var_12m,
            },
            {
                "Período": "Em 24 meses",
                "Nível base (pts)": base_24m,
                "Nível atual (pts)": ultimo_close,
                "Variação (%)": var_24m,
            },
        ]

        return pd.DataFrame(dados_tabela)

    except Exception:
        # Em caso de erro, devolve DF vazio para não derrubar o app
        return pd.DataFrame(
            columns=[
                "Período",
                "Nível base (pts)",
                "Nível atual (pts)",
                "Variação (%)",
            ]
        )


def _escolher_di_por_prazo(
    df: pd.DataFrame,
    anos_alvo: float,
    tolerancia: float = 0.75,
) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    """
    Escolhe o contrato DI1 cujo vencimento está mais perto de `anos_alvo`.

    - anos_alvo em anos (ex.: 2.0, 5.0)
    - devolve (ticker, taxa, delta_em_pontos_percentuais)
    """
    if df is None or df.empty:
        return None, None, None

    if "vencimento" not in df.columns or "taxa" not in df.columns:
        return None, None, None

    hoje = date.today()

    df = df.copy()

    # converte vencimento para datetime, mas derruba datas absurdas (ex.: 9999-12-31)
    venc_dt = pd.to_datetime(df["vencimento"], errors="coerce")

    # mantém só anos "plausíveis" para DI (ex.: entre 2000 e 2100)
    mask_anos_ok = (venc_dt.dt.year >= 2000) & (venc_dt.dt.year <= 2100)
    venc_dt = venc_dt.where(mask_anos_ok)

    df["vencimento"] = venc_dt.dt.date
    df = df.dropna(subset=["vencimento"])

    df["dias_ate_venc"] = df["vencimento"].apply(
        lambda d: (d - hoje).days if isinstance(d, date) else None
    )
    df = df.dropna(subset=["dias_ate_venc"])

    # converte para anos usando ~252 dias úteis
    df["anos_ate_venc"] = df["dias_ate_venc"].astype(float) / 252.0
    df["diff_anos"] = (df["anos_ate_venc"] - float(anos_alvo)).abs()

    # restringe à janela em torno do alvo; se não tiver, usa todos
    df_janela = df[df["diff_anos"] <= tolerancia]
    if df_janela.empty:
        df_janela = df

    # ordena pela proximidade do alvo e, se existir, por volume desc
    sort_cols = ["diff_anos"]
    ascending = [True]
    if "volume" in df_janela.columns:
        sort_cols.append("volume")
        ascending.append(False)

    df_janela = df_janela.sort_values(sort_cols, ascending=ascending)

    linha = df_janela.iloc[0]

    taxa_raw = linha.get("taxa")
    variacao_bps_raw = linha.get("variacao_bps")
    ticker = linha.get("ticker")

    taxa = float(taxa_raw) if pd.notnull(taxa_raw) else None

    if variacao_bps_raw is None or pd.isna(variacao_bps_raw):
        delta_pp = None
    else:
        try:
            delta_pp = float(variacao_bps_raw) / 100.0
        except Exception:
            delta_pp = None

    return ticker, taxa, delta_pp


def _delta_di_vs_d1(
    df_hist: pd.DataFrame,
    ticker: Optional[str],
    taxa_atual: Optional[float],
) -> Optional[float]:
    """
    Calcula o delta em p.p. vs D-1 a partir do histórico di1_historico.csv
    para um determinado ticker.
    """
    if df_hist is None or df_hist.empty or ticker is None or taxa_atual is None:
        return None

    df_tk = df_hist[df_hist["ticker"] == ticker].copy()
    if df_tk.empty:
        return None

    df_tk = df_tk.sort_values("data")

    if len(df_tk) < 2:
        return None

    taxa_d1_raw = df_tk["taxa"].iloc[-2]
    try:
        taxa_d1 = float(taxa_d1_raw)
    except Exception:
        return None

    # delta em p.p. (taxa atual - taxa do dia anterior)
    return taxa_atual - taxa_d1

def _delta_di_vs_inicio_ano(
    df_hist: pd.DataFrame,
    ticker: Optional[str],
    taxa_atual: Optional[float],
) -> Optional[float]:
    """
    Calcula o delta em p.p. vs INÍCIO DO ANO (primeiro pregão do ano)
    para um determinado ticker, usando o histórico di1_historico.csv.
    """
    if df_hist is None or df_hist.empty or ticker is None or taxa_atual is None:
        return None

    df_tk = df_hist[df_hist["ticker"] == ticker].copy()
    if df_tk.empty:
        return None

    # garante ordenação por data
    df_tk = df_tk.sort_values("data")

    # converte para datetime para filtrar por ano
    df_tk["data_dt"] = pd.to_datetime(df_tk["data"])
    ano_ref = datetime.today().year

    df_ano = df_tk[df_tk["data_dt"].dt.year == ano_ref]
    if df_ano.empty:
        return None

    # primeira taxa do ano para esse ticker
    taxa_ini_raw = df_ano.sort_values("data_dt")["taxa"].iloc[0]
    try:
        taxa_ini = float(taxa_ini_raw)
    except Exception:
        return None

    # delta em p.p. (taxa atual - taxa do início do ano)
    return taxa_atual - taxa_ini


def _carregar_di_futuro_2e5_anos() -> Tuple[
    Optional[str],
    Optional[float],
    Optional[float],
    str,
    Optional[str],
    Optional[float],
    Optional[float],
    str,
]:
    """
    Usa o snapshot DI Futuro B3 + histórico (csv) para aproximar
    as taxas de 2 anos e 5 anos e a variação:

      - Se a API da B3 trouxer `variacao_bps`, usamos como delta intraday.
      - Caso contrário, tentamos calcular delta vs D-1 a partir do histórico.
      - Se o snapshot do dia falhar, usamos o ÚLTIMO DIA disponível no histórico
        para nível e delta (fonte = 'D-1').

    Retorna:
      (ticker_2a, di_2a_taxa, di_2a_delta, fonte_2,
       ticker_5a, di_5a_taxa, di_5a_delta, fonte_5)
    """
    di_2_taxa: Optional[float] = None
    di_2_delta: Optional[float] = None
    di_5_taxa: Optional[float] = None
    di_5_delta: Optional[float] = None
    fonte_di2 = "none"
    fonte_di5 = "none"
    ticker_di2: Optional[str] = None
    ticker_di5: Optional[str] = None

    # 1) Snapshot do dia (tenta intraday)
    try:
        df = baixar_snapshot_di_futuro()
    except Exception:
        df = None

    if df is not None and not df.empty:
        ticker_di2, di_2_taxa, di_2_delta_intr = _escolher_di_por_prazo(
            df, anos_alvo=2.0
        )
        ticker_di5, di_5_taxa, di_5_delta_intr = _escolher_di_por_prazo(
            df, anos_alvo=5.0
        )

        if di_2_taxa is not None and di_2_delta_intr is not None:
            di_2_delta = di_2_delta_intr
            fonte_di2 = "intraday"

        if di_5_taxa is not None and di_5_delta_intr is not None:
            di_5_delta = di_5_delta_intr
            fonte_di5 = "intraday"

    # 2) Histórico (csv) – para delta D-1 e fallback de nível
    try:
        df_hist = carregar_historico_di_futuro()
    except Exception:
        df_hist = None

    if df_hist is not None and not df_hist.empty:
        # 2a) Se já temos taxa do snapshot, mas não temos delta intraday,
        #     calculamos delta vs D-1.
        if di_2_taxa is not None and (di_2_delta is None or fonte_di2 == "none"):
            delta_d1 = _delta_di_vs_d1(df_hist, ticker_di2, di_2_taxa)
            if delta_d1 is not None:
                di_2_delta = delta_d1
                fonte_di2 = "D-1"

        if di_5_taxa is not None and (di_5_delta is None or fonte_di5 == "none"):
            delta_d1 = _delta_di_vs_d1(df_hist, ticker_di5, di_5_taxa)
            if delta_d1 is not None:
                di_5_delta = delta_d1
                fonte_di5 = "D-1"

        # 2b) Fallback TOTAL: snapshot falhou (ou veio vazio) → usamos o
        #     último dia disponível no histórico para nível + delta.
        if ticker_di2 is None or di_2_taxa is None or ticker_di5 is None or di_5_taxa is None:
            # último dia com dados
            ultima_data_hist = df_hist["data"].max()
            df_ult = df_hist[df_hist["data"] == ultima_data_hist].copy()

            # 2 anos
            if ticker_di2 is None or di_2_taxa is None:
                tk2, taxa2, _ = _escolher_di_por_prazo(df_ult, anos_alvo=2.0)
                if ticker_di2 is None:
                    ticker_di2 = tk2
                if di_2_taxa is None:
                    di_2_taxa = taxa2

                if di_2_taxa is not None and ticker_di2 is not None:
                    delta_d1 = _delta_di_vs_d1(df_hist, ticker_di2, di_2_taxa)
                    if delta_d1 is not None:
                        di_2_delta = delta_d1
                        fonte_di2 = "D-1"

            # 5 anos
            if ticker_di5 is None or di_5_taxa is None:
                tk5, taxa5, _ = _escolher_di_por_prazo(df_ult, anos_alvo=5.0)
                if ticker_di5 is None:
                    ticker_di5 = tk5
                if di_5_taxa is None:
                    di_5_taxa = taxa5

                if di_5_taxa is not None and ticker_di5 is not None:
                    delta_d1 = _delta_di_vs_d1(df_hist, ticker_di5, di_5_taxa)
                    if delta_d1 is not None:
                        di_5_delta = delta_d1
                        fonte_di5 = "D-1"

    # 3) Sempre retorna alguma coisa (mesmo que seja tudo None)
    return (
        ticker_di2,
        di_2_taxa,
        di_2_delta,
        fonte_di2,
        ticker_di5,
        di_5_taxa,
        di_5_delta,
        fonte_di5,
    )

def atualizar_cache_curto_prazo() -> None:
    """
    Atualiza e salva em CSV as séries SGS usadas no bloco de curto prazo:
    - Selic Meta (% a.a.)  -> 4 anos de histórico (pra ter 12/24/36/48m)
    - CDI diário (% a.d.)  -> 2 anos de histórico
    - PTAX venda (R$/US$)  -> 2 anos de histórico
    """
    SGS_DIR.mkdir(parents=True, exist_ok=True)

    hoje = _hoje_str()
    dois_anos = _dois_anos_atras_str()
    quatro_anos = _quatro_anos_atras_str()

    # ---------- Selic Meta – 4 anos ----------
    df_selic = buscar_selic_meta_aa(
        data_inicial=quatro_anos,
        data_final=hoje,
    )
    if not df_selic.empty:
        df_selic.to_csv(
            CAMINHO_SGS_SELIC,
            index=False,
            date_format="%Y-%m-%d",
        )

    # ---------- CDI diário – 2 anos ----------
    df_cdi = buscar_cdi_diario(
        data_inicial=dois_anos,
        data_final=hoje,
    )
    if not df_cdi.empty:
        df_cdi.to_csv(
            CAMINHO_SGS_CDI,
            index=False,
            date_format="%Y-%m-%d",
        )

    # ---------- PTAX venda – 2 anos ----------
    # aqui NÃO passamos data_inicial/data_final,
    # porque a própria função buscar_ptax_venda já cuida disso
    df_ptax = buscar_ptax_venda()
    if not df_ptax.empty:
        df_ptax.to_csv(
            CAMINHO_SGS_PTAX,
            index=False,
            date_format="%Y-%m-%d",
        )

# =============================================================================
# FUNÇÃO PRINCIPAL – CARREGA TUDO
# =============================================================================


def carregar_dados_curto_prazo_br() -> DadosCurtoPrazoBR:
    """
    Carrega todos os dados usados no bloco 'Indicadores de Curto Prazo – Brasil'.

     Agora:
      • Selic, CDI e PTAX vêm de dados reais da API do BCB (SGS).
      • Ibovespa via histórico local (CSV do Ipeadata, atualizado automaticamente).
      • DI Futuro 2a / 5a via snapshot B3 (di_futuro_b3).
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
        if CAMINHO_SGS_SELIC.exists():
            df_selic = pd.read_csv(CAMINHO_SGS_SELIC)
            df_selic["data"] = pd.to_datetime(df_selic["data"])
        else:
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
        if CAMINHO_SGS_CDI.exists():
            df_cdi = pd.read_csv(CAMINHO_SGS_CDI)
            df_cdi["data"] = pd.to_datetime(df_cdi["data"])
        else:
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
        if CAMINHO_SGS_PTAX.exists():
            df_ptax = pd.read_csv(CAMINHO_SGS_PTAX)
            df_ptax["data"] = pd.to_datetime(df_ptax["data"])
        else:
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
    data_curva, taxa_2a, taxa_5a = _obter_taxas_pref_2e5_anos()

    # Ibovespa (nível + variações)
    ibov_nivel, ibov_var_dia, ibov_var_mes, ibov_var_ano = _carregar_ibovespa_curto()

    # DI Futuro ~2 anos e ~5 anos (B3)
    try:
        (
            ticker_di2,
            di_2_taxa,
            di_2_delta,
            fonte_di2,
            ticker_di5,
            di_5_taxa,
            di_5_delta,
            fonte_di5,
        ) = _carregar_di_futuro_2e5_anos()
    except Exception:
        ticker_di2 = ticker_di5 = None
        di_2_taxa = di_2_delta = di_5_taxa = di_5_delta = None
        fonte_di2 = fonte_di5 = "none"

    # NOVO: variação vs início do ano (mesmo ticker / mesmo prazo)
    di_2_delta_ano = None
    di_5_delta_ano = None
    try:
        df_hist_di = carregar_historico_di_futuro()
        di_2_delta_ano = _delta_di_vs_inicio_ano(
            df_hist_di, ticker_di2, di_2_taxa
        )
        di_5_delta_ano = _delta_di_vs_inicio_ano(
            df_hist_di, ticker_di5, di_5_taxa
        )
    except Exception:
        di_2_delta_ano = None
        di_5_delta_ano = None

    # Se tenho a taxa e não tenho variação, mostra seta "flat" (0,00 p.p.)
    if di_2_taxa is not None and di_2_delta is None:
        di_2_delta = 0.0
    if di_5_taxa is not None and di_5_delta is None:
        di_5_delta = 0.0

    curva_obs = (
        "Curva ANBIMA consolidada (pré-fixada). "
        "Se algum vértice vier como '—', significa que o prazo exato "
        "não foi encontrado na última curva salva."
    )

    ativos_domesticos = AtivosDomesticosCurtoPrazo(
        ibov_nivel=ibov_nivel,
        ibov_var_dia=ibov_var_dia,
        ibov_var_mes=ibov_var_mes,
        ibov_var_ano=ibov_var_ano,
        data_curva_anbima=data_curva,
        pre_2_anos=taxa_2a,
        pre_5_anos=taxa_5a,
        curva_obs=curva_obs,
        di_2_anos_taxa=di_2_taxa,
        di_2_anos_delta=di_2_delta,
        di_5_anos_taxa=di_5_taxa,
        di_5_anos_delta=di_5_delta,
        # NOVO: variação vs início do ano
        di_2_anos_delta_ano=di_2_delta_ano,
        di_5_anos_delta_ano=di_5_delta_ano,
        # infos auxiliares
        di_2_anos_ticker=ticker_di2,
        di_5_anos_ticker=ticker_di5,
        di_2_anos_fonte_delta=fonte_di2,
        di_5_anos_fonte_delta=fonte_di5,
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

if __name__ == "__main__":
    print("Atualizando cache de curto prazo (Selic, CDI, PTAX)...")
    atualizar_cache_curto_prazo()
    print("Pronto: cache atualizado em data/curto_prazo.")

