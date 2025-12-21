# indicadores_macro_br.py
# -*- coding: utf-8 -*-

import math
import streamlit_shadcn_ui as ui
import altair as alt
import requests
import pandas as pd
import numpy as np
import unicodedata
from datetime import datetime, date, timedelta
from datetime import timedelta as _td
from dateutil.relativedelta import relativedelta
import streamlit as st
from typing import Optional, Dict, List, Tuple
from functools import lru_cache
from pathlib import Path
from dados_macro_fiscal_br import carregar_dados_macro_fiscal_br
from di_futuro_b3 import carregar_historico_di_futuro
from caged_saldo_brasil import carregar_caged_saldo_csv, atualizar_caged_saldo_brasil_csv
from fgv_confianca import resumo_fgv_indice
import re
import html
import html as _html


from risco_brasil_spread_10y import (
    atualizar_spread_10y,
    carregar_risco_brasil_spread_10y,
    CSV_SPREAD,
)

from bloco_curto_prazo_br import (
    render_bloco_curto_prazo_br,
    metric_card,
    _inject_ion_css_curto_prazo,
    ICON_PERCENT,
    ICON_CHART,
    ICON_DOLLAR,
    _format_delta_br,
)

from dados_curto_prazo_br import carregar_dados_curto_prazo_br

from curvas_anbima import (
    montar_curva_anbima_hoje,
    montar_curva_anbima_variacoes as _montar_curva_anbima_variacoes_raw,
)

from di_futuro_b3 import carregar_historico_di_futuro
from ibovespa_ipea import carregar_historico_ibovespa

from bloco_curto_prazo_br import (
    render_bloco_curto_prazo_br,
    metric_card,
    _inject_ion_css_curto_prazo,
    ICON_PERCENT,
    ICON_CHART,
    ICON_DOLLAR,
)
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

@st.cache_data(ttl=60 * 5)
def montar_curva_anbima_variacoes_cached(anos: int) -> pd.DataFrame:
    """
    Versão cacheada da montar_curva_anbima_variacoes original.

    - anos: vértice da curva (1, 2, 3, ... 9)
    - retorna o mesmo DataFrame da função original
    """
    return _montar_curva_anbima_variacoes_raw(anos)


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

EMBI_CACHE_PATH = Path("data/embi_brasil.csv")


# FOCUS – endpoint definitivo (ExpectativasMercadoAnuais)
FOCUS_BASE_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/"
    "Expectativas/versao/v1/odata/ExpectativasMercadoAnuais"
)

FOCUS_TOP5_ANUAIS_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/"
    "Expectativas/versao/v1/odata/ExpectativasMercadoTop5Anuais"
)

# Endpoint para expectativas MENSais (IPCA, câmbio, etc.)
FOCUS_MENSAIS_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/"
    "Expectativas/versao/v1/odata/ExpectativaMercadoMensais"
)


# Tolerância para considerar variações "nulas" no Focus (em pontos percentuais)
FOCUS_DIFF_TOL = 0.01  # 0,01 = 1 basis point

# Diretórios principais de dados (iguais às 3 seções do site)
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

DATA_CURTO_PRAZO_DIR = DATA_DIR / "curto_prazo"
DATA_EXPECTATIVAS_DIR = DATA_DIR / "expectativas"
DATA_CURVAS_TESOURO_DIR = DATA_DIR / "curvas_tesouro"

# Arquivos de cache do Focus (ficam em data/expectativas/)
FOCUS_CACHE_DIR = DATA_EXPECTATIVAS_DIR
FOCUS_CACHE_FILE = FOCUS_CACHE_DIR / "focus_expectativas_anuais.csv"
FOCUS_TOP5_CACHE_FILE = FOCUS_CACHE_DIR / "focus_expectativas_top5_anuais.csv"
FOCUS_MENSAIS_CACHE_FILE = FOCUS_CACHE_DIR / "focus_expectativas_mensais.csv"

DATA_CURVAS_TESOURO_DIR = DATA_DIR / "curvas_tesouro"

# === Balança Comercial (CSV mensal em US$ milhões) ==========================
DATA_SETOR_EXTERNO_DIR = DATA_DIR / "setor_externo"
BALANCA_COMERCIAL_CSV = DATA_SETOR_EXTERNO_DIR / "balanca_comercial_mensal_usd.csv"

# Preços / Inflação
DATA_PRECOS_DIR = DATA_DIR / "precos"
IPCA_MENSAL_CSV = DATA_PRECOS_DIR / "ipca_mensal_ibge.csv"

DATA_ATIVIDADE_DIR = DATA_DIR / "atividade"
NUCI_CSV = DATA_ATIVIDADE_DIR / "nuci_capacidade.csv"
IBC_BR_CSV = DATA_ATIVIDADE_DIR / "ibcbr.csv"

# IBGE (CSV offline) – Coincidentes de atividade
PIM_CSV = DATA_ATIVIDADE_DIR / "pim_pf.csv"
PMS_CSV = DATA_ATIVIDADE_DIR / "pms.csv"
PMC_CSV = DATA_ATIVIDADE_DIR / "pmc.csv"

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

def _icon_mes_ref(mes_ref: Optional[str]) -> str:
    """
    Gera o HTML do 'badge' de mês (ex.: 10/25) no lugar do ícone padrão.
    Se não tiver mês, cai no ícone de porcentagem (padrão dos outros cards).
    """
    if not mes_ref:
        return ICON_PERCENT

    mes_ref = mes_ref.strip()
    return f"""
    <div class="metric-icon metric-icon-text">
        <span>{mes_ref}</span>
    </div>
    """

def _parse_mes_pt_abrev(s: str) -> pd.Timestamp:
    """
    Converte datas mensais comuns para Timestamp (dia=1).

    Aceita:
      - 'jan/2011', 'jan-2011'
      - '01/2011', '1/2011'
      - '2011-01', '2011/01'
      - '2011-01-01' (ou qualquer ISO parseável)
    """
    s = str(s).strip().lower()

    # tenta formatos ISO / parseáveis pelo pandas (ex.: 2011-01-01)
    try:
        dt = pd.to_datetime(s, errors="raise", dayfirst=False)
        if pd.notna(dt):
            return pd.Timestamp(year=int(dt.year), month=int(dt.month), day=1)
    except Exception:
        pass

    # normaliza separadores
    s2 = s.replace(".", "/").replace("-", "/")

    # 01/2011
    m1 = re.match(r"^(\d{1,2})\/(\d{4})$", s2)
    if m1:
        mm = int(m1.group(1))
        yy = int(m1.group(2))
        if 1 <= mm <= 12:
            return pd.Timestamp(year=yy, month=mm, day=1)
        return pd.NaT

    # 2011/01
    m2 = re.match(r"^(\d{4})\/(\d{1,2})$", s2)
    if m2:
        yy = int(m2.group(1))
        mm = int(m2.group(2))
        if 1 <= mm <= 12:
            return pd.Timestamp(year=yy, month=mm, day=1)
        return pd.NaT

    # jan/2011
    mm_map = {"jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,"jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12}
    try:
        m = mm_map.get(s2[:3])
        y = int(s2[-4:])
        if not m:
            return pd.NaT
        return pd.Timestamp(year=y, month=m, day=1)
    except Exception:
        return pd.NaT
        return pd.Timestamp(year=y, month=m, day=1)
    except Exception:
        return pd.NaT


def carregar_nuci_csv() -> pd.DataFrame:
    """
    Espera um CSV com colunas: periodo ; valor
    Ex.: jan/2011 ; 84,6
    """
    if not NUCI_CSV.exists() or NUCI_CSV.stat().st_size == 0:
        return pd.DataFrame(columns=["data", "valor"])

    df = pd.read_csv(NUCI_CSV, sep=";", encoding="latin1")
    # normaliza nomes
    cols = {c.strip().lower(): c for c in df.columns}
    col_periodo = cols.get("periodo") or cols.get("mês") or cols.get("mes") or list(df.columns)[0]
    col_valor = cols.get("valor") or list(df.columns)[1]

    df = df[[col_periodo, col_valor]].copy()
    df.columns = ["periodo", "valor"]

    df["data"] = df["periodo"].apply(_parse_mes_pt_abrev)
    df["valor"] = pd.to_numeric(df["valor"].astype(str).str.replace(",", "."), errors="coerce")
    df = df.dropna(subset=["data", "valor"]).sort_values("data").reset_index(drop=True)
    return df[["data", "valor"]]


def resumo_nuci() -> dict:
    """
    Retorna último NUCI (%), e delta m/m em p.p.
    """
    df = carregar_nuci_csv()
    if df.empty or len(df) < 2:
        return {"referencia": None, "nivel": None, "delta_pp": None}

    last = df.iloc[-1]
    prev = df.iloc[-2]

    nivel = float(last["valor"])
    delta_pp = float(last["valor"]) - float(prev["valor"])

    return {
        "referencia": _formata_mes(pd.to_datetime(last["data"])),
        "nivel": nivel,
        "delta_pp": delta_pp,
    }



# =============================================================================
# BANCO CENTRAL (SGS) – FUNÇÃO GENÉRICA COM CACHE + RETRY
# =============================================================================


@lru_cache(maxsize=32)
# =============================================================================
# ATIVIDADE (COINCIDENTES) – ENRIQUECIMENTO “GESTOR-LIKE” (CSV OFFLINE)
# =============================================================================

def _carregar_csv_ibge_atividade(path_csv: Path) -> pd.DataFrame:
    """Lê CSV de atividade IBGE no padrão: data,var_mom,acum_ano,acum_12m."""
    if (not path_csv.exists()) or path_csv.stat().st_size == 0:
        return pd.DataFrame(columns=["data", "var_mom", "acum_ano", "acum_12m"])

    df = pd.read_csv(path_csv)
    # normaliza colunas
    df.columns = [str(c).strip().lower() for c in df.columns]
    # aceita variações de nome
    col_data = "data" if "data" in df.columns else df.columns[0]
    col_mom = "var_mom" if "var_mom" in df.columns else ("var" if "var" in df.columns else None)
    if col_mom is None:
        # tenta achar coluna de variação mensal
        cand = [c for c in df.columns if "mom" in c or "mens" in c]
        col_mom = cand[0] if cand else None

    col_ytd = "acum_ano" if "acum_ano" in df.columns else None
    col_12m = "acum_12m" if "acum_12m" in df.columns else None

    keep = [c for c in [col_data, col_mom, col_ytd, col_12m] if c is not None]
    df = df[keep].copy()
    df.rename(columns={col_data: "data"}, inplace=True)
    if col_mom:
        df.rename(columns={col_mom: "var_mom"}, inplace=True)
    if col_ytd:
        df.rename(columns={col_ytd: "acum_ano"}, inplace=True)
    if col_12m:
        df.rename(columns={col_12m: "acum_12m"}, inplace=True)

    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    for c in ["var_mom", "acum_ano", "acum_12m"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df[c] = np.nan

    df = df.dropna(subset=["data"]).sort_values("data").reset_index(drop=True)
    return df


def _indice_sintetico_base_2022(df: pd.DataFrame) -> pd.DataFrame:
    """Gera um índice sintético (base 2022-01 = 100) a partir de var_mom (%)."""
    if df.empty or "var_mom" not in df.columns:
        return pd.DataFrame(columns=["data", "indice", "var_mom", "acum_ano", "acum_12m"])

    d = df.copy()
    d = d.dropna(subset=["data"]).sort_values("data").reset_index(drop=True)

    # corta em 2022+ para ficar alinhado ao seu racional (quartil 2022+)
    d = d[d["data"] >= pd.Timestamp("2022-01-01")].copy()
    if d.empty:
        return pd.DataFrame(columns=["data", "indice", "var_mom", "acum_ano", "acum_12m"])

    d["indice"] = (1.0 + (d["var_mom"].fillna(0.0) / 100.0)).cumprod() * 100.0
    return d


def _percentil_22plus(val: float, serie_22: pd.Series) -> float | None:
    """Percentil (0..100) do valor vs distribuição 2022+ (maior = mais forte)."""
    try:
        s = pd.to_numeric(serie_22, errors="coerce").dropna()
        if s.empty or val is None or (pd.isna(val)):
            return None
        # rank percentil: fração <= val
        return float((s <= float(val)).mean() * 100.0)
    except Exception:
        return None


def _quartil_label_from_pct_top(pct: float | None) -> str:
    """Converte percentil (0..100) em label onde 1º = TOP 25% (muito forte)."""
    if pct is None or pd.isna(pct):
        return "•"
    if pct >= 75:
        return "1º quartil (muito forte)"
    if pct >= 50:
        return "2º quartil (forte)"
    if pct >= 25:
        return "3º quartil (fraco)"
    return "4º quartil (muito fraco)"


def _fmt_pct(x: float | None, digits: int = 2, signed: bool = False) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "•"
    s = f"{x:.{digits}f}%"
    return ("+" + s) if signed and (x > 0) else s


def _fmt_pp(x: float | None, digits: int = 1, signed: bool = False) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "•"
    s = f"{x:.{digits}f} p.p."
    return ("+" + s) if signed and (x > 0) else s


def _fmt_num(x: float | None, digits: int = 1) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "•"
    return f"{x:.{digits}f}"


def _delta_serie_pct(df: pd.DataFrame, col_val: str, meses: int) -> float | None:
    """Δ % (variação relativa) em 'meses' meses: (v_t / v_{t-m} - 1)*100."""
    if df.empty or col_val not in df.columns:
        return None
    d = df.dropna(subset=[col_val]).copy()
    if len(d) <= meses:
        return None
    v_t = float(d.iloc[-1][col_val])
    v_m = float(d.iloc[-1 - meses][col_val])
    if v_m == 0:
        return None
    return (v_t / v_m - 1.0) * 100.0


def _delta_serie_pp(df: pd.DataFrame, col_val: str, meses: int) -> float | None:
    """Δ em pontos (p.p.) em 'meses' meses: v_t - v_{t-m}."""
    if df.empty or col_val not in df.columns:
        return None
    d = df.dropna(subset=[col_val]).copy()
    if len(d) <= meses:
        return None
    v_t = float(d.iloc[-1][col_val])
    v_m = float(d.iloc[-1 - meses][col_val])
    return (v_t - v_m)


def _carregar_ibcbr_csv_offline() -> pd.DataFrame:
    if (not IBC_BR_CSV.exists()) or IBC_BR_CSV.stat().st_size == 0:
        return pd.DataFrame(columns=["data", "valor"])
    df = pd.read_csv(IBC_BR_CSV)
    df.columns = [str(c).strip().lower() for c in df.columns]
    if "data" not in df.columns:
        df.rename(columns={df.columns[0]: "data"}, inplace=True)
    if "valor" not in df.columns:
        df.rename(columns={df.columns[1]: "valor"}, inplace=True)
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df.dropna(subset=["data", "valor"]).sort_values("data").reset_index(drop=True)
    return df


def _resumo_ibcbr_gestor_like() -> dict:
    """IBC-Br: nível (índice), Δ m/m, Δ 3m, YTD, 12m (tudo em % exceto nível)."""
    df = _carregar_ibcbr_csv_offline()
    if df.empty or len(df) < 2:
        return {"referencia": None, "nivel": None, "mm_pct": None, "d3_pct": None, "ytd_pct": None, "d12_pct": None, "pct22": None}

    df = df.copy()
    df["data"] = pd.to_datetime(df["data"])
    last = df.iloc[-1]
    prev = df.iloc[-2]

    nivel = float(last["valor"])
    mm_pct = (float(last["valor"]) / float(prev["valor"]) - 1.0) * 100.0 if float(prev["valor"]) != 0 else None
    d3_pct = _delta_serie_pct(df, "valor", 3)
    d12_pct = _delta_serie_pct(df, "valor", 12)

    # YTD: compara com último dado de dezembro do ano anterior
    ytd_pct = None
    try:
        ano = int(pd.to_datetime(last["data"]).year)
        ref_dec = df[(df["data"].dt.year == (ano - 1)) & (df["data"].dt.month == 12)]
        if not ref_dec.empty:
            v_dec = float(ref_dec.iloc[-1]["valor"])
            if v_dec != 0:
                ytd_pct = (nivel / v_dec - 1.0) * 100.0
    except Exception:
        ytd_pct = None

    # quartil 2022+: nível
    df22 = df[df["data"] >= pd.Timestamp("2022-01-01")].copy()
    pct22 = _percentil_22plus(nivel, df22["valor"]) if not df22.empty else None

    return {
        "referencia": _formata_mes(pd.to_datetime(last["data"])),
        "nivel": nivel,
        "mm_pct": mm_pct,
        "d3_pct": d3_pct,
        "ytd_pct": ytd_pct,
        "d12_pct": d12_pct,
        "pct22": pct22,
    }


def _resumo_nuci_gestor_like() -> dict:
    """NUCI: nível (%), Δ m/m, Δ 3m, YTD, 12m (em p.p.)."""
    df = carregar_nuci_csv()
    if df.empty or len(df) < 2:
        return {"referencia": None, "nivel": None, "mm_pp": None, "d3_pp": None, "ytd_pp": None, "d12_pp": None, "pct22": None}

    df = df.copy()
    last = df.iloc[-1]
    prev = df.iloc[-2]

    nivel = float(last["valor"])
    mm_pp = float(last["valor"]) - float(prev["valor"])
    d3_pp = _delta_serie_pp(df, "valor", 3)
    d12_pp = _delta_serie_pp(df, "valor", 12)

    ytd_pp = None
    try:
        ano = int(pd.to_datetime(last["data"]).year)
        ref_dec = df[(df["data"].dt.year == (ano - 1)) & (df["data"].dt.month == 12)]
        if not ref_dec.empty:
            v_dec = float(ref_dec.iloc[-1]["valor"])
            ytd_pp = (nivel - v_dec)
    except Exception:
        ytd_pp = None

    df22 = df[df["data"] >= pd.Timestamp("2022-01-01")].copy()
    pct22 = _percentil_22plus(nivel, df22["valor"]) if not df22.empty else None

    return {
        "referencia": _formata_mes(pd.to_datetime(last["data"])),
        "nivel": nivel,
        "mm_pp": mm_pp,
        "d3_pp": d3_pp,
        "ytd_pp": ytd_pp,
        "d12_pp": d12_pp,
        "pct22": pct22,
    }


def _resumo_ibge_gestor_like(nome: str, csv_path: Path) -> dict:
    """
    IBGE (PIM/PMS/PMC): usa CSV offline para:
      - Δ m/m (%): var_mom
      - No ano (%): acum_ano
      - 12m (%): acum_12m
      - Δ 3m (%): via índice sintético base 2022=100
      - Nível: índice sintético base 2022=100
      - Quartil 2022+: nível do índice sintético
    """
    df = _carregar_csv_ibge_atividade(csv_path)
    if df.empty:
        return {"referencia": None, "mm_pct": None, "d3_pct": None, "ytd_pct": None, "d12_pct": None, "nivel": None, "pct22": None}

    # referência do CSV é a última data disponível
    last_row = df.dropna(subset=["data"]).iloc[-1]
    ref = _formata_mes(pd.to_datetime(last_row["data"]))

    mm_pct = float(last_row["var_mom"]) if pd.notna(last_row["var_mom"]) else None
    ytd_pct = float(last_row["acum_ano"]) if pd.notna(last_row["acum_ano"]) else None
    d12_pct = float(last_row["acum_12m"]) if pd.notna(last_row["acum_12m"]) else None

    # índice sintético 2022+
    d22 = _indice_sintetico_base_2022(df)
    nivel = float(d22.iloc[-1]["indice"]) if not d22.empty else None
    d3_pct = _delta_serie_pct(d22, "indice", 3) if not d22.empty else None
    
    # Quartil baseado em Δ 12m (momentum) em vez de nível
    df_hist = df[df["data"] >= pd.Timestamp("2022-01-01")].copy()
    if not df_hist.empty and d12_pct is not None:
        pct22 = (df_hist["acum_12m"] <= d12_pct).sum() / len(df_hist) * 100
    else:
        pct22 = None

    return {"referencia": ref, "mm_pct": mm_pct, "d3_pct": d3_pct, "ytd_pct": ytd_pct, "d12_pct": d12_pct, "nivel": nivel, "pct22": pct22}

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
    """
    Meta Selic (% a.a.).

    Versão offline-first para o SITE:
    - Se existir o arquivo data/curto_prazo/selic_meta_aa.csv, usa esse CSV;
    - Se não existir ou estiver ruim, cai para a API SGS (como era antes).
    """
    # Caminho do CSV de Selic que o dados_curto_prazo_br.py salva
    base_dir = Path(__file__).parent
    caminho_csv = base_dir / "data" / "curto_prazo" / "selic_meta_aa.csv"

    # 1) Tentar usar o CSV local (modo offline)
    if caminho_csv.exists():
        try:
            df = pd.read_csv(caminho_csv)

            # Garante que a coluna de data está em datetime
            if "data" in df.columns:
                df["data"] = pd.to_datetime(df["data"], errors="coerce")

            # Opcional: ordena por data, só pra garantir
            df = df.sort_values("data").reset_index(drop=True)
            return df
        except Exception:
            # Se der problema para ler o CSV, cai pro modo online
            pass

    # 2) Fallback: busca na API SGS (comportamento antigo)
    return buscar_serie_sgs(
        SGS_SERIES["selic_meta_aa"],
        data_inicial=_quatro_anos_atras_str(),
        data_final=_hoje_str(),
    )


def buscar_cdi_diario() -> pd.DataFrame:
    """
    CDI diário (% a.d.).

    Versão offline-first para o SITE:
    - Se existir o arquivo data/curto_prazo/cdi_diario.csv, usa esse CSV;
    - Se não existir ou estiver ruim, cai para a API SGS (janela de 2 anos).
    """

    base_dir = Path(__file__).parent
    caminho_csv = base_dir / "data" / "curto_prazo" / "cdi_diario.csv"

    # 1) Tenta usar o CSV local (modo offline)
    if caminho_csv.exists():
        try:
            df = pd.read_csv(caminho_csv)

            # Garante tipos corretos
            if "data" in df.columns:
                df["data"] = pd.to_datetime(df["data"], errors="coerce")
            if "valor" in df.columns:
                df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

            df = (
                df.dropna(subset=["data", "valor"])
                  .sort_values("data")
                  .reset_index(drop=True)
            )
            return df
        except Exception:
            # Se der problema pra ler o CSV, cai pro modo online
            pass

    # 2) Fallback: busca na API SGS (comportamento antigo)
    df = buscar_serie_sgs(
        SGS_SERIES["cdi_diario"],
        data_inicial=_dois_anos_atras_str(),
        data_final=_hoje_str(),
    )

    # Garante o mesmo tratamento de tipos e ordenação
    if not df.empty:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        df = (
            df.dropna(subset=["data", "valor"])
              .sort_values("data")
              .reset_index(drop=True)
        )

    return df



def buscar_ptax_venda() -> pd.DataFrame:
    """
    Dólar PTAX - venda (R$/US$).

    Versão offline-first para o SITE:
    - Se existir o arquivo data/curto_prazo/ptax_venda.csv, usa esse CSV;
    - Se não existir ou der erro na leitura, cai para a API SGS (comportamento antigo).
    """
    base_dir = Path(__file__).parent
    caminho_csv = base_dir / "data" / "curto_prazo" / "ptax_venda.csv"

    # 1) Tenta usar o CSV salvo pelo dados_curto_prazo_br / atualiza_dados_pesados.py
    if caminho_csv.exists():
        try:
            df = pd.read_csv(caminho_csv)

            # garante tipos
            if "data" in df.columns:
                df["data"] = pd.to_datetime(df["data"], errors="coerce")
            if "valor" in df.columns:
                df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

            df = (
                df.dropna(subset=["data", "valor"])
                  .sort_values("data")
                  .reset_index(drop=True)
            )

            return df

        except Exception:
            # se der problema no CSV, volta pro modo online
            pass

    # 2) Fallback: busca direto na API SGS (como era antes)
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
    """
    IPCA - variação mensal (%).

    Versão offline-first para o SITE:
    - 1º tenta ler data/precos/ipca_mensal_ibge.csv (gerado pelo ipca_ibge.py);
    - se não existir ou estiver zoado, cai para a API SIDRA (last60), igual antes.
    """
    # 1) Tenta usar o CSV local (modo offline)
    try:
        if IPCA_MENSAL_CSV.exists():
            df = pd.read_csv(IPCA_MENSAL_CSV)

            # garante tipos corretos
            if "data" in df.columns:
                df["data"] = pd.to_datetime(df["data"], errors="coerce")
            if "valor" in df.columns:
                df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

            df = (
                df[["data", "valor"]]
                .dropna(subset=["data", "valor"])
                .sort_values("data")
                .drop_duplicates(subset=["data"], keep="last")
                .reset_index(drop=True)
            )

            if not df.empty:
                return df
    except Exception:
        # se der ruim na leitura do CSV, ignora e vai pro modo online
        pass

    # 2) Fallback: busca direto na API SIDRA (comportamento antigo)
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
# FOCUS – EXPECTATIVAS MENSAIS (para surpresa do IPCA mensal)
# =============================================================================

@lru_cache(maxsize=1)
def _carregar_focus_mensais_raw() -> pd.DataFrame:
    """
    Carrega o dataset de Expectativas de Mercado Mensais do BCB.

    - Se existir CSV em cache e estiver legível, usa o cache.
    - Caso contrário, baixa da API Olinda e salva um CSV novo.
    """
    # 1) Tenta usar o CSV em cache
    if FOCUS_MENSAIS_CACHE_FILE.exists():
        try:
            df_cache = pd.read_csv(FOCUS_MENSAIS_CACHE_FILE)

            if "Data" in df_cache.columns:
                df_cache["Data"] = pd.to_datetime(
                    df_cache["Data"], errors="coerce"
                )
            if "DataReferencia" in df_cache.columns:
                df_cache["DataReferencia"] = pd.to_datetime(
                    df_cache["DataReferencia"], errors="coerce"
                )

            return df_cache
        except Exception:
            # Se der erro ao ler o cache, ignora e baixa de novo
            pass

    # 2) Baixa da API OLINDA
    url = (
        f"{FOCUS_MENSAIS_URL}"
        "?$format=json"
        "&$top=50000"
    )

    try:
        resp = _get_with_retry(url)
        dados_json = resp.json()
        dados = dados_json.get("value", [])
    except Exception:
        return pd.DataFrame()

    if not dados:
        return pd.DataFrame()

    df = pd.DataFrame(dados)

    # Garante colunas de data
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    else:
        df["Data"] = pd.NaT

    if "DataReferencia" in df.columns:
        df["DataReferencia"] = pd.to_datetime(
            df["DataReferencia"], errors="coerce"
        )
    else:
        df["DataReferencia"] = pd.NaT

    # Normaliza nome do indicador pra facilitar filtro de IPCA
    df["indicador_norm"] = df["Indicador"].apply(_normalizar_str)
    if "IndicadorDetalhe" in df.columns:
        df["detalhe_norm"] = df["IndicadorDetalhe"].fillna("").apply(
            _normalizar_str
        )
    else:
        df["detalhe_norm"] = ""

    # 3) Salva CSV em cache pra próximas execuções
    try:
        FOCUS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(FOCUS_MENSAIS_CACHE_FILE, index=False)
    except Exception:
        # Se não conseguir salvar, só segue com o DF em memória
        pass

    return df


def buscar_focus_ipca_mensal_para_mes(
    ref_data: pd.Timestamp,
) -> Optional[float]:
    """
    Retorna a mediana MAIS RECENTE do Focus Mensal de IPCA
    para o mesmo mês/ano de `ref_data`.
    """
    if pd.isna(ref_data):
        return None

    df = _carregar_focus_mensais_raw().copy()
    if df.empty:
        return None

    df = df.dropna(subset=["Data", "DataReferencia"])
    if df.empty:
        return None

    # Filtra apenas IPCA "cheio"
    ind_norm = _normalizar_str("IPCA")
    col_ind = df["indicador_norm"]
    mask_ind = col_ind == ind_norm
    if not mask_ind.any():
        mask_ind = col_ind.str.contains(ind_norm, na=False)

    alvo = ref_data.to_period("M")
    df["mes_ref"] = df["DataReferencia"].dt.to_period("M")

    df_mes = df[mask_ind & (df["mes_ref"] == alvo)].copy()
    if df_mes.empty:
        return None

    df_mes = df_mes.sort_values("Data")
    med = df_mes.iloc[-1]["Mediana"]

    try:
        return float(med)
    except Exception:
        return None


def resumo_ipca_com_focus_mensal() -> Dict[str, Optional[float]]:
    """
    Junta:
      - IPCA mensal / acum ano / acum 12m (IBGE)
      - Mediana Focus mensal para esse mês
      - Surpresa: IPCA_real - Focus (em p.p.)
    """
    df_ipca = buscar_ipca_ibge()
    resumo = resumo_inflacao(df_ipca)

    if df_ipca is None or df_ipca.empty:
        return {
            "referencia": resumo.get("referencia"),
            "mensal": resumo.get("mensal"),
            "acum_ano": resumo.get("acum_ano"),
            "acum_12m": resumo.get("acum_12m"),
            "focus_mensal": None,
            "surpresa_mensal": None,
        }

    data_ref = df_ipca["data"].max()
    focus_mensal = buscar_focus_ipca_mensal_para_mes(data_ref)

    ipca_mensal = resumo.get("mensal")
    surpresa = None
    if (
        focus_mensal is not None
        and not pd.isna(focus_mensal)
        and ipca_mensal is not None
        and not pd.isna(ipca_mensal)
    ):
        # diferença em pontos percentuais
        surpresa = ipca_mensal - focus_mensal

    return {
        "referencia": resumo.get("referencia"),
        "mensal": ipca_mensal,
        "acum_ano": resumo.get("acum_ano"),
        "acum_12m": resumo.get("acum_12m"),
        "focus_mensal": focus_mensal,
        "surpresa_mensal": surpresa,
    }

def montar_tabela_focus_mensal_proximo_mes() -> Tuple[pd.DataFrame, str, str]:
    """
    Monta uma tabela com as medianas do Focus MENSAL
    para o próximo mês-calendário.

    Retorna:
      - df_show: DataFrame com colunas [Indicador, Mês de referência, Mediana Focus]
      - mes_txt: texto do mês de referência (ex.: "12/2025")
      - data_base_txt: data da última coleta utilizada (ex.: "21/11/2025")
    """
    df = _carregar_focus_mensais_raw().copy()
    if df.empty:
        return pd.DataFrame(), "sem mês disponível", "sem data disponível"

    # garante que temos as datas principais
    df = df.dropna(subset=["Data", "DataReferencia"])
    if df.empty:
        return pd.DataFrame(), "sem mês disponível", "sem data disponível"

    # -----------------------------
    # descobre o próximo mês calendário
    # -----------------------------
    hoje = date.today()
    primeiro_mes = hoje.replace(day=1)
    prox_mes = primeiro_mes + relativedelta(months=1)
    alvo_period = pd.Period(prox_mes, freq="M")

    df["mes_ref"] = df["DataReferencia"].dt.to_period("M")
    df_mes = df[df["mes_ref"] == alvo_period].copy()

    # se não tiver projeção pro próximo mês, tenta o mês atual
    if df_mes.empty:
        mes_atual_period = pd.Period(primeiro_mes, freq="M")
        df_mes = df[df["mes_ref"] == mes_atual_period].copy()
        alvo_period = mes_atual_period
        if df_mes.empty:
            return pd.DataFrame(), "sem mês disponível", "sem data disponível"

    # pega a mediana mais recente de cada indicador
    df_mes = df_mes.sort_values(["Indicador", "Data"])
    df_ult = df_mes.groupby("Indicador", as_index=False).tail(1)

    # -----------------------------
    # ORDEM E RÓTULOS – seguindo a lógica da tabela grande
    # -----------------------------
    # (substring que vem da API, rótulo exibido, é percentual?)
    configs: List[Tuple[str, str, bool]] = [
        ("IPCA",                         "IPCA (variação %)", True),
        ("Câmbio",                       "Câmbio (R\\$/US\\$)", False),
        ("IGP-M",                        "IGP-M (variação %)", True),
        ("IPCA Administrados",           "IPCA Administrados (variação %)", True),
        ("IPCA Alimentação no domicílio","IPCA Alimentação no domicílio (variação %)", True),
        ("IPCA Bens industrializados",   "IPCA Bens industrializados (variação %)", True),
        ("IPCA Livres",                  "IPCA Livres (variação %)", True),
        ("IPCA Serviços",                "IPCA Serviços (variação %)", True),
        ("Taxa de desocupação",          "Taxa de desocupação (%)", True),
    ]

    # mapa para achar posição na ordem
    ordem_map = {sub: idx for idx, (sub, _, _) in enumerate(configs)}

    def _achar_config(indic_api: str) -> Tuple[int, str, bool]:
        """Retorna (ordem, rótulo bonitinho, se é %) para um indicador da API."""
        for sub, rotulo, eh_pct in configs:
            if indic_api == sub:
                return ordem_map[sub], rotulo, eh_pct
        # se não estiver na lista, joga pro final, assume % e mantém o nome cru
        return len(configs), indic_api, True

    mes_txt = alvo_period.to_timestamp().strftime("%m/%Y")
    data_base = df_ult["Data"].max()
    if pd.notna(data_base):
        data_base_txt = pd.to_datetime(data_base).strftime("%d/%m/%Y")
    else:
        data_base_txt = "sem data disponível"

    linhas: List[Dict[str, str]] = []

    for _, row in df_ult.iterrows():
        indic_api = str(row["Indicador"])
        ordem, nome_exib, eh_percentual = _achar_config(indic_api)

        # formata a mediana com 2 casas
        try:
            mediana_val = float(row["Mediana"])
        except Exception:
            mediana_val = float("nan")

        if math.isnan(mediana_val):
            mediana_str = "-"
        else:
            mediana_str = f"{mediana_val:.2f}%"
            if not eh_percentual:
                mediana_str = f"{mediana_val:.2f}"

        linhas.append(
            {
                "ordem": ordem,
                "Indicador": nome_exib,
                "Mês de referência": mes_txt,
                "Mediana Focus": mediana_str,
            }
        )

    df_show = (
        pd.DataFrame(linhas)
        .sort_values(["ordem", "Indicador"])
        .drop(columns=["ordem"])
        .reset_index(drop=True)
    )

    return df_show, mes_txt, data_base_txt

def carregar_balanca_comercial_mensal_de_csv() -> pd.DataFrame:
    """
    Lê o CSV com saldo mensal da balança comercial em US$ milhões.

    Espera um arquivo em:
      data/setor_externo/balanca_comercial_mensal_usd.csv

    Colunas esperadas (nomes flexíveis):
      - data
      - saldo_usd_milhoes  (ou 'saldo' / 'valor')
    """
    try:
        df = pd.read_csv(BALANCA_COMERCIAL_CSV)
    except FileNotFoundError:
        return pd.DataFrame(columns=["data", "valor"])

    # normaliza nomes
    df.columns = [c.strip().lower() for c in df.columns]

    # coluna de data
    col_data = "data"
    if col_data not in df.columns:
        for cand in ("mes", "competencia"):
            if cand in df.columns:
                col_data = cand
                break

    # coluna de valor
    col_valor = "saldo_usd_milhoes"
    if col_valor not in df.columns:
        for cand in ("saldo", "valor"):
            if cand in df.columns:
                col_valor = cand
                break

    df["data"] = pd.to_datetime(df[col_data], errors="coerce")
    df["valor"] = pd.to_numeric(df[col_valor], errors="coerce")

    df = df[["data", "valor"]].dropna().sort_values("data").reset_index(drop=True)
    return df


def resumo_balanca_comercial_mensal() -> Dict[str, Optional[float]]:
    """
    A partir da série mensal (US$ milhões) devolve:

      - referencia        : 'mm/aaaa'
      - saldo_mes_bi      : saldo do mês (US$ bi)
      - var_mes_pct_aa    : % vs mesmo mês do ano anterior
      - acum_ano_bi       : acumulado no ano (US$ bi)
      - acum_ano_var_pct  : % acima/abaixo do mesmo período do ano anterior
    """
    df = carregar_balanca_comercial_mensal_de_csv()
    if df is None or df.empty:
        return {
            "referencia": None,
            "saldo_mes_bi": None,
            "var_mes_pct_aa": None,
            "acum_ano_bi": None,
            "acum_ano_var_pct": None,
        }

    df = df.sort_values("data").copy()
    df["periodo"] = df["data"].dt.to_period("M")

    # último mês disponível
    ultimo = df.iloc[-1]
    data_ref = ultimo["data"]
    periodo_ref = ultimo["periodo"]
    ref_str = data_ref.strftime("%m/%Y")

    saldo_mes_milhoes = float(ultimo["valor"])
    saldo_mes_bi = saldo_mes_milhoes / 1000.0

    # mesmo mês do ano anterior
    periodo_aa = periodo_ref - 12
    df_aa = df[df["periodo"] == periodo_aa]
    if df_aa.empty:
        var_mes_pct_aa = None
    else:
        valor_aa = float(df_aa.iloc[-1]["valor"])
        if valor_aa == 0:
            var_mes_pct_aa = None
        else:
            var_mes_pct_aa = (saldo_mes_milhoes / valor_aa - 1.0) * 100.0

    # acumulado no ano (até o mês de referência)
    ano_ref = data_ref.year
    mes_ref = data_ref.month

    df_ano = df[(df["data"].dt.year == ano_ref) & (df["data"].dt.month <= mes_ref)]
    acum_ano_milhoes = float(df_ano["valor"].sum()) if not df_ano.empty else None

    ano_aa = ano_ref - 1
    df_ano_aa = df[
        (df["data"].dt.year == ano_aa) & (df["data"].dt.month <= mes_ref)
    ]
    acum_ano_aa_milhoes = float(df_ano_aa["valor"].sum()) if not df_ano_aa.empty else None

    if acum_ano_milhoes is None:
        acum_ano_bi = None
        acum_ano_var_pct = None
    else:
        acum_ano_bi = acum_ano_milhoes / 1000.0
        if not acum_ano_aa_milhoes or acum_ano_aa_milhoes == 0:
            acum_ano_var_pct = None
        else:
            acum_ano_var_pct = (acum_ano_milhoes / acum_ano_aa_milhoes - 1.0) * 100.0

    return {
        "referencia": ref_str,
        "saldo_mes_bi": saldo_mes_bi,
        "var_mes_pct_aa": var_mes_pct_aa,
        "acum_ano_bi": acum_ano_bi,
        "acum_ano_var_pct": acum_ano_var_pct,
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



def _carregar_focus_raw() -> pd.DataFrame:
    """
    Carrega o dataset de Expectativas de Mercado Anuais (estatísticas).

    Primeiro tenta ler de um CSV local em cache
    (data/expectativas/focus_expectativas_anuais.csv).
    Se o arquivo não existir ou estiver ruim, baixa da API do BCB,
    processa e salva o CSV para usos futuros.
    """
    # 1) tentar ler do cache local (modo "offline")
    if FOCUS_CACHE_FILE.exists():
        try:
            df_cache = pd.read_csv(FOCUS_CACHE_FILE)
            if "Data" in df_cache.columns:
                df_cache["Data"] = pd.to_datetime(
                    df_cache["Data"], errors="coerce"
                )
            return df_cache
        except Exception:
            # se o CSV estiver corrompido, ignora e baixa de novo
            pass

    # 2) se não tiver cache, baixa da API
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

    # 3) salvar no cache para os próximos runs ficarem rápidos/offline
    try:
        FOCUS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(FOCUS_CACHE_FILE, index=False)
    except Exception:
        # erro ao salvar cache não deve quebrar o app
        pass

    return df

# ----------------------------
# Ipeadata – Novo CAGED (saldo Brasil)
# ----------------------------
IPEADATA_BASE_URL = "http://ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{codigo}')"
SERIE_CAGED_SALDO_BR = "CAGED12_SALDON12"

def _carregar_caged_saldo_df(max_anos: int = 10) -> pd.DataFrame:
    """
    Versão offline-first do CAGED:

    1) Tenta usar o CSV data/mercado_trabalho/caged_saldo_brasil.csv,
       gerado pelo caged_saldo_brasil.py + atualiza_dados_pesados.py.
    2) Se não existir ou der erro, cai para um fallback online
       (atualizar_caged_saldo_brasil_csv), atualiza o CSV e usa o resultado.
    """
    # 1) tenta CSV
    df = carregar_caged_saldo_csv(max_anos=max_anos)
    if df is not None and not df.empty:
        return df

    # 2) fallback: baixa da API e já atualiza o CSV
    df_online = atualizar_caged_saldo_brasil_csv(max_anos=max_anos)

    if "data" in df_online.columns:
        df_online["data"] = pd.to_datetime(df_online["data"], errors="coerce")
    if "valor" in df_online.columns:
        df_online["valor"] = pd.to_numeric(df_online["valor"], errors="coerce")

    df_online = (
        df_online.dropna(subset=["data", "valor"])
        .sort_values("data")
        .reset_index(drop=True)
    )

    if not df_online.empty:
        corte = pd.Timestamp.today() - pd.DateOffset(years=max_anos)
        df_online = df_online[df_online["data"] >= corte].reset_index(drop=True)

    return df_online


def resumo_caged_saldo_novo() -> dict:
    """
    Monta um resumo simples para o card do CAGED:
      - saldo_atual       -> último valor (vagas)
      - saldo_12m         -> valor 12 meses atrás
      - saldo_24m         -> valor 24 meses atrás
      - delta_12m         -> diferença (atual - 12m atrás), em vagas
      - referencia        -> "mm/aaaa" do último dado
      - media_mes_5anos   -> média dos últimos 5 anos do MESMO mês (ex.: todos os outubros)
    """
    try:
        df = _carregar_caged_saldo_df()
    except Exception:
        return {
            "saldo_atual": None,
            "saldo_12m": None,
            "saldo_24m": None,
            "delta_12m": None,
            "referencia": None,
            "media_mes_5anos": None,
        }

    if df.empty:
        return {
            "saldo_atual": None,
            "saldo_12m": None,
            "saldo_24m": None,
            "delta_12m": None,
            "referencia": None,
            "media_mes_5anos": None,
        }

    # último dado disponível na série
    ult = df.iloc[-1]
    data_ref = ult["data"]             # ex.: 2025-10-01
    saldo_atual = float(ult["valor"])  # pessoas

    idx_ult = df.index[-1]

    saldo_12m = float(df.iloc[idx_ult - 12]["valor"]) if idx_ult >= 12 else None
    saldo_24m = float(df.iloc[idx_ult - 24]["valor"]) if idx_ult >= 24 else None

    delta_12m = saldo_atual - saldo_12m if saldo_12m is not None else None

    ref_str = data_ref.strftime("%m/%Y")   # "10/2025"

    # -------- média dos últimos 5 anos do MESMO mês --------
    ano_ref = data_ref.year
    mes_ref = data_ref.month

    mask_5anos = (
        (df["data"].dt.month == mes_ref)
        & (df["data"].dt.year >= ano_ref - 5)
        & (df["data"].dt.year < ano_ref)   # só anos ANTERIORES
    )
    df_5anos = df.loc[mask_5anos]

    media_mes_5anos = float(df_5anos["valor"].mean()) if not df_5anos.empty else None

    return {
        "saldo_atual": saldo_atual,
        "saldo_12m": saldo_12m,
        "saldo_24m": saldo_24m,
        "delta_12m": delta_12m,
        "referencia": ref_str,
        "media_mes_5anos": media_mes_5anos,
    }


@lru_cache(maxsize=1)
def _carregar_focus_top5_raw() -> pd.DataFrame:
    """
    Carrega o dataset de Expectativas Anuais Top5.

    Primeiro tenta ler de um CSV local em cache
    (data/expectativas/focus_expectativas_top5_anuais.csv).
    Se não existir, baixa da API, processa e salva.
    """
    # 1) tentar usar cache local
    if FOCUS_TOP5_CACHE_FILE.exists():
        try:
            df_cache = pd.read_csv(FOCUS_TOP5_CACHE_FILE)
            if "Data" in df_cache.columns:
                df_cache["Data"] = pd.to_datetime(
                    df_cache["Data"], errors="coerce"
                )
            return df_cache
        except Exception:
            pass

    # 2) baixa da API se não tiver cache
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

    # se um dia tiver IndicadorDetalhe aqui também, tratamos igual ao outro
    if "IndicadorDetalhe" in df.columns:
        df["detalhe_norm"] = (
            df["IndicadorDetalhe"]
            .fillna("")
            .apply(_normalizar_str)
        )
    else:
        df["detalhe_norm"] = ""

    # 3) salvar no cache local
    try:
        FOCUS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(FOCUS_TOP5_CACHE_FILE, index=False)
    except Exception:
        pass

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
    Calcula um resumo semanal para a mediana do Focus de um indicador/ano,
    copiando a metodologia do PDF:

    - "hoje":     última mediana (arredondada em 2 casas)
    - "semana_4": valor de 4 semanas atrás
    - "comp":     texto '▲ (3)', '▼ (1)', '= (2)', etc.
    """
    df = _carregar_focus_raw().copy()
    if df.empty:
        return {}

    # 1) filtra pelo ano
    mask = df["ano_ref"] == ano_desejado

    # 2) filtra pelo indicador (IPCA, PIB, Selic, câmbio...)
    ind_norm = _normalizar_str(indicador_substr)
    col_ind = df["indicador_norm"]
    mask_ind = col_ind == ind_norm
    if not mask_ind.any():
        mask_ind = col_ind.str.contains(ind_norm, na=False)
    mask &= mask_ind

    # 3) filtra pelo detalhe, se houver (ex.: "Top 5", etc.)
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

    # 4) datas válidas
    df_f["Data"] = pd.to_datetime(df_f["Data"], errors="coerce")
    df_f = df_f.dropna(subset=["Data"])
    if df_f.empty:
        return {}

    # 5) semana Focus = semana que termina na sexta (W-FRI)
    df_f["semana_focus"] = df_f["Data"].dt.to_period("W-FRI")

    # 6) dentro de cada semana, pega o ÚLTIMO valor
    df_sem = (
        df_f.sort_values("Data")
        .groupby("semana_focus", as_index=False)
        .last()
    )
    if df_sem.empty:
        return {}

    # 7) valores e diferença na MESMA base do PDF (2 casas decimais)
    df_sem["Mediana_float"] = df_sem["Mediana"].astype(float)
    df_sem["Mediana_round"] = df_sem["Mediana_float"].round(2)
    df_sem["Diff_vs_ant"] = df_sem["Mediana_round"].diff()

    def _classificar_mov(diff: float) -> str:
        """Replica a lógica do Focus:
        - diff > 0  => ▲
        - diff < 0  => ▼
        - diff == 0 => =
        """
        if pd.isna(diff):
            return "="
        if diff > 0:
            return "▲"
        if diff < 0:
            return "▼"
        return "="

    df_sem["Seta"] = df_sem["Diff_vs_ant"].apply(_classificar_mov)

    # 8) calcula o streak (quantas semanas seguidas nesse comportamento)
    setas = df_sem["Seta"].tolist()
    streaks = []
    ultimo = None
    cont = 0
    for s in setas:
        if s == ultimo:
            cont += 1
        else:
            ultimo = s
            cont = 1
        streaks.append(cont)

    df_sem["Streak"] = streaks

    n = len(df_sem)
    if n == 0:
        return {}

    valores = df_sem["Mediana_round"].tolist()
    val_hoje = valores[-1]
    val_4 = valores[-5] if n >= 5 else None

    comp_txt = "-"
    if n >= 2:
        seta_hoje = df_sem["Seta"].iloc[-1]
        streak_hoje = int(df_sem["Streak"].iloc[-1])
        comp_txt = f"{seta_hoje} ({streak_hoje})"

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
        ("IPCA (a.a.)",                 "ipca",       None, True),
        ("PIB Total (var.% a.a.)",      "pib total",  None, True),
        ("Selic (a.a.)",                "selic",      None, True),
        ("Câmbio (R\\$/US\\$)",             "cambio",     None, False),
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
                "Data ref.": data_ult.strftime("%d/%m/%Y"),
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
                "Data ref.": "-",
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
            "Data ref.",
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
    """
    Monta a tabela de câmbio – Dólar PTAX (venda) para o bloco de curto prazo.

    - Usa buscar_ptax_venda() (que já está offline-first via CSV).
    - Mostra "Data ref." (última data usada).
    - Nível há 12m / 24m vêm só com valor, sem data entre parênteses.
    """
    linhas: List[Dict[str, str]] = []

    try:
        df = buscar_ptax_venda()
        r = resumo_cambio(df)

        if r["ultimo"] is not None:
            # Data de referência (última observação)
            ultima_data_str = r["ultima_data"].strftime("%d/%m/%Y")
            nivel_atual = f"R$ {r['ultimo']:.4f}"

            # Níveis de 12m e 24m: só valor
            if r["valor_12m"] is not None:
                nivel_12m = f"R$ {r['valor_12m']:.4f}"
            else:
                nivel_12m = "-"

            if r["valor_24m"] is not None:
                nivel_24m = f"R$ {r['valor_24m']:.4f}"
            else:
                nivel_24m = "-"

            # Variações
            var_mes = f"{r['var_mes']:+.2f}%" if r["var_mes"] is not None else "-"
            var_ano = f"{r['var_ano']:+.2f}%" if r["var_ano"] is not None else "-"
            var_12m = f"{r['var_12m']:+.2f}%" if r["var_12m"] is not None else "-"
            var_24m = f"{r['var_24m']:+.2f}%" if r["var_24m"] is not None else "-"
        else:
            ultima_data_str = "-"
            nivel_atual = "sem dados"
            nivel_12m = "-"
            nivel_24m = "-"
            var_mes = "-"
            var_ano = "-"
            var_12m = "-"
            var_24m = "-"

        linhas.append(
            {
                "Indicador": "Dólar PTAX - venda",
                "Data ref.": ultima_data_str,
                "Nível atual": nivel_atual,
                "Nível há 12m": nivel_12m,
                "Nível há 24m": nivel_24m,
                "Var. mês": var_mes,
                "Var. ano": var_ano,
                "Var. 12m": var_12m,
                "Var. 24m": var_24m,
                "Fonte": "BCB / SGS (10813)",
            }
        )

    except Exception as e:
        linhas.append(
            {
                "Indicador": "Dólar PTAX - venda",
                "Data ref.": "-",
                "Nível atual": f"Erro: {e}",
                "Nível há 12m": "-",
                "Nível há 24m": "-",
                "Var. mês": "-",
                "Var. ano": "-",
                "Var. 12m": "-",
                "Var. 24m": "-",
                "Fonte": "BCB / SGS (10813)",
            }
        )

    df = pd.DataFrame(linhas)
    ordem_colunas = [
        "Indicador",
        "Data ref.",
        "Nível atual",
        "Nível há 12m",
        "Nível há 24m",
        "Var. mês",
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

def obter_historico_ibovespa_inteligente() -> pd.DataFrame:
    """
    Para o SITE:

    - Usa SOMENTE o histórico local salvo em CSV.
    - Quem atualiza esse CSV é o script ibovespa_ipea.py,
      rodando 1x por dia (por exemplo, de madrugada).

    Assim, o app fica leve e não depende do humor do Ipeadata.
    """
    return carregar_historico_ibovespa()



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

        try:
            # 1) Tenta atualizar histórico (API Ipeadata) sempre que o app roda
            #    (sem cache diário).
            df_hist = obter_historico_ibovespa_inteligente()

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

        fonte = "Ipeadata (GM366_IBVSP366)"

        linhas.append(
            {
                "Indicador": "Ibovespa - fechamento",
                "Data ref.": data_str,
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
                "Data ref.": "-",
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
        "Data ref.",
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

    def _fmt_pts(x, signed=False):
        if x is None:
            return "•"
        try:
            v = float(x)
        except Exception:
            return "•"
        s = f"{v:+.1f}" if signed else f"{v:.1f}"
        return f"{s.replace('.', ',')} pts"

    # FGV IBRE – Antecedentes (índices de confiança)
    fgv_map = [
        ("ICC",  "Confiança do Consumidor (ICC)"),
        ("ICI",  "Confiança da Indústria (ICI)"),
        ("ICS",  "Confiança de Serviços (ICS)"),
        ("ICOM", "Confiança do Comércio (ICOM)"),
        ("ICST", "Confiança da Construção (ICST)"),
        ("ICE",  "Confiança Empresarial (ICE)"),
    ]

    for sigla, nome in fgv_map:
        r = resumo_fgv_indice(sigla)

        # se não tiver dado, não adiciona linha
        if r.get("referencia", "-") == "-" or r.get("nivel", None) is None:
            continue

        linhas.append(
            {
                "Indicador": f"{nome} – nível",
                "Classificação": "🟢 Antecedente",
                "Mês ref.": r["referencia"],
                "Var. mensal": _fmt_pts(r.get("delta_pts"), signed=True),
                "Acum. no ano": "•",
                "Acum. 12 meses": _fmt_pts(r["nivel"]),
                "Fonte": f"FGV / {sigla} (Portal IBRE – FGV)",
            }
        )

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

# NUCI (Capacidade Instalada) – COINCIDENTE
    try:
        df_nuci = carregar_nuci_csv()  # já existe no seu arquivo
        if df_nuci is not None and not df_nuci.empty:
            df_nuci = df_nuci.sort_values("data")
            v_last = float(df_nuci["valor"].iloc[-1])
            ref = df_nuci["data"].iloc[-1].strftime("%m/%Y")

            # variação m/m em p.p.
            if len(df_nuci) >= 2:
                v_prev = float(df_nuci["valor"].iloc[-2])
                delta_pp = v_last - v_prev
                delta_txt = f"{delta_pp:+.1f} p.p."
            else:
                delta_txt = "•"

            linhas.append({
                "Indicador": "NUCI – utilização da capacidade",
                "Classificação": "🟡 Coincidente",
                "Mês ref.": ref,
                "Var. mensal": delta_txt,     # não é %, é p.p.
                "Acum. no ano": "•",
                "Acum. 12 meses": f"{v_last:.1f}%",
                "Fonte": "CNI (NUCI) – via CSV local",
            })
    except Exception as e:
        linhas.append({
            "Indicador": "NUCI – utilização da capacidade",
            "Classificação": "🟡 Coincidente",
            "Mês ref.": "-",
            "Var. mensal": f"Erro: {e}",
            "Acum. no ano": "-",
            "Acum. 12 meses": "-",
            "Fonte": "CNI (NUCI) – via CSV local",
        })



    # -------------------------------------------------------------------------
    # IBC-Br – COINCIDENTE
    # (1) tenta CSV offline-first em data/atividade/ibcbr.csv
    # (2) se não existir, você pode decidir cair pra SGS ou deixar vazio
    # -------------------------------------------------------------------------
    def _baixar_serie_sgs_json(codigo: int, n_ultimos: int = 36) -> pd.DataFrame:
        import requests
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        dados = r.json() or []
        df = pd.DataFrame(dados)
        if df.empty:
            raise RuntimeError(f"Série {codigo}: API retornou vazio.")
        df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
        df["valor"] = pd.to_numeric(df["valor"].astype(str).str.replace(",", "."), errors="coerce")
        df = df.dropna(subset=["data", "valor"]).sort_values("data").reset_index(drop=True)
        if n_ultimos > 0:
            df = df.tail(n_ultimos)
        return df

    def _carregar_ibcbr_offline() -> pd.DataFrame:
        if not IBC_BR_CSV.exists() or IBC_BR_CSV.stat().st_size == 0:
            return pd.DataFrame(columns=["data", "valor"])
        df = pd.read_csv(IBC_BR_CSV)
        if "data" not in df.columns or "valor" not in df.columns:
            return pd.DataFrame(columns=["data", "valor"])
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        df = df.dropna(subset=["data", "valor"]).sort_values("data").reset_index(drop=True)
        return df


    try:
        df_ibc = _carregar_ibcbr_offline()
        if len(df_ibc) >= 13:
            last = df_ibc.iloc[-1]
            prev = df_ibc.iloc[-2]

            # m/m (%)
            var_mensal = (float(last["valor"]) / float(prev["valor"]) - 1) * 100.0

            # YTD (%): último / primeiro do ano - 1
            ano = pd.to_datetime(last["data"]).year
            first_year = df_ibc[df_ibc["data"].dt.year == ano].iloc[0]
            acum_ano = (float(last["valor"]) / float(first_year["valor"]) - 1) * 100.0

            # 12m (%): último / 12m atrás - 1
            v_12m = float(df_ibc.iloc[-13]["valor"])
            acum_12m = (float(last["valor"]) / v_12m - 1) * 100.0

            # variação a/a from NSA series (24363)
            var_aa = 99.99  # test
            try:
                df_nsa = _baixar_serie_sgs_json(24363, n_ultimos=120)
                df_nsa = df_nsa.sort_values("data").reset_index(drop=True)
                data_ref = pd.to_datetime(last["data"])
                # valor atual na série sem ajuste
                mask_atual = (df_nsa["data"].dt.year == data_ref.year) & (df_nsa["data"].dt.month == data_ref.month)
                df_atual = df_nsa.loc[mask_atual]
                # valor do mesmo mês do ano anterior
                mask_aa = (df_nsa["data"].dt.year == data_ref.year - 1) & (df_nsa["data"].dt.month == data_ref.month)
                df_aa = df_nsa.loc[mask_aa]
                if not df_atual.empty and not df_aa.empty:
                    valor_atual = float(df_atual.iloc[-1]["valor"])
                    valor_aa = float(df_aa.iloc[-1]["valor"])
                    var_aa = (valor_atual / valor_aa - 1.0) * 100.0
            except Exception:
                pass

            linhas.append(
                {
                    "Indicador": "IBC-Br (BCB) – índice",
                    "Mês ref": _formata_mes(pd.to_datetime(last["data"])),
                    "Nível": round(float(last["valor"]), 2),
                    "Var. mensal": round(var_mensal, 2),
                    "Acum. ano": round(acum_ano, 2),
                    "Acum. 12m": round(var_aa, 2),
                    "Classificação": "🟡 Coincidente",
                    "Fonte": "BCB (CSV offline)",
                }
            )
    except Exception:
        pass


    return pd.DataFrame(linhas)



@lru_cache(maxsize=1)
def carregar_risco_brasil_embi() -> Tuple[Optional[float], Optional[float], Optional[str], Optional[float]]:
    """
    Agora NÃO usa mais o EMBI do Ipeadata.

    Lê o CSV 'data/curto_prazo/risco_brasil_spread_10y.csv',
    gerado por risco_brasil_spread_10y.atualizar_spread_10y().

    Retorna:
    - risco_nivel: último valor do spread (pontos-base)
    - risco_delta_aa: diferença vs ~12 meses atrás (pontos-base), se houver base
    - referencia: string "MM/AAAA" da última data disponível
    - risco_media_12m: média dos últimos 12 meses (pontos-base), se houver base
    """
    base_dir = Path(__file__).resolve().parent
    csv_path = base_dir / "data" / "curto_prazo" / "risco_brasil_spread_10y.csv"

    if not csv_path.exists():
        print(f"[spread 10y] CSV não encontrado: {csv_path}")
        return None, None, None, None

    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:  # noqa: BLE001
        print(f"[spread 10y] Erro ao ler CSV: {exc}")
        return None, None, None, None

    if "data" not in df.columns or "spread_pb" not in df.columns:
        print(f"[spread 10y] Colunas 'data' e 'spread_pb' não encontradas em {csv_path}")
        return None, None, None, None

    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df["spread_pb"] = pd.to_numeric(df["spread_pb"], errors="coerce")

    df = (
        df.dropna(subset=["data", "spread_pb"])
        .sort_values("data")
        .reset_index(drop=True)
    )

    if df.empty:
        return None, None, None, None

    # Último ponto
    ultimo = df.iloc[-1]
    data_ult = ultimo["data"]
    risco_nivel = float(ultimo["spread_pb"])
    referencia = data_ult.strftime("%m/%Y")

    # Ponto ~12 meses atrás (janela ±7 dias) — se ainda não tiver 12m de dados, fica None
    alvo_aa = data_ult - _td(days=365)
    janela = df[
        (df["data"] >= alvo_aa - _td(days=7))
        & (df["data"] <= alvo_aa + _td(days=7))
    ]

    risco_delta_aa: Optional[float] = None
    if not janela.empty:
        valor_aa = float(janela.iloc[-1]["spread_pb"])
        risco_delta_aa = risco_nivel - valor_aa

    # Média 12m
    risco_media_12m: Optional[float] = None
    ult_12m = df[df["data"] >= (data_ult - _td(days=365))]
    if not ult_12m.empty:
        risco_media_12m = float(ult_12m["spread_pb"].mean())

    return risco_nivel, risco_delta_aa, referencia, risco_media_12m

def selecionar_contrato_di_5_anos(
    df_hist_di: Optional[pd.DataFrame],
    horizonte_anos: float = 5.0,
) -> Optional[Dict[str, object]]:
    """
    A partir do histórico de DI futuro (di1_historico.csv),
    escolhe o contrato cujo vencimento está mais próximo de `horizonte_anos`
    anos à frente da última data disponível.

    Retorna um dicionário com:
      - ticker
      - data_ref (último pregão)
      - vencimento
      - taxa_atual
      - taxa_d_1
      - taxa_inicio_ano
      - df_ticker (histórico só desse contrato, ordenado por data)
    """
    if df_hist_di is None or df_hist_di.empty:
        return None

    df = df_hist_di.copy()

    try:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
    except Exception:
        return None

    if "vencimento" not in df.columns:
        return None

    try:
        df["vencimento"] = pd.to_datetime(df["vencimento"], errors="coerce")
    except Exception:
        return None

    df = df.dropna(subset=["data", "vencimento", "taxa", "ticker"])
    if df.empty:
        return None

    # última data do histórico
    data_ref = df["data"].max().normalize()

    df_ref = df[df["data"] == data_ref].copy()
    if df_ref.empty:
        return None

    # anos até o vencimento
    dias_ate_venc = (df_ref["vencimento"] - data_ref).dt.days
    df_ref["anos_ate_venc"] = dias_ate_venc / 365.25
    df_ref = df_ref[df_ref["anos_ate_venc"] > 0]
    if df_ref.empty:
        return None

    # escolhe o contrato cujo prazo está mais perto de 5 anos
    idx_sel = (df_ref["anos_ate_venc"] - horizonte_anos).abs().idxmin()
    linha_sel = df_ref.loc[idx_sel]

    ticker = str(linha_sel["ticker"])

    # histórico apenas desse ticker
    df_ticker = df[df["ticker"] == ticker].copy()
    df_ticker = df_ticker.sort_values("data")
    df_ticker = df_ticker.dropna(subset=["taxa"])
    if df_ticker.empty:
        return None

    linha_ult = df_ticker.iloc[-1]
    taxa_atual = float(linha_ult["taxa"])
    data_ult = linha_ult["data"]

    # D-1 (mesmo contrato, pregão anterior)
    taxa_d_1 = None
    if len(df_ticker) >= 2:
        taxa_d_1 = float(df_ticker.iloc[-2]["taxa"])

    # taxa no início do ano
    ano_ref = data_ult.year
    primeiro_dia_ano = datetime(ano_ref, 1, 1)
    df_ano = df_ticker[df_ticker["data"] >= primeiro_dia_ano]
    taxa_inicio_ano = float(df_ano.iloc[0]["taxa"]) if not df_ano.empty else None

    return {
        "ticker": ticker,
        "data_ref": data_ult.date() if hasattr(data_ult, "date") else data_ult,
        "vencimento": (
            linha_sel["vencimento"].date()
            if not pd.isna(linha_sel["vencimento"])
            else None
        ),
        "taxa_atual": taxa_atual,
        "taxa_d_1": taxa_d_1,
        "taxa_inicio_ano": taxa_inicio_ano,
        "df_ticker": df_ticker,
    }


@lru_cache(maxsize=1)
def obter_referencia_di_futuro() -> Optional[str]:
    """
    Data de referência do DI Futuro ~5 anos:
    último pregão do contrato escolhido em selecionar_contrato_di_5_anos.
    """
    try:
        df_di = carregar_historico_di_futuro()
    except Exception:
        return None

    selecao = selecionar_contrato_di_5_anos(df_di)
    if not selecao:
        return None

    data_ref = selecao["data_ref"]
    try:
        return data_ref.strftime("%d/%m/%Y")
    except Exception:
        return None

    
@lru_cache(maxsize=1)
def obter_referencia_ptax() -> Optional[str]:
    """Data de referência da PTAX (último dado), em dd/mm/aaaa."""
    try:
        df_fx = buscar_ptax_venda()
    except Exception:
        return None

    if df_fx is None or df_fx.empty:
        return None

    data_ult = df_fx["data"].max()
    if hasattr(data_ult, "date"):
        data_ult = data_ult.date()

    try:
        return data_ult.strftime("%d/%m/%Y")
    except Exception:
        return None


@lru_cache(maxsize=1)
def obter_referencia_ibovespa() -> Optional[str]:
    """Data de referência do Ibovespa (último fechamento), em dd/mm/aaaa."""
    try:
        df_hist = obter_historico_ibovespa_inteligente()
    except Exception:
        try:
            df_hist = carregar_historico_ibovespa()
        except Exception:
            return None

    if df_hist is None or df_hist.empty:
        return None

    data_ult = df_hist["data"].max()
    if hasattr(data_ult, "date"):
        data_ult = data_ult.date()

    try:
        return data_ult.strftime("%d/%m/%Y")
    except Exception:
        return None



def render_bloco_termometro_macro_br() -> None:
    """
    Termômetro macro – Brasil com dados reais onde já temos back-end pronto:
      1) Selic Meta (Copom)
      2) DI Futuro ~5 anos
      3) IPCA do mês (m/m vs Focus)
      7) Câmbio BRL/USD (PTAX)
      8) Ibovespa – nível / variação no ano

    Os demais ainda ficam com números de exemplo, só layout.
    """
    _inject_ion_css_curto_prazo()

    # -----------------------------
    # Carrega curto prazo (Selic, PTAX, Ibov, DI)
    # -----------------------------
    try:
        dados = carregar_dados_curto_prazo_br()
    except Exception:
        dados = None

    moeda = getattr(dados, "moeda_juros", None) if dados is not None else None
    ativos = getattr(dados, "ativos_domesticos", None) if dados is not None else None

    # -----------------------------
    # Macro / fiscal (IBC-Br, etc.)
    # -----------------------------
    dados_macro = carregar_dados_macro_fiscal_br()

    ibc_nivel = dados_macro.ibcbr_nivel
    ibc_var_mom = dados_macro.ibcbr_var_mom
    ibc_var_aa = dados_macro.ibcbr_var_aa
    ibc_var_3m = dados_macro.ibcbr_var_3m_dessaz
    ibc_ref = dados_macro.ibcbr_referencia

   
    divida_nivel = dados_macro.divida_bruta_pct_pib
    divida_delta_mom = dados_macro.divida_bruta_delta_pp_12m  # ainda é m/m
    divida_12m_ago = dados_macro.divida_bruta_pct_pib_12m_atras
    divida_24m_ago = dados_macro.divida_bruta_pct_pib_24m_atras
    divida_ref = dados_macro.divida_bruta_referencia

    # Δ a/a em p.p. (nível atual – nível do mesmo mês há 12m)
    divida_delta_aa = None
    if (divida_nivel is not None) and (divida_12m_ago is not None):
        divida_delta_aa = divida_nivel - divida_12m_ago

    # badge com a referência da dívida, se existir (ex.: "10/2025")
    badge_divida = divida_ref or "vs a/a (p.p.)"


    primario_mes_real_bi = dados_macro.primario_mes_real_bi
    primario_mes_delta_real_bi_aa = dados_macro.primario_mes_delta_real_bi_aa
    receita_real_var_aa_pct = dados_macro.receita_real_var_aa_pct
    despesa_real_var_aa_pct = dados_macro.despesa_real_var_aa_pct
    primario_ano_real_bi = dados_macro.primario_ano_real_bi
    primario_ano_real_bi_prev = dados_macro.primario_ano_real_bi_prev


    # -----------------------------
    # IPCA + Focus (já vem do próprio indicadores_macro)
    # -----------------------------
    try:
        resumo_ipca = resumo_ipca_com_focus_mensal()
    except Exception:
        resumo_ipca = {}

    ipca_referencia = resumo_ipca.get("referencia")
    ipca_mensal = resumo_ipca.get("mensal")
    ipca_focus_mensal = resumo_ipca.get("focus_mensal")
    ipca_surpresa_mensal = resumo_ipca.get("surpresa_mensal")

    st.markdown("#### Termômetro Macro – Brasil")

    # =====================================================
    # LINHA 1
    # =====================================================
    col1, col2, col3, col4 = st.columns(4)

    # 1) Selic meta – Δ vs última reunião do Copom
    with col1:
        selic_atual = getattr(moeda, "selic_meta", None) if moeda is not None else None
        selic_ultima = (
            getattr(moeda, "selic_ultima_decisao", None) if moeda is not None else None
        )

        selic_delta = None
        if selic_atual is not None and selic_ultima is not None:
            # seta = Selic atual – Selic na ÚLTIMA reunião
            selic_delta = selic_atual - selic_ultima

        # datas vindas do objeto "moeda"
        data_proxima = getattr(moeda, "selic_referencia", None)           # próxima reunião
        data_ultima = getattr(moeda, "selic_data_ultima_reuniao", None)   # última reunião

        # Canto superior direito → PRÓXIMA reunião do Copom
        badge_selic = data_proxima or "Próx. Copom"

        # Texto de baixo → data da ÚLTIMA reunião + explicação do delta
        if data_ultima:
            selic_subtext = f"Última decisão do Copom: {data_ultima}"
        else:
            selic_subtext = "Δ vs última decisão do Copom"
        
        # Texto do botão "i" (tooltip), explicando a data do canto superior direito
        if data_proxima and data_ultima:
            info_selic = (
                f"A data no canto superior direito ({badge_selic}) indica a "
                f"**próxima reunião do Copom**. O valor do card é a Selic meta "
                f"atual, e a seta compara com a taxa definida na última reunião "
                f"({data_ultima})."
            )
        elif data_proxima:
            info_selic = (
                f"A data no canto superior direito ({badge_selic}) indica a "
                "próxima reunião do Copom. A variação mostra a diferença em "
                "pontos percentuais entre a Selic atual e a última decisão."
            )
        else:
            info_selic = (
                "Quando houver calendário definido, a data no canto superior "
                "direito mostrará a próxima reunião do Copom. O valor do card é "
                "a Selic meta atual e a seta compara com a última decisão disponível."
            )

        metric_card(
            label="Selic meta vs última decisão do Copom",
            value=selic_atual,
            delta=selic_delta,
            fmt_value="{:.2f}",
            value_is_pct=True,   # 15,00 sem símbolo de %
            delta_is_pp=True,     # delta em p.p.
            badge=badge_selic,
            icon_html=ICON_PERCENT,
            subtext=selic_subtext,
            info_text=info_selic,
        )


    # 2) DI Futuro ~5 anos (ex.: DI Jan/31)
    with col2:
        # tenta extrair tudo direto do histórico da B3 (CSV local)
        selecao_di5 = None
        try:
            df_hist_di = carregar_historico_di_futuro()
            selecao_di5 = selecionar_contrato_di_5_anos(df_hist_di)
        except Exception:
            selecao_di5 = None

        if selecao_di5:
            di5_taxa = selecao_di5["taxa_atual"]
            taxa_d_1 = selecao_di5["taxa_d_1"]
            taxa_inicio_ano = selecao_di5["taxa_inicio_ano"]
            di5_ticker = selecao_di5["ticker"]

            di5_delta = None
            if (di5_taxa is not None) and (taxa_d_1 is not None):
                di5_delta = di5_taxa - taxa_d_1

            di5_delta_ano = None
            if (di5_taxa is not None) and (taxa_inicio_ano is not None):
                di5_delta_ano = di5_taxa - taxa_inicio_ano

            di5_fonte = "Curva DI Futuro B3 (CSV local)"
        else:
            # fallback: usa os valores calculados em dados_curto_prazo_br
            di5_taxa = getattr(ativos, "di_5_anos_taxa", None) if ativos is not None else None
            di5_delta = getattr(ativos, "di_5_anos_delta", None) if ativos is not None else None
            di5_fonte = (
                getattr(ativos, "di_5_anos_fonte_delta", None) if ativos is not None else None
            )
            di5_ticker = (
                getattr(ativos, "di_5_anos_ticker", None) if ativos is not None else None
            )
            di5_delta_ano = (
                getattr(ativos, "di_5_anos_delta_ano", None) if ativos is not None else None
            )

        # título no padrão pedido: "DI1N29 (B3) vs d-1"
        if di5_ticker:
            titulo_di5 = f"{di5_ticker} (B3) vs d-1"
        else:
            titulo_di5 = "DI Futuro ~5 anos (B3) vs d-1"

        # badge: última data do contrato escolhido
        di5_referencia = obter_referencia_di_futuro()
        badge_di5 = di5_referencia or "VS D-1"

        # subtexto: resumo desde início do ano
        subtext_di5 = None
        if di5_delta_ano is not None:
            if abs(di5_delta_ano) < 0.01:
                subtext_di5 = "estável vs início do ano"
            elif di5_delta_ano > 0:
                subtext_di5 = (
                    "abriu "
                    + _format_delta_br(abs(di5_delta_ano), 2)
                    + " p.p. desde o início do ano"
                )
            else:
                subtext_di5 = (
                    "fechou "
                    + _format_delta_br(abs(di5_delta_ano), 2)
                    + " p.p. desde o início do ano"
                )

        # texto do botão "i"
        if di5_referencia:
            info_di5 = (
                f"A data no canto superior direito ({badge_di5}) indica o "
                "último pregão com dado disponível para o contrato de DI "
                "mostrado no título do card. "
                "O valor principal é a taxa anualizada desse DI futuro e a seta "
                "mostra a variação, em pontos percentuais, em relação ao "
                "fechamento do dia útil anterior (D-1). "
                "O texto abaixo resume quanto essa taxa abriu ou fechou, em "
                "pontos percentuais, desde o início do ano."
            )
        else:
            info_di5 = (
                "Este card mostra a taxa anualizada de um contrato de DI futuro "
                "(~5 anos). A seta compara a taxa de hoje com o fechamento do "
                "dia útil anterior (D-1). Quando houver dado atualizado, a data "
                "no canto superior direito passará a indicar o último pregão "
                "com informação disponível."
            )

        metric_card(
            label=titulo_di5,
            value=di5_taxa,
            delta=di5_delta,
            fmt_value="{:.2f}",
            value_is_pct=True,
            delta_is_pp=True,
            badge=badge_di5,
            icon_html=ICON_PERCENT,
            subtext=subtext_di5,
            info_text=info_di5,
        )


    # 3) IPCA – variação mensal (mesmo padrão do Curto Prazo)
    with col3:
        # badge: referência do mês (ex.: "10/2025")
        badge_ipca = ipca_referencia or "mês ref."

        # texto padrão
        subtext_ipca = "IPCA mensal x mediana Focus."

        focus_str = None
        if ipca_focus_mensal is not None:
            focus_str = f"{ipca_focus_mensal:.2f}%"

        # se tiver Focus, mostra: "Mediana Focus para o mês: X,XX%"
        if (ipca_mensal is not None) and (focus_str is not None):
            subtext_ipca = f"Mediana Focus para o mês: {focus_str}"
        
        # Texto do botão "i" (tooltip), explicando a data do canto superior direito
        if badge_ipca and ipca_mensal is not None:
            info_ipca = (
                f"A data no canto superior direito ({badge_ipca}) indica o "
                "mês de referência do último dado de IPCA divulgado pelo IBGE. "
                "O valor do card é a variação percentual do IPCA naquele mês, "
                "e a seta mostra a diferença, em pontos percentuais (p.p.), "
                "entre o IPCA realizado e a mediana das expectativas Focus "
                "para esse mesmo mês."
            )
        elif badge_ipca:
            info_ipca = (
                f"A data no canto superior direito ({badge_ipca}) indica o "
                "mês de referência do IPCA mais recente disponível. Quando o "
                "dado for atualizado, o valor do card mostrará a variação "
                "percentual desse mês e a seta comparará com a mediana Focus."
            )
        else:
            info_ipca = (
                "Este card mostra a variação mensal do IPCA e compara o dado "
                "divulgado pelo IBGE com a mediana das expectativas Focus "
                "para o mesmo mês."
            )


        metric_card(
            "IPCA – variação mensal",
            ipca_mensal,              # valor principal (IBGE)
            ipca_surpresa_mensal,     # Δ vs Focus (em p.p.)
            fmt_value="{:.2f}",
            value_is_pct=True,        # X,XX%
            delta_is_pp=True,         # seta: X,XX p.p.
            badge=badge_ipca,         # "10/2025", por exemplo
            icon_html=ICON_PERCENT,
            subtext=subtext_ipca,
            info_text=info_ipca,
        )


    # 4) IBC-Br – nível / variação (dados reais)
    with col4:
        partes_ibc = []

        if ibc_var_3m is not None:
            partes_ibc.append(f"3m (dessaz.): {ibc_var_3m:+.2f}%")

        if ibc_var_aa is not None:
            partes_ibc.append(f"a/a (sem ajuste): {ibc_var_aa:+.2f}%")

        if partes_ibc:
            subtext_ibc = " | ".join(partes_ibc)
        else:
            subtext_ibc = (
                "Variações em 3m e a/a não disponíveis "
                "(erro ao carregar séries)."
            )
        # Texto do botão "i" (tooltip), explicando a data do canto superior direito
        if ibc_ref and ibc_var_mom is not None:
            info_ibc = (
                f"A data no canto superior direito ({ibc_ref}) indica o mês de "
                "referência do IBC-Br mais recente divulgado pelo Banco "
                "Central. O valor do card é o nível do índice de atividade, "
                "e a seta mostra a variação percentual dessazonalizada em "
                "relação ao mês imediatamente anterior (M-1). "
                "O texto abaixo resume as variações em 3 meses "
                "(dessazonalizado) e em 12 meses (sem ajuste sazonal), "
                "quando disponíveis."
            )
        elif ibc_ref:
            info_ibc = (
                f"A data no canto superior direito ({ibc_ref}) indica o mês de "
                "referência do IBC-Br mais recente disponível. Quando a série "
                "está completa, o card mostra a variação dessazonalizada mês "
                "a mês (M-1) e as variações em 3 meses e em 12 meses."
            )
        else:
            info_ibc = (
                "Este card mostra o nível e a variação mensal do IBC-Br, um "
                "indicador de atividade econômica calculado pelo Banco "
                "Central. A seta compara o mês corrente com o mês "
                "imediatamente anterior (M-1) e o texto abaixo resume "
                "variações em 3 meses e em 12 meses."
            )


        metric_card(
            label="IBC-Br – nível / variação M-1",
            value=ibc_nivel,           # nível atual (série SA)
            delta=ibc_var_mom,         # m/m dessaz. → pílula
            fmt_value="{:.2f}",
            value_is_pct=False,
            delta_is_pct=True,         # delta em %
            badge=ibc_ref,             # seta = mês vs mês anterior
            icon_html=ICON_CHART,
            subtext=subtext_ibc,       # "3m (dessaz.): ... | a/a (sem ajuste): ..."
            info_text=info_ibc,
        )



    st.markdown("&nbsp;", unsafe_allow_html=True)

    # =====================================================
    # LINHA 2
    # =====================================================
    col5, col6, col7, col8 = st.columns(4)

    # 5) Emprego formal – saldo (mês vs a/a, com média 5 anos)
    with col5:
        resumo_caged = resumo_caged_saldo_novo()

        saldo_atual = resumo_caged.get("saldo_atual")
        delta_12m = resumo_caged.get("delta_12m")
        media_mes_5anos = resumo_caged.get("media_mes_5anos")
        ref_caged = resumo_caged.get("referencia") or "-"

        if saldo_atual is None:
            info_caged = (
                "Este card mostra o saldo líquido de empregos formais reportado "
                "pelo Novo Caged (admissões menos demissões). Quando os dados "
                "estão indisponíveis, o painel exibe esta mensagem de aviso."
            )

            metric_card(
                label="Emprego formal – saldo (mês vs a/a)",
                value=0.0,
                delta=0.0,
                fmt_value="{:.0f}",
                value_is_pct=False,
                delta_is_pct=False,
                badge="Sem dados CAGED",
                icon_html=ICON_CHART,
                subtext="Não foi possível carregar o Novo Caged (Ipeadata).",
                info_text=info_caged,   # 👈 NOVO
            )

        else:
            # saldo e delta em MIL vagas
            valor_mil = saldo_atual / 1000.0
            delta_mil = (
                round(delta_12m / 1000.0, 0) if delta_12m is not None else 0.0
            )

            media_5anos_mil = (
                media_mes_5anos / 1000.0 if media_mes_5anos is not None else None
            )

            if media_5anos_mil is not None:
                subtext_caged = (
                    f"Média deste mês nos últimos 5 anos: "
                    f"{media_5anos_mil:.0f} mil."
                )
            else:
                subtext_caged = "Saldo mensal de vagas formais. Δ vs mesmo mês há 12m."

            # Texto do botão "i" (tooltip), explicando a data do canto superior direito
            if ref_caged and ref_caged != "-":
                info_caged = (
                    f"A data no canto superior direito ({ref_caged}) indica o "
                    "mês de referência do saldo de empregos formais reportado "
                    "pelo Novo Caged. O valor do card mostra o saldo líquido de "
                    "vagas formais criado no mês (admissões menos demissões), "
                    "em milhares de postos de trabalho. A pílula embaixo indica "
                    "a diferença, também em milhares de vagas, em relação ao "
                    "mesmo mês do ano anterior (variação a/a). O texto abaixo "
                    "compara o saldo do mês com a média dos últimos cinco anos "
                    "para esse mesmo mês."
                )
            else:
                info_caged = (
                    "Este card mostra o saldo líquido de empregos formais "
                    "reportado pelo Novo Caged, em milhares de vagas, e a "
                    "variação em relação ao mesmo mês do ano anterior."
                )

            metric_card(
                label="Emprego formal – saldo (mês vs a/a)",
                value=valor_mil,                 # ex.: 85 -> "85 mil"
                delta=delta_mil,                 # ex.: -48
                fmt_value="{:.0f} mil",
                value_is_pct=False,
                delta_is_pct=False,              # delta não é %, é nível
                badge=ref_caged,                 # "10/2025", etc.
                icon_html=ICON_CHART,
                subtext=subtext_caged,           # texto da média 5 anos
                delta_is_money=True,             # ativa modo dinheiro
                delta_money_prefix="R$ ",
                delta_money_suffix=" mil",
                delta_money_decimals=0,
                info_text=info_caged,            # 👈 NOVO
            )




    # 6) Taxa de Desemprego – PNAD Contínua (trimestre móvel, dados reais)
    with col6:
        desemp_atual = getattr(dados_macro, "desemprego_pnad", None)

        # Se por algum motivo não carregou a PNAD, mostra um fallback
        if desemp_atual is None:
            metric_card(
                label="Desemprego – PNAD Contínua (trimestre móvel, %)",
                value=0.0,
                delta=0.0,
                fmt_value="{:.1f}",
                value_is_pct=True,
                delta_is_pp=True,
                badge="Sem dados PNAD",
                icon_html=ICON_PERCENT,
                subtext="Não foi possível carregar a PNAD Contínua do SIDRA.",
            )
        else:
            desemp_delta = dados_macro.desemprego_delta_pp_12m or 0.0
            desemp_12m = dados_macro.desemprego_pnad_12m_atras
            desemp_24m = dados_macro.desemprego_pnad_24m_atras

            partes_subtexto = []
            if desemp_12m is not None:
                partes_subtexto.append(f"há 12m: {desemp_12m:.1f}%")
            if desemp_24m is not None:
                partes_subtexto.append(f"há 24m: {desemp_24m:.1f}%")
            subtexto = " | ".join(partes_subtexto)

            # badge com o texto do trimestre móvel, ex.: "TRI ATÉ OUT/2025"
            badge_pnad = dados_macro.desemprego_pnad_referencia or "PNAD – tri móvel"

            # Texto do botão "i"
            info_pnad = (
                "Este card mostra a taxa de desemprego medida pela PNAD Contínua "
                "(IBGE), calculada sobre o trimestre móvel mais recente. "
                f"A data '{badge_pnad}' no canto superior direito indica o "
                "trimestre móvel de referência. "
                "O valor principal é a taxa média de desocupação desse período. "
                "A variação (Δ) mostra a diferença, em pontos percentuais, em "
                "relação ao mesmo trimestre móvel de 12 meses atrás. "
                "No texto abaixo, são exibidas as taxas observadas há 12 e 24 "
                "meses, para comparação histórica."
            )

            metric_card(
                label="Desemprego – PNAD Contínua (trimestre móvel, %)",
                value=desemp_atual,
                delta=desemp_delta,
                fmt_value="{:.1f}",
                value_is_pct=True,
                delta_is_pp=True,  # delta é em p.p.
                badge=badge_pnad,
                icon_html=ICON_PERCENT,
                subtext=subtexto,
                info_text=info_pnad,  # 👈 ativa o botão "i" com a explicação
            )


    # 7) Câmbio BRL/USD (PTAX) – mesmo visual do Curto Prazo
    with col7:
        ptax = getattr(moeda, "ptax_fechamento", None) if moeda is not None else None
        ptax_var_dia = (
            getattr(moeda, "ptax_variacao_dia", None) if moeda is not None else None
        )

        # Monta texto: "12m: X,XX% | 24m: Y,YY%"
        ptax_var_12m = (
            getattr(moeda, "ptax_var_12m", None) if moeda is not None else None
        )
        ptax_var_24m = (
            getattr(moeda, "ptax_var_24m", None) if moeda is not None else None
        )

        fx_parts = []
        if ptax_var_12m is not None:
            fx_parts.append("12m: " + _format_delta_br(ptax_var_12m, 2) + "%")
        if ptax_var_24m is not None:
            fx_parts.append("24m: " + _format_delta_br(ptax_var_24m, 2) + "%")
        fx_subtext = " | ".join(fx_parts) if fx_parts else None

        # badge = data de referência da PTAX (dd/mm/aaaa)
        badge_ptax = obter_referencia_ptax() or "-"

        # Texto do botão "i" (tooltip), explicando a data do canto superior direito
        if badge_ptax != "-" and ptax is not None:
            info_ptax = (
                f"A data no canto superior direito ({badge_ptax}) indica o dia "
                "de referência da PTAX de venda divulgada pelo Banco Central. "
                "O valor do card mostra a cotação do dólar em reais (R$/US$) "
                "para esse dia. A variação diária (Δ) exibe, em percentual, "
                "quanto a PTAX se apreciou ou se depreciou em relação ao "
                "fechamento do dia útil anterior. No texto abaixo, são "
                "mostradas as variações acumuladas em 12 meses e em 24 meses."
            )
        else:
            info_ptax = (
                "Este card mostra a cotação do dólar PTAX de venda em R$/US$ "
                "e a variação diária em relação ao fechamento anterior. "
                "O texto abaixo resume as variações acumuladas em 12 e 24 meses."
            )


        metric_card(
            "PTAX – dólar (R$) – intraday",
            ptax,
            ptax_var_dia,
            fmt_value="R$ {:.2f}",
            value_is_pct=False,
            delta_is_pct=True,   # variação em %
            badge=badge_ptax,    # dd/mm/aaaa
            icon_html=ICON_DOLLAR,
            subtext=fx_subtext,
            info_text=info_ptax,
        )


    # 8) Ibovespa – pts – igual ao card de Curto Prazo
    with col8:
        # nível do Ibov
        valor_ibov = (
            getattr(ativos, "ibov_nivel", None) if ativos is not None else None
        )

        # delta diário vs D-1
        delta_ibov = (
            getattr(ativos, "ibov_var_dia", None) if ativos is not None else None
        )

        # monta subtexto: "mês: X,XX% | ano: Y,YY%"
        ibov_var_mes = (
            getattr(ativos, "ibov_var_mes", None) if ativos is not None else None
        )
        ibov_var_ano_val = (
            getattr(ativos, "ibov_var_ano", None) if ativos is not None else None
        )

        ibov_parts = []
        if ibov_var_mes is not None:
            ibov_parts.append("mês: " + _format_delta_br(ibov_var_mes, 2) + "%")
        if ibov_var_ano_val is not None:
            ibov_parts.append("ano: " + _format_delta_br(ibov_var_ano_val, 2) + "%")
        ibov_subtext = " | ".join(ibov_parts) if ibov_parts else None

        # badge = data do último fechamento (dd/mm/aaaa)
        ibov_referencia = obter_referencia_ibovespa() or "-"

        # Texto do botão "i" (tooltip), explicando a data do canto superior direito
        if ibov_referencia != "-" and valor_ibov is not None:
            info_ibov = (
                f"A data no canto superior direito ({ibov_referencia}) indica o "
                "último pregão com fechamento disponível do Ibovespa. "
                "O valor principal do card mostra o nível do índice em pontos, "
                "enquanto a variação diária (Δ) indica, em percentual, quanto o "
                "Ibovespa subiu ou caiu em relação ao fechamento do dia útil "
                "anterior (D-1). No texto abaixo, são exibidas as variações "
                "acumuladas no mês e no ano."
            )
        else:
            info_ibov = (
                "Este card mostra o nível do Ibovespa em pontos, a variação "
                "diária em relação ao fechamento anterior (D-1) e, no texto "
                "inferior, as variações acumuladas no mês e no ano."
            )

        metric_card(
            "Ibovespa – pts – VS D-1",
            valor_ibov,
            delta_ibov,
            fmt_value="{:,.2f}",
            value_is_pct=False,
            delta_is_pct=True,
            badge=ibov_referencia,   # dd/mm/aaaa
            icon_html=ICON_CHART,
            subtext=ibov_subtext,
            info_text=info_ibov,
        )


    st.markdown("&nbsp;", unsafe_allow_html=True)

    # =====================================================
    # LINHA 3 (fiscal & risco país – ainda mock)
    # =====================================================
    col9, col10, col11, col12 = st.columns(4)

    # 9) Risco-Brasil – spread soberano (10Y BR – 10Y US)
    with col9:
        (
            risco_nivel,
            risco_delta_aa,
            risco_ref,
            risco_media_12m,
            risco_delta_d1,
            risco_inicio_mes,
            risco_data_inicio_mes,
        ) = carregar_risco_brasil_spread_10y()

        if risco_nivel is None:
            info_risco = (
                "Este card mostra o spread de risco-país em pontos-base (p.b.) "
                "entre o título soberano brasileiro de 10 anos e o título "
                "americano de 10 anos (US Treasury). Quando os dados não estão "
                "disponíveis, o painel exibe esta mensagem de aviso."
            )

            metric_card(
                label="Risco-Brasil – spread 10Y (proxy risco-país)",
                value=0.0,
                delta=0.0,
                fmt_value="{:.0f}",
                value_is_pct=False,
                delta_is_pct=False,
                badge="sem dados",
                icon_html=ICON_CHART,
                subtext="Não foi possível carregar o spread 10Y Brasil local – US10Y.",
                info_text=info_risco,  # 👈 NOVO
            )

        else:
            # texto enxuto: apenas início do mês em p.b.
            if risco_inicio_mes is not None:
                subtext_spread = f"Início do mês: {risco_inicio_mes:.0f} p.b."
            else:
                subtext_spread = None

            # Texto do botão "i" (tooltip), explicando a data do canto superior direito
            if risco_ref:
                info_risco = (
                    f"A data no canto superior direito ({risco_ref}) indica o "
                    "último dia com dado disponível para o spread de 10 anos "
                    "entre a taxa soberana brasileira e o título do Tesouro "
                    "americano de 10 anos. O valor do card mostra esse spread "
                    "em pontos-base (p.b.). A variação diária (Δ) indica, "
                    "também em p.b., quanto o spread se alargou ou fechou em "
                    "relação ao dia útil anterior (D-1). O texto abaixo resume "
                    f"o nível do spread no início do mês "
                    f"({risco_inicio_mes:.0f} p.b., quando disponível)."
                    if risco_inicio_mes is not None
                    else (
                        f"A data no canto superior direito ({risco_ref}) indica o "
                        "último dia com dado disponível para o spread de 10 anos "
                        "entre Brasil e EUA. O valor do card mostra esse spread "
                        "em pontos-base (p.b.) e a variação diária (Δ) indica a "
                        "mudança em relação ao dia útil anterior."
                    )
                )
            else:
                info_risco = (
                    "Este card mostra o spread, em pontos-base (p.b.), entre a "
                    "taxa de juros soberana brasileira de 10 anos e o título do "
                    "Tesouro americano de 10 anos. A variação diária (Δ) indica "
                    "quanto esse spread se alargou ou fechou em relação ao dia "
                    "anterior, e o texto abaixo destaca o nível observado no "
                    "início do mês."
                )

            metric_card(
                label="Risco-País – spread 10Y (Brasil/USA)",
                value=risco_nivel,
                # setinha: variação D-1 em p.b.
                delta=risco_delta_d1 or 0.0,
                fmt_value="{:.0f}",      # número grande só "917"
                value_is_pct=False,
                delta_is_pct=False,
                badge=risco_ref or "",
                icon_html=ICON_CHART,
                subtext=subtext_spread,  # "Início do mês: 928 p.b."
                # força o sufixo " p.b." na pílula do delta
                delta_is_money=True,
                delta_money_prefix="",
                delta_money_suffix=" p.b.",
                delta_money_decimals=2,
                info_text=info_risco,     # 👈 NOVO
            )


    # 10) Dívida Bruta GG (% PIB) – dados reais
    with col10:
        # monta o texto com níveis de 12m e 24m atrás
        if divida_12m_ago is not None and divida_24m_ago is not None:
            subtext_divida = (
                f"há 12m: {divida_12m_ago:.1f}% | "
                f"há 24m: {divida_24m_ago:.1f}%"
            )
        elif divida_12m_ago is not None:
            subtext_divida = f"há 12m: {divida_12m_ago:.1f}%"
        elif divida_24m_ago is not None:
            subtext_divida = f"há 24m: {divida_24m_ago:.1f}%"
        else:
            subtext_divida = (
                "Níveis de 12m e 24m não disponíveis "
                "(erro ao carregar série)."
            )
        # Texto do botão "i" (tooltip), explicando a data do canto superior direito
        if badge_divida:
            info_divida = (
                f"A data no canto superior direito ({badge_divida}) indica o "
                "mês de referência da dívida bruta do governo geral. "
                "O valor do card mostra o estoque de dívida bruta como "
                "percentual do PIB naquele mês. A variação (Δ) indica, em "
                "pontos percentuais, quanto esse indicador mudou em relação "
                "ao mesmo mês do ano anterior. O texto abaixo resume os "
                "níveis observados há 12 e 24 meses."
            )
        else:
            info_divida = (
                "Este card mostra a dívida bruta do governo geral como "
                "percentual do PIB e a variação em pontos percentuais em "
                "relação ao mesmo mês do ano anterior."
            )


        metric_card(
            label="Dívida Bruta GG – nível (% PIB, Δ a/a em p.p.)",
            value=divida_nivel,        # ex.: 78,6% do PIB
            delta=divida_delta_aa,     # Δ vs mesmo mês do ano anterior, em p.p.
            fmt_value="{:.1f}",
            value_is_pct=True,         # valor grande em %
            delta_is_pp=True,          # seta em p.p. (não %)
            badge=badge_divida,        # ex.: "10/2025"
            icon_html=ICON_PERCENT,
            subtext=subtext_divida,    # "há 12m: ... | há 24m: ..."
            info_text=info_divida,
        )


    # 11) Resultado Primário GC – mês (R$ bi, vs a/a)
    with col11:
        dm = dados_macro

        # número grande: resultado primário do mês (R$ bi)
        prim_mes = dm.primario_mes_real_bi

        # setinha: diferença em R$ bi vs MESMO mês do ano anterior
        prim_delta_bi_aa = dm.primario_mes_delta_real_bi_aa

        # acumulado no ano (jan → mês de referência)
        prim_acum_ano = dm.primario_ano_real_bi

        # referência do mês (vem como "10/25" e convertemos para "10/2025")
        prim_ref = getattr(dm, "primario_referencia", None)

        badge_prim = "ano fiscal"
        if prim_ref is not None:
            if isinstance(prim_ref, str):
                # tenta converter de MM/YY -> MM/YYYY
                try:
                    dt_prim = datetime.strptime(prim_ref, "%m/%y")
                    badge_prim = dt_prim.strftime("%m/%Y")
                except ValueError:
                    # se não bater o formato, usa do jeito que veio
                    badge_prim = prim_ref
            else:
                badge_prim = str(prim_ref)

        # subtexto bem curto, pra não aumentar a altura do card
        if prim_acum_ano is not None:
            if prim_acum_ano >= 0:
                subtext_prim = f"Acum. no ano: superávit de R$ {prim_acum_ano:,.1f} bi."
            else:
                subtext_prim = f"Acum. no ano: déficit de R$ {abs(prim_acum_ano):,.1f} bi."


        else:
            subtext_prim = "Acum. no ano indisponível."
        
        # Texto do botão "i" (tooltip), explicando a data do canto superior direito
        if badge_prim and prim_mes is not None:
            info_prim = (
                f"A data no canto superior direito ({badge_prim}) indica o "
                "mês de referência do resultado primário do Governo Central. "
                "O valor do card mostra o resultado primário do mês, em "
                "bilhões de reais (já em termos reais). A variação (Δ) mostra "
                "a diferença, também em bilhões de reais, em relação ao "
                "mesmo mês do ano anterior. O texto abaixo resume o resultado "
                "primário acumulado no ano até o mês de referência."
            )
        else:
            info_prim = (
                "Este card mostra o resultado primário mensal do Governo "
                "Central em bilhões de reais, a diferença em relação ao "
                "mesmo mês do ano anterior e o resultado acumulado no ano."
            )


        metric_card(
            label="Resultado Primário Governo – mês vs a/a",
            value=prim_mes,
            delta=prim_delta_bi_aa,        # Δ em R$ bi vs mesmo mês do ano anterior
            fmt_value="R$ {:,.1f} bi",
            value_is_pct=False,
            delta_is_pct=False,
            delta_is_money=True,           # <<--- aqui
            badge=badge_prim,              # ex.: 10/25
            icon_html=ICON_DOLLAR,
            subtext=subtext_prim,          # "Acum. no ano: déficit de R$ 63,7 bi."
            info_text=info_prim,
        )



    # 12) Balança Comercial - mês vs a/a  (US$ bi)
    with col12:
        resumo_balanca = resumo_balanca_comercial_mensal()

        saldo_mes = resumo_balanca.get("saldo_mes_bi")
        var_mes_pct_aa = resumo_balanca.get("var_mes_pct_aa")
        acum_ano_bi = resumo_balanca.get("acum_ano_bi")
        acum_ano_var_pct = resumo_balanca.get("acum_ano_var_pct")
        ref_bal = resumo_balanca.get("referencia") or "-"

        if saldo_mes is None:
            info_bal = (
                "Este card mostra o saldo mensal da balança comercial em "
                "bilhões de dólares e a variação em relação ao mesmo mês do "
                "ano anterior. Quando os dados não estão disponíveis, o "
                "painel exibe esta mensagem de aviso."
            )

            metric_card(
                label="Balança Comercial - mês vs a/a  (US$ bi)",
                value=0.0,
                delta=0.0,
                fmt_value="US$ {:,.1f} bi",
                value_is_pct=False,
                delta_is_pct=False,
                badge="sem dados",
                icon_html=ICON_DOLLAR,
                subtext="Não foi possível carregar a balança comercial (ver CSV).",
                info_text=info_bal,   # 👈 NOVO
            )

        else:
            # texto pequeno: acumulado no ano + % vs mesmo período do ano anterior
            if acum_ano_bi is not None:
                valor_abs = abs(acum_ano_bi)
                if acum_ano_var_pct is not None:
                    sinal = "+" if acum_ano_var_pct >= 0 else "-"
                    pct_abs = abs(acum_ano_var_pct)
                    # Ex.: "Acum. ano: US$ 45,6 bi (-18,1% a/a)."
                    subtext_bal = (
                        f"Acum. ano: US$ {valor_abs:,.1f} bi ({sinal}{pct_abs:.1f}% a/a)."
                    )
                else:
                    # Ex.: "Acum. ano: US$ 45,6 bi."
                    subtext_bal = f"Acum. ano: US$ {valor_abs:,.1f} bi."

            else:
                subtext_bal = "Acum. no ano indisponível."

            delta_val = var_mes_pct_aa if var_mes_pct_aa is not None else 0.0
            delta_is_pct_flag = var_mes_pct_aa is not None

            # Texto do botão "i" (tooltip), explicando a data do canto superior direito
            if ref_bal and ref_bal != "-":
                info_bal = (
                    f"A data no canto superior direito ({ref_bal}) indica o "
                    "mês de referência do saldo da balança comercial. "
                    "O valor do card mostra o saldo do mês em bilhões de "
                    "dólares. A variação (Δ) indica, em percentual, quanto "
                    "esse saldo mudou em relação ao mesmo mês do ano anterior. "
                    "O texto abaixo resume o saldo acumulado no ano e a "
                    "variação desse acumulado em relação ao mesmo período "
                    "do ano anterior."
                )
            else:
                info_bal = (
                    "Este card mostra o saldo mensal da balança comercial em "
                    "bilhões de dólares, a variação percentual em relação ao "
                    "mesmo mês do ano anterior e o saldo acumulado no ano."
                )


            metric_card(
                label="Balança Comercial - mês vs a/a  (US$ bi)",
                value=saldo_mes,                  # ex.: US$ 5,8 bi
                delta=delta_val,                  # var % vs mesmo mês do ano anterior
                fmt_value="US$ {:,.1f} bi",
                value_is_pct=False,
                delta_is_pct=delta_is_pct_flag,   # seta em %
                badge=ref_bal,                    # badge = mm/aaaa
                icon_html=ICON_DOLLAR,
                subtext=subtext_bal,
                info_text=info_bal,
            )


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
        subtab_indic_br, subtab_exp_br = st.tabs(
            ["Painel", "Histórico"]
        )


        # -------- Indicadores BR --------
        with subtab_indic_br:
            # Termômetro macro – Brasil (12 cards)
            render_bloco_termometro_macro_br()


            # ---------- Ibovespa: dados para o card ----------
            ibov_nivel_atual = None


            if df_ibov_curto is not None and not df_ibov_curto.empty:
                linha_ibov = df_ibov_curto.iloc[0]

                # Ex.: "155.278,00 pts" -> 155278.00
                nivel_str = str(linha_ibov.get("Nível atual", ""))
                try:
                    # tira o " pts" e converte de BR para float Python
                    nivel_str = nivel_str.split(" ")[0]
                    nivel_str = nivel_str.replace(".", "").replace(",", ".")
                    ibov_nivel_atual = float(nivel_str)
                except Exception:
                    ibov_nivel_atual = None

                # Ex.: "+29,26%" -> 29.26
                var_ano_str = str(linha_ibov.get("Var. ano", ""))
                try:
                    var_ano_str = var_ano_str.replace("%", "").replace(",", ".")
                    ibov_var_ano = float(var_ano_str)
                except Exception:
                    ibov_var_ano = None

            # Bloco de cards / visão rápida (já vem com título próprio)

            # ---------- IPCA: resumo p/ card + Focus mensal ----------
            try:
                resumo_ipca = resumo_ipca_com_focus_mensal()
            except Exception:
                resumo_ipca = {
                    "referencia": "-",
                    "mensal": float("nan"),
                    "acum_ano": float("nan"),
                    "acum_12m": float("nan"),
                    "focus_mensal": None,
                    "surpresa_mensal": None,
                }

            # pega os valores e troca NaN por None
            ipca_referencia = resumo_ipca.get("referencia", "-")

            ipca_mensal = resumo_ipca.get("mensal")
            if isinstance(ipca_mensal, float) and math.isnan(ipca_mensal):
                ipca_mensal = None

            ipca_acum_ano = resumo_ipca.get("acum_ano")
            if isinstance(ipca_acum_ano, float) and math.isnan(ipca_acum_ano):
                ipca_acum_ano = None

            ipca_acum_12m = resumo_ipca.get("acum_12m")
            if isinstance(ipca_acum_12m, float) and math.isnan(ipca_acum_12m):
                ipca_acum_12m = None

            ipca_focus_mensal = resumo_ipca.get("focus_mensal")
            ipca_surpresa_mensal = resumo_ipca.get("surpresa_mensal")

            # Bloco de cards / visão rápida (já vem com título próprio)
            #render_bloco_curto_prazo_br(
                #ibov_nivel_atual=ibov_nivel_atual,
                #ibov_var_ano=ibov_var_ano,
                #ipca_mensal=ipca_mensal,
                #ipca_surpresa_mensal=ipca_surpresa_mensal,
                #ipca_focus_mensal=ipca_focus_mensal,
                #ipca_referencia=ipca_referencia,



        # -------- Expectativas BR --------
        with subtab_exp_br:

            # Título só para os QUADROS abaixo (tabelas)
            st.markdown("### Histórico de Indicadores – Brasil")
            st.caption(
                "Quadros complementares aos indicadores do Painel Brasil."
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

            # Inflação
            st.markdown("**Inflação – IPCA**")

            if ipca_mensal is not None:
                valor_mensal_str = f"{ipca_mensal:.2f}%"
                valor_ano_str = (
                    f"{ipca_acum_ano:.2f}%" if ipca_acum_ano is not None else "-"
                )
                valor_12m_str = (
                    f"{ipca_acum_12m:.2f}%" if ipca_acum_12m is not None else "-"
                )
            else:
                valor_mensal_str = "sem dados"
                valor_ano_str = "-"
                valor_12m_str = "-"

            df_ipca_curto = pd.DataFrame(
                [
                    {
                        "Indicador": "IPCA (variação mensal)",
                        "Data ref.": ipca_referencia or "-",
                        "Variação mensal": valor_mensal_str,
                        "Acum. no ano": valor_ano_str,
                        "Acum. 12 meses": valor_12m_str,
                        "Fonte": "IBGE / SIDRA (Tabela 1737)",
                    }
                ]
            )
            st.table(df_ipca_curto.set_index("Indicador"))

            # DI Futuro – contrato ~5 anos (B3)
            st.markdown("**DI Futuro – contrato ~5 anos (B3)**")

            if df_hist_di is None or df_hist_di.empty:
                st.info(
                    "Ainda não há histórico salvo de DI Futuro. "
                    "Rode o app em dias úteis para ir acumulando observações em "
                    "`data/di_futuro/di1_historico.csv`."
                )
            else:
                try:
                    df_di = df_hist_di.copy()
                except Exception:
                    st.info("Não foi possível tratar o histórico de DI Futuro.")
                else:
                    selecao = selecionar_contrato_di_5_anos(df_di)
                    if not selecao:
                        st.info(
                            "Não foi possível encontrar um contrato de DI Futuro "
                            "com vencimento em torno de 5 anos à frente."
                        )
                    else:
                        ticker_alvo = selecao["ticker"]
                        df_di_ticker = selecao["df_ticker"].copy()

                        # garante coluna data como date
                        df_di_ticker["data"] = pd.to_datetime(
                            df_di_ticker["data"], errors="coerce"
                        ).dt.date
                        df_di_ticker = df_di_ticker.dropna(subset=["data", "taxa"])

                        if df_di_ticker.empty:
                            st.info(
                                f"Não encontrei histórico para o contrato {ticker_alvo} "
                                "em `di1_historico.csv`."
                            )
                        else:
                            df_di_ticker = df_di_ticker.sort_values("data")

                            # última data disponível (Data ref.)
                            data_ref = df_di_ticker["data"].max()
                            taxa_atual = float(
                                df_di_ticker.loc[
                                    df_di_ticker["data"] == data_ref, "taxa"
                                ].iloc[-1]
                            )

                            # taxa há 1 semana
                            data_1sem = data_ref - timedelta(days=7)
                            df_sem = df_di_ticker[
                                (df_di_ticker["data"] >= data_1sem)
                                & (df_di_ticker["data"] <= data_ref)
                            ]
                            taxa_1sem = (
                                float(df_sem.sort_values("data").iloc[0]["taxa"])
                                if not df_sem.empty
                                else None
                            )

                            # taxa no início do mês
                            primeiro_dia_mes = data_ref.replace(day=1)
                            df_mes = df_di_ticker[
                                (df_di_ticker["data"] >= primeiro_dia_mes)
                                & (df_di_ticker["data"] <= data_ref)
                            ]
                            taxa_inicio_mes = (
                                float(df_mes.sort_values("data").iloc[0]["taxa"])
                                if not df_mes.empty
                                else None
                            )

                            # variações em p.p.
                            var_sem = (
                                taxa_atual - taxa_1sem
                                if taxa_1sem is not None
                                else None
                            )
                            var_mes = (
                                taxa_atual - taxa_inicio_mes
                                if taxa_inicio_mes is not None
                                else None
                            )

                            def _fmt_pct(v):
                                if v is None or (isinstance(v, float) and pd.isna(v)):
                                    return "-"
                                return f"{v:.2f}% a.a."

                            def _fmt_pp(v):
                                if v is None or (isinstance(v, float) and pd.isna(v)):
                                    return "-"
                                sinal = "+" if v > 0 else ""
                                return f"{sinal}{v:.2f} p.p."

                            linha_di = {
                                "Indicador": f"{ticker_alvo} (B3) – taxa DI Futuro ~5 anos",
                                "Data ref.": data_ref.strftime("%d/%m/%Y"),
                                "Taxa atual": _fmt_pct(taxa_atual),
                                "Taxa há 1 sem": _fmt_pct(taxa_1sem),
                                "Início do mês": _fmt_pct(taxa_inicio_mes),
                                "Var. sem": _fmt_pp(var_sem),
                                "Var. mês": _fmt_pp(var_mes),
                                "Fonte": "Curva DI Futuro B3 (CSV local)",
                            }

                            df_di_curto = pd.DataFrame([linha_di])
                            st.table(df_di_curto.set_index("Indicador"))


            # --- CAGED (saldo de empregos formais) ---
            st.markdown("**Mercado de Trabalho – CAGED (saldo de empregos formais)**")

            resumo_caged = resumo_caged_saldo_novo()

            # se não conseguiu carregar (None em tudo), mostra aviso
            if resumo_caged.get("saldo_atual") is None:
                st.info("Não foi possível carregar o resumo do CAGED.")
            else:
                # helper para deixar tudo em 'mil vagas' com formatação BR
                def _fmt_saldo_mil(v: float | None) -> str:
                    if v is None:
                        return "-"
                    valor_mil = v / 1_000.0
                    return _format_br_number(valor_mil, 1) + " mil"

                linha_caged = {
                    "Indicador": "CAGED – saldo de empregos formais",
                    "Data ref.": resumo_caged.get("referencia") or "-",
                    "Saldo atual": _fmt_saldo_mil(resumo_caged.get("saldo_atual")),
                    "Saldo há 12m": _fmt_saldo_mil(resumo_caged.get("saldo_12m")),
                    "Saldo há 24m": _fmt_saldo_mil(resumo_caged.get("saldo_24m")),
                    "Δ vs 12m": _fmt_saldo_mil(resumo_caged.get("delta_12m")),
                    "Média 5 anos (mesmo mês)": _fmt_saldo_mil(
                        resumo_caged.get("media_mes_5anos")
                    ),
                    "Fonte": "Novo CAGED – Ministério do Trabalho",
                }

                df_caged_curto = pd.DataFrame([linha_caged])
                st.table(df_caged_curto.set_index("Indicador"))
            
            # --- Bloco macro/fiscal: IBC-Br, desemprego, dívida, primário, balança comercial ---

            # Helpers genéricos de formatação
            def _fmt_pct_1(v: float | None) -> str:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return "-"
                return f"{v:.1f}%".replace(".", ",")

            def _fmt_pct_2(v: float | None) -> str:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return "-"
                return f"{v:.2f}%".replace(".", ",")

            def _fmt_pp_1(v: float | None) -> str:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return "-"
                sinal = "+" if v > 0 else ""
                return f"{sinal}{v:.1f} p.p.".replace(".", ",")

            def _fmt_real_bi(v: float | None) -> str:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return "-"
                return "R$ " + _format_br_number(v, 1) + " bi"

            def _fmt_usd_bi(v: float | None) -> str:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return "-"
                return "US$ " + _format_br_number(v, 1) + " bi"

            # Tenta carregar o pacote macro/fiscal completo
            try:
                dados_macro = carregar_dados_macro_fiscal_br()
            except Exception:
                dados_macro = None

            # --- Atividade – IBC-Br ---
            st.markdown("**Atividade – IBC-Br (dessazonalizado)**")
            if (dados_macro is None) or (dados_macro.ibcbr_nivel is None):
                st.info("Não foi possível carregar o IBC-Br dessazonalizado.")
            else:
                ibc_nivel = dados_macro.ibcbr_nivel
                ibc_var_mom = dados_macro.ibcbr_var_mom
                ibc_var_3m = dados_macro.ibcbr_var_3m_dessaz
                ibc_var_aa = dados_macro.ibcbr_var_aa
                ibc_ref = dados_macro.ibcbr_referencia or "-"

                linha_ibc = {
                    "Indicador": "IBC-Br – atividade (SA)",
                    "Data ref.": ibc_ref,
                    "Nível (índice)": _format_br_number(ibc_nivel, 2),
                    "Var. m/m (dessaz.)": _fmt_pct_2(ibc_var_mom),
                    "Var. 3m (dessaz.)": _fmt_pct_2(ibc_var_3m),
                    "Var. a/a (sem ajuste)": _fmt_pct_2(ibc_var_aa),
                    "Fonte": "BCB / SGS (IBC-Br dessazonalizado)",
                }
                df_ibc = pd.DataFrame([linha_ibc])
                st.table(df_ibc.set_index("Indicador"))

            # --- Mercado de Trabalho – Desemprego PNAD ---
            st.markdown("**Mercado de Trabalho – Desemprego (PNAD Contínua)**")
            if (dados_macro is None) or (dados_macro.desemprego_pnad is None):
                st.info("Não foi possível carregar a PNAD Contínua (desemprego).")
            else:
                dm = dados_macro
                desemp_atual = dm.desemprego_pnad
                desemp_12m = dm.desemprego_pnad_12m_atras
                desemp_24m = dm.desemprego_pnad_24m_atras
                desemp_delta_pp_12m = dm.desemprego_delta_pp_12m
                ref_pnad = dm.desemprego_pnad_referencia or "-"

                linha_desemp = {
                    "Indicador": "Desemprego – PNAD Contínua (tri móvel)",
                    "Data ref.": ref_pnad,
                    "Nível atual": _fmt_pct_1(desemp_atual),
                    "Nível há 12m": _fmt_pct_1(desemp_12m),
                    "Nível há 24m": _fmt_pct_1(desemp_24m),
                    "Δ 12m (p.p.)": _fmt_pp_1(desemp_delta_pp_12m),
                    "Fonte": "IBGE – PNAD Contínua (trimestre móvel)",
                }
                df_desemp = pd.DataFrame([linha_desemp])
                st.table(df_desemp.set_index("Indicador"))

            # --- Dívida Bruta do Governo Geral (% PIB) ---
            st.markdown("**Dívida Bruta do Governo Geral (% do PIB)**")
            if (dados_macro is None) or (dados_macro.divida_bruta_pct_pib is None):
                st.info("Não foi possível carregar a série de dívida bruta/PIB.")
            else:
                dm = dados_macro
                div_nivel = dm.divida_bruta_pct_pib
                div_12m = dm.divida_bruta_pct_pib_12m_atras
                div_24m = dm.divida_bruta_pct_pib_24m_atras
                div_ref = dm.divida_bruta_referencia or "-"
                # Δ a/a em p.p. (nível atual – nível de 12m atrás)
                div_delta_aa = None
                if (div_nivel is not None) and (div_12m is not None):
                    div_delta_aa = div_nivel - div_12m

                linha_divida = {
                    "Indicador": "Dívida Bruta Governo Geral (% PIB)",
                    "Data ref.": div_ref,
                    "Nível atual": _fmt_pct_2(div_nivel),
                    "Nível há 12m": _fmt_pct_2(div_12m),
                    "Nível há 24m": _fmt_pct_2(div_24m),
                    "Δ a/a (p.p.)": _fmt_pp_1(div_delta_aa),
                    "Fonte": "BCB / SGS (Dívida Bruta GG)",
                }
                df_divida = pd.DataFrame([linha_divida])
                st.table(df_divida.set_index("Indicador"))

            # --- Resultado Primário – Governo Central (R$ bi) ---
            st.markdown("**Resultado Primário do Governo Central (R$ bi)**")
            if (dados_macro is None) or (dados_macro.primario_mes_real_bi is None):
                st.info("Não foi possível carregar o resultado primário do Governo Central.")
            else:
                dm = dados_macro
                prim_mes = dm.primario_mes_real_bi
                prim_delta_mes_aa = dm.primario_mes_delta_real_bi_aa
                prim_acum_ano = dm.primario_ano_real_bi
                prim_ref = getattr(dm, "primario_referencia", None) or "-"

                linha_prim = {
                    "Indicador": "Resultado primário – mês (nominal)",
                    "Data ref.": prim_ref,
                    "Resultado do mês": _fmt_real_bi(prim_mes),
                    "Δ vs mesmo mês a/a": _fmt_real_bi(prim_delta_mes_aa),
                    "Acum. no ano": _fmt_real_bi(prim_acum_ano),
                    "Fonte": "Tesouro Nacional / STN (resultado primário nominal)",
                }
                df_prim = pd.DataFrame([linha_prim])
                st.table(df_prim.set_index("Indicador"))

            # --- Setor Externo – Balança Comercial (US$) ---
            st.markdown("**Balança Comercial (US$)**")
            try:
                resumo_balanca = resumo_balanca_comercial_mensal()
            except Exception:
                resumo_balanca = None

            if (resumo_balanca is None) or (resumo_balanca.get("saldo_mes_bi") is None):
                st.info("Não foi possível carregar o resumo mensal da balança comercial.")
            else:
                ref_bal = resumo_balanca.get("referencia") or "-"
                saldo_mes_bi = resumo_balanca.get("saldo_mes_bi")
                var_mes_pct_aa = resumo_balanca.get("var_mes_pct_aa")
                acum_ano_bi = resumo_balanca.get("acum_ano_bi")
                acum_ano_var_pct = resumo_balanca.get("acum_ano_var_pct")

                linha_balanca = {
                    "Indicador": "Balança comercial – saldo do mês",
                    "Data ref.": ref_bal,
                    "Saldo do mês": _fmt_usd_bi(saldo_mes_bi),
                    "Var. mês vs a/a": _fmt_pct_2(var_mes_pct_aa),
                    "Acum. no ano": _fmt_usd_bi(acum_ano_bi),
                    "Var. acum. ano vs a/a": _fmt_pct_2(acum_ano_var_pct),
                    "Fonte": "BCB – Balança Comercial (SISBACEN/SGS)",
                }
                df_balanca = pd.DataFrame([linha_balanca])
                st.table(df_balanca.set_index("Indicador"))


            # --- Risco-País – spread 10Y (Brasil/USA) ---
            st.markdown("**Risco-País – spread 10Y (Brasil/USA)**")

            (
                nivel_atual,
                _delta_aa,
                referencia,
                _media_12m,
                _delta_d1,
                inicio_mes,
                data_inicio_mes,
            ) = carregar_risco_brasil_spread_10y()

            if nivel_atual is None:
                st.info("Não foi possível carregar o spread 10Y Brasil/USA para o histórico.")
            else:
                # Lê o CSV completo para calcular o nível há 1 semana
                try:
                    df_risco = pd.read_csv(CSV_SPREAD)
                    df_risco["data"] = pd.to_datetime(
                        df_risco["data"], errors="coerce"
                    ).dt.date
                    df_risco = df_risco.dropna(subset=["data", "spread_pb"]).copy()
                    df_risco = df_risco.sort_values("data").reset_index(drop=True)
                except Exception:
                    df_risco = pd.DataFrame()

                if df_risco.empty:
                    st.info(
                        "Não foi possível carregar o histórico detalhado do spread 10Y."
                    )
                else:
                    data_ult = df_risco.iloc[-1]["data"]

                    # alvo: 1 semana atrás
                    alvo_sem = data_ult - timedelta(days=7)
                    df_sem = df_risco[df_risco["data"] <= alvo_sem]

                    nivel_1sem = (
                        float(df_sem.iloc[-1]["spread_pb"]) if not df_sem.empty else None
                    )

                    # variações
                    var_sem = (
                        float(nivel_atual - nivel_1sem)
                        if (nivel_1sem is not None)
                        else None
                    )
                    var_mes = (
                        float(nivel_atual - inicio_mes)
                        if (inicio_mes is not None)
                        else None
                    )

                    # helpers de formatação
                    def _fmt_nivel_pb(v: float | None) -> str:
                        if v is None:
                            return "-"
                        return f"{int(round(v))} p.b."

                    def _fmt_var_pb(v: float | None) -> str:
                        if v is None:
                            return "-"
                        n = int(round(v))
                        sinal = "+" if n > 0 else ""
                        return f"{sinal}{n} p.b."

                    linha_risco = {
                        "Indicador": "Spread 10Y Brasil/USA",
                        "Data ref.": referencia or data_ult.strftime("%d/%m/%Y"),
                        "Nível atual": _fmt_nivel_pb(nivel_atual),
                        "Nível há 1 sem": _fmt_nivel_pb(nivel_1sem),
                        "Início do mês": _fmt_nivel_pb(inicio_mes),
                        "Var. sem": _fmt_var_pb(var_sem),
                        "Var. mês": _fmt_var_pb(var_mes),  # vs início do mês
                        "Fonte": "Curva soberana BR10Y – US10Y (CSV local)",
                    }

                    df_risco_curto = pd.DataFrame([linha_risco])
                    st.table(df_risco_curto.set_index("Indicador"))


    # ==========================
    # ABA MUNDO
    # ==========================
    with tab_mundo:
        subtab_indic_world, subtab_exp_world = st.tabs(
            ["Painel", "Histórico"]
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
    if df_ativ is None or df_ativ.empty:
        st.info("Ainda não há dados de atividade econômica disponíveis.")
        return

    st.markdown("### Atividade econômica – Indicadores (IBGE/FGV)")
    st.caption("Indicadores classificados em antecedentes, coincidentes e defasados do ciclo econômico.")

    with st.container(border=True):
        st.markdown("##### Classificação cíclica dos indicadores")

        # -----------------------------
        # Helpers locais (não mexe no resto do projeto)
        # -----------------------------
        def _fmt_pts(x, signed=False):
            if x is None:
                return "•"
            try:
                v = float(x)
            except Exception:
                return "•"
            s = f"{v:+.1f}" if signed else f"{v:.1f}"
            return f"{s.replace('.', ',')} pts"
        

        # --- CSS simples do ícone/tooltip (usa o tooltip nativo do browser via title="...") ---
        st.markdown(
            """
        <style>
        /* mantém o look da tabela padrão (classe dataframe já é usada abaixo) */
        table.dataframe { width: 100%; }

        /* ícone de informação */
        .ion-info {
        margin-left: 6px;
        font-size: 0.92em;
        opacity: 0.75;
        cursor: help;
        }
        .ion-info:hover { opacity: 1; }
        </style>
        """,
            unsafe_allow_html=True,
        )

        # --- Descrição curta (aparece no tooltip) ---
        _FGV_DESC = {
            "ICC": "ICC: Confiança do consumidor (percepção atual + expectativas).",
            "ICE": "ICE: Confiança empresarial (síntese de indústria, serviços, comércio e construção).",
            "ICI": "ICI: Confiança da indústria (situação atual + expectativas).",
            "ICS": "ICS: Confiança de serviços (situação atual + expectativas).",
            "ICOM": "ICOM: Confiança do comércio (situação atual + expectativas).",
            "ICST": "ICST: Confiança da construção (situação atual + expectativas).",
        }

        def _fgv_stats_22plus(sigla: str):
            """
            Retorna estatísticas do nível (pontos) na janela >= 2022-01.
            Usa consolidado: data/sondagens_fgv/sondagens_fgv_consolidado.csv
            (mesmo CSV que você já confirmou que existe e tem colunas.)
            """
            try:
                if not sigla:
                    return None
                sig = str(sigla).upper()
                cut = pd.Timestamp("2022-01-01")

                # cache simples para não reler CSV toda hora
                if not hasattr(_fgv_stats_22plus, "_cache"):
                    _fgv_stats_22plus._cache = {}
                cache = _fgv_stats_22plus._cache

                if "dfc" not in cache:
                    arq_c = DATA_DIR / "sondagens_fgv" / "sondagens_fgv_consolidado.csv"
                    if arq_c.exists() and arq_c.stat().st_size > 0:
                        try:
                            cache["dfc"] = pd.read_csv(arq_c)
                        except Exception:
                            cache["dfc"] = None
                    else:
                        cache["dfc"] = None

                dfc = cache.get("dfc")
                if dfc is None or dfc.empty:
                    return None

                df = dfc[dfc["sigla"].astype(str).str.upper() == sig].copy()
                if df.empty:
                    return None

                df["mes_ref"] = pd.to_datetime(df["mes_ref"], errors="coerce")
                df["pontos"] = pd.to_numeric(df["pontos"], errors="coerce")
                df = df.dropna(subset=["mes_ref", "pontos"]).sort_values("mes_ref")
                win = df[df["mes_ref"] >= cut].copy()
                if win.empty:
                    return None

                # min / max com o mês
                i_min = win["pontos"].idxmin()
                i_max = win["pontos"].idxmax()
                r_min = win.loc[i_min]
                r_max = win.loc[i_max]

                out = {
                    "n": int(len(win)),
                    "mean": float(win["pontos"].mean()),
                    "min": float(r_min["pontos"]),
                    "min_mes": pd.to_datetime(r_min["mes_ref"]).strftime("%m/%Y"),
                    "max": float(r_max["pontos"]),
                    "max_mes": pd.to_datetime(r_max["mes_ref"]).strftime("%m/%Y"),
                }
                return out
            except Exception:
                return None

        def _tooltip_text(sigla: str) -> str:
            sig = str(sigla).upper()
            desc = _FGV_DESC.get(sig, f"{sig}: Índice de confiança (FGV).")
            stt = _fgv_stats_22plus(sig)
            if not stt:
                return desc
            # Quebra de linha no tooltip: usar &#10; no HTML depois
            return (
                f"{desc}\n"
                f"Pós-2022 (n={stt['n']}): média {stt['mean']:.1f} | "
                f"mín {stt['min']:.1f} ({stt['min_mes']}) | "
                f"máx {stt['max']:.1f} ({stt['max_mes']})"
            )

        def _tooltip_attr(sigla: str) -> str:
            # escapa aspas e transforma \n em quebra de linha do tooltip HTML
            txt = _tooltip_text(sigla)
            return _html.escape(txt, quote=True).replace("\n", "&#10;")

        def _render_table_html(df_: pd.DataFrame):
            # mantém classe dataframe para herdar o CSS do tema
            html_table = df_.to_html(index=False, escape=False, classes="dataframe")
            st.markdown(html_table, unsafe_allow_html=True)


        def _fmt_pct(x, digits=1, signed=False):
            if x is None:
                return "•"
            try:
                v = float(x)
            except Exception:
                return "•"

            s = f"{v:+.{digits}f}" if signed else f"{v:.{digits}f}"
            return f"{s.replace('.', ',')}%"


        def _fgv_percentil_22plus(sigla: str):
            """
            Percentil do nível atual dentro da amostra pós-pandemia (>= 2022-01).
            Usa consolidado: data/sondagens_fgv/sondagens_fgv_consolidado.csv
            Fallback:        data/sondagens_fgv/{sigla}_fgv.csv
            """
            try:
                if not sigla:
                    return None
                sig = str(sigla).upper()
                cut = pd.Timestamp("2022-01-01")

                # cache (reaproveita o mesmo padrão do delta)
                if not hasattr(_fgv_percentil_22plus, "_cache"):
                    _fgv_percentil_22plus._cache = {}
                cache = _fgv_percentil_22plus._cache

                df = None

                # 1) consolidado
                if "dfc" not in cache:
                    arq_c = DATA_DIR / "sondagens_fgv" / "sondagens_fgv_consolidado.csv"
                    if arq_c.exists() and arq_c.stat().st_size > 0:
                        try:
                            cache["dfc"] = pd.read_csv(arq_c)
                        except Exception:
                            cache["dfc"] = None
                    else:
                        cache["dfc"] = None

                dfc = cache.get("dfc")
                if dfc is not None and not dfc.empty and "sigla" in dfc.columns:
                    dfx = dfc[dfc["sigla"].astype(str).str.upper() == sig].copy()
                    if not dfx.empty:
                        df = dfx

                # 2) fallback individual
                if df is None:
                    arq = DATA_DIR / "sondagens_fgv" / f"{sig.lower()}_fgv.csv"
                    if not arq.exists() or arq.stat().st_size == 0:
                        return None
                    df = pd.read_csv(arq)

                if df is None or df.empty or "mes_ref" not in df.columns or "pontos" not in df.columns:
                    return None

                df["mes_ref"] = pd.to_datetime(df["mes_ref"], errors="coerce")
                df["pontos"] = pd.to_numeric(df["pontos"], errors="coerce")
                df = df.dropna(subset=["mes_ref", "pontos"]).sort_values("mes_ref")

                if df.empty:
                    return None

                # nível atual = último mês disponível
                v_atual = float(df.iloc[-1]["pontos"])

                # janela pós-pandemia
                win = df[df["mes_ref"] >= cut]
                if win.empty:
                    return None

                x = win["pontos"].astype(float)
                n = len(x)
                if n < 12:  # evita percentil “mentiroso” com amostra muito curta
                    return None

                pct = 100.0 * (x.le(v_atual).sum() / n)
                return pct
            except Exception:
                return None

        def _quartil_label_from_pct_top(pct: float) -> str:
            """
            Aqui '1º quartil' = TOP 25% (melhor), como você quer na tela.
            pct = percentil (0..100), onde 100 = topo (muito alto), 0 = fundo (muito baixo).
            """
            if pct is None or (isinstance(pct, float) and math.isnan(pct)):
                return "•"
            pct = float(pct)

            if pct >= 75:
                return "1º quartil (muito forte)"
            if pct >= 50:
                return "2º quartil (forte)"
            if pct >= 25:
                return "3º quartil (fraco)"
            return "4º quartil (muito fraco)"

        def _fgv_stats_22plus(sigla: str):
            """
            Retorna min/média/máx e n na janela 2022+ (mesma janela do percentil).
            Usa o consolidado: data/sondagens_fgv/sondagens_fgv_consolidado.csv
            """
            try:
                if not sigla:
                    return None
                sig = str(sigla).upper()
                cut = pd.Timestamp("2022-01-01")

                # reaproveita o mesmo cache do percentil, se existir
                dfc = None
                if hasattr(_fgv_percentil_22plus, "_cache"):
                    dfc = getattr(_fgv_percentil_22plus, "_cache", {}).get("dfc")

                if dfc is None:
                    arq_c = DATA_DIR / "sondagens_fgv" / "sondagens_fgv_consolidado.csv"
                    if not arq_c.exists() or arq_c.stat().st_size == 0:
                        return None
                    dfc = pd.read_csv(arq_c)

                dfx = dfc[dfc["sigla"].astype(str).str.upper() == sig].copy()
                if dfx.empty:
                    return None

                dfx["mes_ref"] = pd.to_datetime(dfx["mes_ref"], errors="coerce")
                dfx = dfx.dropna(subset=["mes_ref"])
                dfx = dfx[dfx["mes_ref"] >= cut].copy()
                if dfx.empty:
                    return None

                vals = pd.to_numeric(dfx["pontos"], errors="coerce").dropna()
                if vals.empty:
                    return None

                return {
                    "n": int(vals.shape[0]),
                    "min": float(vals.min()),
                    "mean": float(vals.mean()),
                    "max": float(vals.max()),
                }
            except Exception:
                return None

        _FGV_DESC = {
            "ICE": "Índice de Confiança Empresarial (síntese de confiança do setor empresarial).",
            "ICC": "Índice de Confiança do Consumidor (humor das famílias/consumo).",
            "ICI": "Índice de Confiança da Indústria (humor da indústria).",
            "ICS": "Índice de Confiança de Serviços (humor do setor de serviços).",
            "ICOM": "Índice de Confiança do Comércio (humor do varejo/comércio).",
            "ICST": "Índice de Confiança da Construção (humor da construção).",
        }

        _COINCIDENTE_DESC = {
            "NUCI (CNI)": "NUCI: Nível de Utilização da Capacidade Instalada (CNI). Mede o percentual de uso da capacidade produtiva da indústria brasileira.",
            "IBC-Br (BCB)": "IBC-Br: Índice de Atividade Econômica do Banco Central. Indicador coincidente que estima a atividade econômica mensal.",
            "Indústria (PIM-PF) – produção física": "PIM-PF: Pesquisa Industrial Mensal - Produção Física (IBGE). Volume de produção industrial mensal.",
            "Serviços (PMS) – volume": "PMS: Pesquisa Mensal de Serviços (IBGE). Volume de serviços prestados mensalmente.",
            "Varejo (PMC) – volume": "PMC: Pesquisa Mensal do Comércio (IBGE). Volume de vendas no varejo mensal.",
        }

        def _with_tooltip_indicador(label: str, sigla: str) -> str:
            """
            Retorna HTML com tooltip (title) + ícone ⓘ visível.
            """
            sig = (str(sigla).upper() if sigla else "")
            desc = _FGV_DESC.get(sig, "Índice de confiança (FGV). Valores mais altos indicam maior confiança.")

            stt = _fgv_stats_22plus(sig)  # pode ter min/mean/max (e às vezes mês)
            if stt:
                # tenta pegar mês do min/max se existir
                min_txt = f"{stt['min']:.1f}" + (f" ({stt.get('min_mes')})" if stt.get("min_mes") else "")
                max_txt = f"{stt['max']:.1f}" + (f" ({stt.get('max_mes')})" if stt.get("max_mes") else "")
                tip = (
                    f"{sig} — {desc}\n"
                    f"Janela: 2022+ (n={stt['n']})\n"
                    f"Mín: {min_txt} | Média: {stt['mean']:.1f} | Máx: {max_txt}"
                )
            else:
                tip = f"{sig} — {desc}"

            tip_html = html.escape(tip).replace("\n", "&#10;")
            label_html = html.escape(str(label))

            return (
                f"<span class='fgv-ind' title='{tip_html}'>{label_html}</span>"
                f"<span class='fgv-info' title='{tip_html}'>ⓘ</span>"
            )


        def _with_tooltip_indicador_coincidente(label: str) -> str:
            """
            Retorna HTML com tooltip para indicadores coincidentes.
            """
            desc = _COINCIDENTE_DESC.get(label, "Indicador de atividade econômica coincidente.")
            tip = f"{label} — {desc}"
            tip_html = html.escape(tip).replace("\n", "&#10;")
            label_html = html.escape(str(label))

            return (
                f"<span class='fgv-ind' title='{tip_html}'>{label_html}</span>"
                f"<span class='fgv-info' title='{tip_html}'>ⓘ</span>"
            )


        def _render_table_html(df: pd.DataFrame):
            """
            Renderiza tabela HTML (permite tooltip por célula).
            """
            css = """
            <style>
            .fgv_tbl table { width: 100%; border-collapse: collapse; }
            .fgv_tbl th, .fgv_tbl td { padding: 10px 12px; border-bottom: 1px solid rgba(255,255,255,0.08); }
            .fgv_tbl th { font-weight: 600; text-align: left; }
            .fgv_tbl tr:hover td { background: rgba(255,255,255,0.03); }
            .fgv_tbl tr.quartil-forte { background-color: rgba(0,255,0,0.1); }  /* Verde claro para forte */
            .fgv_tbl tr.quartil-fraco { background-color: rgba(255,0,0,0.1); }  /* Vermelho claro para fraco */

            /* tooltip / ícone */
            .fgv-ind { cursor: help; }
            .fgv-info{
                display: inline-block;
                margin-left: 6px;
                font-size: 12px;
                line-height: 1;
                opacity: 0.75;
                cursor: help;
                user-select: none;
            }
            .fgv-info:hover { opacity: 1; }
            </style>
            """
            st.markdown(css, unsafe_allow_html=True)
            
            # Adicionar classe de linha baseada no quartil
            def _add_row_class(row):
                quartil = str(row["Quartil (2022+)"])
                if "1º" in quartil or "2º" in quartil:
                    return "quartil-forte"
                elif "3º" in quartil or "4º" in quartil:
                    return "quartil-fraco"
                return ""
            
            df = df.copy()
            df["row_class"] = df.apply(_add_row_class, axis=1)
            
            # Renderizar HTML com classes
            html_rows = []
            for _, row in df.iterrows():
                row_class = row["row_class"]
                cells = [f"<td>{cell}</td>" for cell in row.drop("row_class")]
                html_rows.append(f"<tr class='{row_class}'>{' '.join(cells)}</tr>")
            
            headers = [f"<th>{col}</th>" for col in df.columns if col != "row_class"]
            html_table = f"<table class='dataframe'><thead><tr>{' '.join(headers)}</tr></thead><tbody>{' '.join(html_rows)}</tbody></table>"
            
            st.markdown(f"<div class='fgv_tbl'>{html_table}</div>", unsafe_allow_html=True)



        def _quartil_forca_from_pct(pct: float) -> str:
            # pct é "percentil por baixo": 0 = muito baixo, 100 = muito alto
            if pct is None:
                return "•"

            # Queremos: TOP 25% = 1º quartil (muito forte)
            if pct >= 75:
                return "1º quartil (muito forte)"
            elif pct >= 50:
                return "2º quartil (forte)"
            elif pct >= 25:
                return "3º quartil (fraco)"
            else:
                return "4º quartil (muito fraco)"
            

        def _extract_sigla(indicador: str):
            # pega (ICC), (ICE), etc dentro do texto "Confiança ... (ICC) – nível"
            m = re.search(r"\((ICC|ICI|ICS|ICOM|ICST|ICE)\)", str(indicador))
            return m.group(1) if m else None

        def _fgv_delta_pts(sigla: str, meses_atras: int):
            """Calcula (pontos do último mês) - (pontos de N meses atrás).

            Preferência: data/sondagens_fgv/sondagens_fgv_consolidado.csv
            Fallback:    data/sondagens_fgv/{sigla_lower}_fgv.csv
            """
            try:
                if not sigla:
                    return None

                sig = str(sigla).upper()

                # cache (evita reler CSV a cada linha)
                if not hasattr(_fgv_delta_pts, "_cache"):
                    _fgv_delta_pts._cache = {}
                cache = _fgv_delta_pts._cache

                df = None

                # 1) consolidado
                if "dfc" not in cache:
                    arq_c = DATA_DIR / "sondagens_fgv" / "sondagens_fgv_consolidado.csv"
                    if arq_c.exists() and arq_c.stat().st_size > 0:
                        try:
                            cache["dfc"] = pd.read_csv(arq_c)
                        except Exception:
                            cache["dfc"] = None
                    else:
                        cache["dfc"] = None

                dfc = cache.get("dfc", None)
                if dfc is not None and not dfc.empty and "sigla" in dfc.columns:
                    dfx = dfc[dfc["sigla"].astype(str).str.upper() == sig].copy()
                    if dfx is not None and not dfx.empty:
                        df = dfx

                # 2) fallback individual
                if df is None:
                    arq = DATA_DIR / "sondagens_fgv" / f"{sig.lower()}_fgv.csv"
                    if not arq.exists() or arq.stat().st_size == 0:
                        return None
                    df = pd.read_csv(arq)

                if df is None or df.empty or "mes_ref" not in df.columns or "pontos" not in df.columns:
                    return None

                df["mes_ref"] = pd.to_datetime(df["mes_ref"], errors="coerce")
                df = df.dropna(subset=["mes_ref"]).sort_values("mes_ref")
                if df.empty:
                    return None
                
                df["pontos"] = pd.to_numeric(df["pontos"], errors="coerce")
                df = df.dropna(subset=["pontos"])
                if df.empty:
                    return None

                df["per"] = df["mes_ref"].dt.to_period("M")
                per_last = df["per"].iloc[-1]
                per_target = per_last - int(meses_atras)

                row_last = df.iloc[-1]
                row_target = df[df["per"] == per_target]
                if row_target.empty:
                    return None

                v_last = float(row_last["pontos"])
                v_prev = float(row_target.iloc[-1]["pontos"])
                return v_last - v_prev
            except Exception:
                return None
        # -----------------------------
        # Base
        # -----------------------------
        df_base = df_ativ.copy()

        # Garante colunas esperadas (evita quebrar se alguma vier faltando)
        for col in ["Indicador", "Classificação", "Mês ref.", "Var. mensal", "Acum. no ano", "Acum. 12 meses", "Fonte"]:
            if col not in df_base.columns:
                df_base[col] = "•"

        # Separa por tipo (sem radios / sem filtros)
        df_ant = df_base[df_base["Classificação"].astype(str).str.contains("Antecedente", na=False)].copy()
        df_coi = df_base[df_base["Classificação"].astype(str).str.contains("Coincidente", na=False)].copy()
        df_def = df_base[df_base["Classificação"].astype(str).str.contains("Defasado", na=False)].copy()

        # -----------------------------
        # 1) ANTECEDENTES (FGV) — colunas “de gestor”
        #    - Nível
        #    - Δ m/m (pts)
        #    - Δ 3m (pts)
        #    - Δ 12m (pts)
        # -----------------------------
        if not df_ant.empty:
            df_ant["sigla"] = df_ant["Indicador"].apply(_extract_sigla)

            df_ant["Δ 3m"] = df_ant["sigla"].apply(lambda s: _fmt_pts(_fgv_delta_pts(s, 3), signed=True) if s else "•")
            df_ant["Δ 12m"] = df_ant["sigla"].apply(lambda s: _fmt_pts(_fgv_delta_pts(s, 12), signed=True) if s else "•")
            
            # percentil numérico (0..100) e quartil (onde 1º = TOP 25%)
            df_ant["_pct22"] = df_ant["sigla"].apply(lambda s: _fgv_percentil_22plus(s) if s else None)
            df_ant["Quartil (2022+)"] = df_ant["_pct22"].apply(_quartil_label_from_pct_top)
            
            # --- FIX: evita duplicar "Nível" quando o df_ativ já vem com essa coluna ---
            if ("Nível" in df_ant.columns) and ("Acum. 12 meses" in df_ant.columns):
                # Para FGV, o "nível" que você está usando é o que está em "Acum. 12 meses"
                # (você renomeia ela para "Nível" logo abaixo). Então removemos o "Nível" pré-existente.
                df_ant = df_ant.drop(columns=["Nível"])

            # Seu dataframe “cru” usa "Acum. 12 meses" para guardar o nível (FGV)
            df_ant_view = df_ant.copy()

            # 1) Renomeia só o que não conflita
            df_ant_view = df_ant_view.rename(
                columns={
                    "Var. mensal": "Δ m/m",
                    "Acum. no ano": "No ano (YTD)",
                }
            )

            # 2) Garante que "Nível" vai existir UMA vez só
            if "Nível" in df_ant_view.columns:
                df_ant_view = df_ant_view.drop(columns=["Nível"])

            df_ant_view["Nível"] = df_ant_view["Acum. 12 meses"]


            # tooltip no nome do indicador (usa sigla da própria linha)
            df_ant_view["Indicador"] = df_ant_view.apply(
                lambda r: _with_tooltip_indicador(r["Indicador"], r.get("sigla")),
                axis=1
            )

            # Mantém só o que faz sentido para antecedente
            df_ant_view = df_ant_view[
                ["Indicador", "Mês ref.", "Δ m/m", "Δ 3m", "Δ 12m", "Quartil (2022+)", "Nível", "Fonte"]
            ].copy()

            # Ordenação simples e estável
            # --- FIX: evita erro pandas "The column label 'Nível' is not unique"
            # 1) normaliza nomes (trim) e 2) remove duplicadas mantendo a última (normalmente a mais "recente/correta")
            df_ant_view.columns = pd.Index(df_ant_view.columns).map(lambda x: str(x).strip())
            if df_ant_view.columns.duplicated().any():
                df_ant_view = df_ant_view.loc[:, ~df_ant_view.columns.duplicated(keep="last")].copy()

            # Ordenação simples e estável (só usa 'Nível' se existir e estiver único)
            sort_cols = ["Mês ref.", "Fonte"]
            ascending = [False, True]
            if "Nível" in df_ant_view.columns and list(df_ant_view.columns).count("Nível") == 1:
                sort_cols.append("Nível")
                ascending.append(False)

            df_ant_view = df_ant_view.sort_values(sort_cols, ascending=ascending)

            st.markdown("**Antecedentes (Confiança – FGV)**")
            _render_table_html(df_ant_view)


        
        # -----------------------------
        # 2) COINCIDENTES (Atividade – IBGE/BCB/CNI) — “gestor-like”
        #
        # Ideia:
        # - Separar claramente *nível* (estado do ciclo) de *momentum* (direção).
        # - Quartil (2022+) é calculado sobre o **nível** da série (ou índice sintético base 2022=100, no caso IBGE).
        # - Δ m/m / Δ 3m / YTD / 12m ficam como “momentum”.
        #
        # Obs.: Para IBGE (PIM/PMS/PMC), o “Nível” é um índice sintético base 2022-01=100 (derivado de var_mom),
        #       para permitir comparação rápida com a média 2022+ sem depender de um índice oficial no CSV.
        # -----------------------------
        if not df_coi.empty:

            # Carrega/resume séries offline (quando disponíveis)
            nuci = _resumo_nuci_gestor_like()
            ibc = _resumo_ibcbr_gestor_like()
            pim = _resumo_ibge_gestor_like("PIM", PIM_CSV)
            pms = _resumo_ibge_gestor_like("PMS", PMS_CSV)
            pmc = _resumo_ibge_gestor_like("PMC", PMC_CSV)

            def _row(indicador: str, ref: str, mm: str, d3: str, d12: str, quartil: str, fonte: str, nivel: str = None) -> dict:
                row = {
                    "Indicador": indicador,
                    "Mês ref.": ref or "•",
                    "Δ m/m": mm,
                    "Δ 3m": d3,
                    "Δ 12m": d12,
                    "Quartil (2022+)": quartil,
                    "Fonte": fonte,
                }
                if nivel is not None:
                    row["Nível"] = nivel
                return row

            rows = []

            # NUCI (p.p. + nível %)
            if nuci.get("referencia"):
                pct22 = nuci.get("pct22")
                quartil = _quartil_label_from_pct_top(pct22)
                # tooltip com média 2022+
                try:
                    df_nuci = carregar_nuci_csv()
                    mu = float(df_nuci[df_nuci["data"] >= pd.Timestamp("2022-01-01")]["valor"].mean())
                    tip = f"Média 2022+: {mu:.1f}%"
                except Exception:
                    tip = "Média 2022+: •"
                indicador = "NUCI (CNI)"
                rows.append(
                    _row(
                        indicador=indicador,
                        ref=nuci.get("referencia"),
                        mm=_fmt_pp(nuci.get("mm_pp"), digits=1, signed=True),
                        d3=_fmt_pp(nuci.get("d3_pp"), digits=1, signed=True),
                        d12=_fmt_pp(nuci.get("d12_pp"), digits=1, signed=True),
                        quartil=quartil,
                        nivel=f'{_fmt_num(nuci.get("nivel"), digits=1)}%',
                        fonte="CNI (NUCI) – via CSV local",
                    )
                )

            # IBC-Br (nível índice + variações %)
            if ibc.get("referencia"):
                pct22 = ibc.get("pct22")
                quartil = _quartil_label_from_pct_top(pct22)
                try:
                    df_ibc = _carregar_ibcbr_csv_offline()
                    mu = float(df_ibc[df_ibc["data"] >= pd.Timestamp("2022-01-01")]["valor"].mean())
                    tip = f"Média 2022+: {mu:.1f}"
                except Exception:
                    tip = "Média 2022+: •"
                indicador = "IBC-Br (BCB)"
                rows.append(
                    _row(
                        indicador=indicador,
                        ref=ibc.get("referencia"),
                        mm=_fmt_pct(ibc.get("mm_pct"), digits=2, signed=False),
                        d3=_fmt_pct(ibc.get("d3_pct"), digits=2, signed=False),
                        d12=_fmt_pct(ibc.get("d12_pct"), digits=2, signed=False),
                        quartil=quartil,
                        nivel=_fmt_num(ibc.get("nivel"), digits=1),
                        fonte="BCB (CSV offline)",
                    )
                )

            # IBGE (índice sintético base 2022=100)
            def _ibge_row(nome: str, r: dict, fonte: str) -> None:
                if not r.get("referencia"):
                    return
                quartil = _quartil_label_from_pct_top(r.get("pct22"))
                indicador = nome
                rows.append(
                    _row(
                        indicador=indicador,
                        ref=r.get("referencia"),
                        mm=_fmt_pct(r.get("mm_pct"), digits=1, signed=False),
                        d3=_fmt_pct(r.get("d3_pct"), digits=2, signed=False),
                        d12=_fmt_pct(r.get("d12_pct"), digits=1, signed=False),
                        quartil=quartil,
                        fonte=fonte,
                    )
                )

            _ibge_row("Indústria (PIM-PF) – produção física", pim, "IBGE / PIM-PF (CSV offline)")
            _ibge_row("Serviços (PMS) – volume", pms, "IBGE / PMS (CSV offline)")
            _ibge_row("Varejo (PMC) – volume", pmc, "IBGE / PMC (CSV offline)")

            df_coi_view = pd.DataFrame(rows)

            # Adiciona tooltip aos indicadores
            df_coi_view["Indicador"] = df_coi_view["Indicador"].apply(_with_tooltip_indicador_coincidente)

            # ordena para ficar “leitura de gestor”: quartil mais forte primeiro, depois Δ 12m
            if not df_coi_view.empty:
                # Mapeie quartil para numérico (1º = 4, 2º = 3, etc.)
                quartil_order = {"1º quartil (muito forte)": 4, "2º quartil (forte)": 3, "3º quartil (fraco)": 2, "4º quartil (muito fraco)": 1}
                df_coi_view["quartil_num"] = df_coi_view["Quartil (2022+)"].map(quartil_order).fillna(0)
                df_coi_view = df_coi_view.sort_values(["quartil_num", "Δ 12m"], ascending=[False, False]).drop(columns=["quartil_num"])

            st.markdown("**Coincidentes (Atividade – IBGE)**")
            st.markdown(_render_table_html(df_coi_view), unsafe_allow_html=True)
            st.caption("💡 Quartil calculado sobre Δ 12m (2022+) para priorizar direção do ciclo econômico.")
        # -----------------------------
        # 3) DEFASADOS (se você adicionar depois)
        # -----------------------------
        if not df_def.empty:
            df_def_view = df_def.rename(
                columns={
                    "Var. mensal": "Δ m/m",
                    "Acum. 12 meses": "12m",
                    "Acum. no ano": "No ano (YTD)",
                }
            )
            df_def_view = df_def_view[["Indicador", "Mês ref.", "Δ m/m", "No ano (YTD)", "12m", "Fonte"]].copy()
            df_def_view = df_def_view.sort_values(["Indicador"])

            st.markdown("**Defasados**")
            df_def_view.index = [""] * len(df_def_view)
            st.table(df_def_view)




def render_bloco_expectativas_focus(
    df_focus: pd.DataFrame,
    df_focus_top5: pd.DataFrame,
):
    """Bloco de expectativas de mercado (Focus) – Brasil."""

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

    # funçãozinha auxiliar p/ formatar a data em texto
    def _fmt_data(d):
        if d is None or pd.isna(d):
            return "sem data disponível"
        try:
            return pd.to_datetime(d).strftime("%d/%m/%Y")
        except Exception:
            return str(d)

    data_mediana_txt = _fmt_data(data_mediana)
    data_top5_txt = _fmt_data(data_top5)

    # ---------- Focus – Mediana ----------
    st.markdown("**Focus – Mediana (consenso do mercado)**")
    st.caption(
        "Mediana das projeções de todas as instituições participantes "
        f"do boletim Focus. Dados de {data_mediana_txt}."
    )
    st.table(df_focus.set_index("Indicador"))

    # ---------- Focus – Top 5 ----------
    st.markdown("**Focus – Top 5 (instituições mais assertivas)**")
    st.caption(
        "Mediana das projeções das 5 instituições com melhor "
        f"desempenho histórico no Focus. Dados de {data_top5_txt}."
    )
    st.table(df_focus_top5.set_index("Indicador"))

    # ---------- Focus – expectativas mensais p/ próximo mês ----------
    df_focus_mensal_prox, mes_prox_txt, data_mensal_txt = (
        montar_tabela_focus_mensal_proximo_mes()
    )

    st.markdown("**Focus – Expectativas mensais para o próximo mês**")
    st.caption(
        "Mediana das projeções mensais para o próximo mês-calendário "
        f"(mês de referência: {mes_prox_txt}). "
        f"Dados do boletim Focus de {data_mensal_txt}."
    )
    if df_focus_mensal_prox.empty:
        st.info(
            "Ainda não há expectativas mensais disponíveis para o próximo mês."
        )
    else:
        st.table(df_focus_mensal_prox.set_index("Indicador"))


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


@st.cache_data(ttl=60 * 60 * 24)
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

    # 🔌 Modo turbo / offline:
# Não atualizamos mais ANBIMA / DI Futuro B3 em tempo real aqui.
# Os dados vêm do cache salvo em disco, atualizado pelo script
# `atualiza_dados_pesados.py`.
#
# Se em algum momento você quiser voltar a atualizar em tempo real,
# é só restaurar o bloco antigo de `atualizar_dados_externos_cache(chave_dia)`.


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
            "📈 Expectativas",
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
            render_bloco_expectativas_focus(
                df_focus=df_focus,
                df_focus_top5=df_focus_top5,
            )


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
