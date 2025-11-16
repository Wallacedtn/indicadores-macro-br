# curvas_anbima.py
# -*- coding: utf-8 -*-

import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import io

# =============================================================================
# CONFIGURAÇÃO DE PASTAS
# =============================================================================

BASE_DIR = "data/curvas_anbima"

if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)

PATH_DI = os.path.join(BASE_DIR, "curva_di.csv")
PATH_PREF = os.path.join(BASE_DIR, "curva_prefixada.csv")
PATH_IPCA = os.path.join(BASE_DIR, "curva_ipca.csv")

# =============================================================================
# FUNÇÕES AUXILIARES DE DOWNLOAD
# =============================================================================


def _baixar_csv_anbima(tipo: str, data: datetime) -> pd.DataFrame:
    """
    tipo:
        - "di"   -> curva cupom limpo MMC
        - "pref" -> curva prefixada MNP
        - "ipca" -> curva NTN-B MSN
    """

    ano = data.year
    dia = data.strftime("%d%m%Y")

    if tipo == "di":
        url = f"https://www.anbima.com.br/informacoes/merc-sec/arqs/mmc/{ano}/mmc{dia}.csv"
    elif tipo == "pref":
        url = f"https://www.anbima.com.br/informacoes/merc-sec/arqs/mnp/{ano}/mnp{dia}.csv"
    elif tipo == "ipca":
        url = f"https://www.anbima.com.br/informacoes/merc-sec/arqs/msn/{ano}/msn{dia}.csv"
    else:
        raise ValueError("Tipo inválido para download ANBIMA.")

    resp = requests.get(url, timeout=30)

    if resp.status_code != 200:
        return pd.DataFrame()

    df = pd.read_csv(
        io.StringIO(resp.text),
        sep=";",
        encoding="latin-1",
        decimal=",",
    )
    
    return df

def _append_historico(df_new: pd.DataFrame, path_csv: str) -> None:
    """
    Anexa histórico no CSV local sem duplicar datas/vértices.
    """

    if df_new.empty:
        return

    try:
        df_old = pd.read_csv(path_csv)
    except FileNotFoundError:
        df_old = pd.DataFrame()

    df_full = pd.concat([df_old, df_new], ignore_index=True)

    # Normalizar nome das colunas
    if "data_ref" in df_full.columns:
        df_full["data_ref"] = pd.to_datetime(df_full["data_ref"]).dt.date

    df_full.drop_duplicates(
        subset=["data_ref", df_full.columns[0]], keep="last", inplace=True
    )

    df_full.to_csv(path_csv, index=False, encoding="utf-8-sig")


# =============================================================================
# DOWNLOAD DIÁRIO
# =============================================================================


def atualizar_todas_as_curvas(data: datetime = None):
    """
    Baixa DI, Prefixada e IPCA da data escolhida (ou hoje)
    e atualiza o histórico local.
    """

    if data is None:
        data = datetime.now()

    # Pode faltar dado se for feriado, domingo ou antes das 18h
    df_di = _baixar_csv_anbima("di", data)
    df_pref = _baixar_csv_anbima("pref", data)
    df_ipca = _baixar_csv_anbima("ipca", data)

    if not df_di.empty:
        _append_historico(df_di, PATH_DI)

    if not df_pref.empty:
        _append_historico(df_pref, PATH_PREF)

    if not df_ipca.empty:
        _append_historico(df_ipca, PATH_IPCA)


# =============================================================================
# EXTRAÇÃO DOS VÉRTICES IMPORTANTES
# =============================================================================


def _extrair_vertice(df: pd.DataFrame, anos: int) -> float:
    """
    Pega o vértice mais próximo do prazo desejado (ano em número inteiro)
    """

    if df.empty:
        return None

    # Coluna "PRAZO" vem em dias corridos; transformar anos aproximados
    df["anos"] = df["PRAZO"] / 252

    alvo = anos

    df["diff"] = (df["anos"] - alvo).abs()
    df = df.sort_values("diff")

    valor = df.iloc[0]["TAXA"]
    return float(valor) if pd.notnull(valor) else None


def montar_curva_anbima_hoje() -> pd.DataFrame:
    """
    Retorna uma tabela consolidada com:
        - Juro nominal (prefixado)
        - Juro real (IPCA+)
        - Breakeven
        - Para vértices de 2, 5, 10, 20 anos
    """

    hoje = datetime.now().date()

    try:
        df_di = pd.read_csv(PATH_DI)
        df_pref = pd.read_csv(PATH_PREF)
        df_ipca = pd.read_csv(PATH_IPCA)
    except FileNotFoundError:
        return pd.DataFrame()

    # Filtrar pelo dia atual
    df_di = df_di[df_di["data_ref"] == str(hoje)]
    df_pref = df_pref[df_pref["data_ref"] == str(hoje)]
    df_ipca = df_ipca[df_ipca["data_ref"] == str(hoje)]

    if df_di.empty and df_pref.empty and df_ipca.empty:
        return pd.DataFrame()

    # Renomear colunas para garantir consistência
    for df in [df_di, df_pref, df_ipca]:
        if "TAXA" in df.columns:
            df["TAXA"] = pd.to_numeric(df["TAXA"], errors="coerce")

    vertices = [2, 5, 10, 20]

    dados = []

    for v in vertices:
        nominal = _extrair_vertice(df_pref.copy(), v)
        real = _extrair_vertice(df_ipca.copy(), v)

        if nominal is not None and real is not None:
            breakeven = nominal - real
        else:
            breakeven = None

        dados.append(
            {
                "Vértice (anos)": v,
                "Juro Nominal (%)": nominal,
                "Juro Real (%)": real,
                "Breakeven (%)": breakeven,
            }
        )

    df_out = pd.DataFrame(dados)

    # Formatação
    for col in ["Juro Nominal (%)", "Juro Real (%)", "Breakeven (%)"]:
        df_out[col] = df_out[col].apply(
            lambda x: f"{x:.3f}%" if pd.notnull(x) else "-"
        )

    return df_out


# =============================================================================
# HISTÓRICO PARA ABERTURA / FECHAMENTO
# =============================================================================


def montar_curva_anbima_variacoes(anos: int) -> pd.DataFrame:
    """
    Retorna um histórico com:
        - Hoje
        - 1 dia
        - 1 semana
        - 1 mês
        - Início do ano
        - 12 meses
        - Variações em bps
    Apenas para um único vértice (anos = 5, 10, 20...)
    """

    try:
        df_pref = pd.read_csv(PATH_PREF)
        df_ipca = pd.read_csv(PATH_IPCA)
    except FileNotFoundError:
        return pd.DataFrame()

    df_pref["data_ref"] = pd.to_datetime(df_pref["data_ref"]).dt.date
    df_ipca["data_ref"] = pd.to_datetime(df_ipca["data_ref"]).dt.date

    df_pref["TAXA"] = pd.to_numeric(df_pref["TAXA"], errors="coerce")
    df_ipca["TAXA"] = pd.to_numeric(df_ipca["TAXA"], errors="coerce")

    hoje = df_pref["data_ref"].max()

    datas = {
        "Hoje": hoje,
        "D-1": hoje - timedelta(days=1),
        "1 semana": hoje - timedelta(days=7),
        "1 mês": hoje - timedelta(days=30),
        "Ano": datetime(hoje.year, 1, 1).date(),
        "12 meses": hoje - timedelta(days=365),
    }

    def get_taxa(df, dt):
        df2 = df[df["data_ref"] <= dt]
        if df2.empty:
            return None
        return _extrair_vertice(df2.copy(), anos)

    linhas = []

    for nome, dt in datas.items():
        nominal = get_taxa(df_pref, dt)
        real = get_taxa(df_ipca, dt)

        if nominal is not None and real is not None:
            breakeven = nominal - real
        else:
            breakeven = None

        linhas.append(
            {
                "Data": nome,
                "Juro Nominal (%)": nominal,
                "Juro Real (%)": real,
                "Breakeven (%)": breakeven,
            }
        )

    df_out = pd.DataFrame(linhas)

    # Converte para bps
    def to_fmt(x):
        return f"{x:.3f}%" if pd.notnull(x) else "-"

    for col in ["Juro Nominal (%)", "Juro Real (%)", "Breakeven (%)"]:
        df_out[col] = df_out[col].apply(to_fmt)

    return df_out
