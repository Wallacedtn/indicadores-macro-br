# curvas_anbima.py
# -*- coding: utf-8 -*-

import os
import io
from datetime import datetime, timedelta

import pandas as pd
import requests

# =============================================================================
# CONFIGURAÇÃO DE PASTAS
# =============================================================================

BASE_DIR = "data/curvas_anbima"
os.makedirs(BASE_DIR, exist_ok=True)

PATH_DI = os.path.join(BASE_DIR, "curva_di.csv")
PATH_PREF = os.path.join(BASE_DIR, "curva_prefixada.csv")
PATH_IPCA = os.path.join(BASE_DIR, "curva_ipca.csv")


# =============================================================================
# FUNÇÕES AUXILIARES DE DOWNLOAD
# =============================================================================


def _baixar_curvas_zero(data: datetime) -> pd.DataFrame:
    """
    Baixa a Curva Zero (Prefixada + IPCA) da ANBIMA via endpoint CZ-down.asp
    e devolve um DataFrame com colunas:

        - PRAZO       (dias úteis até o vencimento)
        - TAXA_PREF   (ETTJ Prefixada)
        - TAXA_IPCA   (ETTJ IPCA)
    """
    url = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"
    dt_str = data.strftime("%d/%m/%Y")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    print(f"[ANBIMA] Baixando Curva Zero para {dt_str} em {url}")

    try:
        resp = requests.post(
            url,
            headers=headers,
            data={
                "escolha": "2",   # Curva Zero
                "Idioma": "PT",
                "saida": "csv",
                "Dt_Ref": dt_str,
            },
            timeout=30,
        )
    except Exception as e:
        print(f"[ANBIMA] ERRO de conexão ao baixar Curva Zero {dt_str}: {e}")
        return pd.DataFrame()

    print(f"[ANBIMA] Resposta HTTP {resp.status_code} para Curva Zero {dt_str}")

    if resp.status_code != 200:
        # Ex.: 404 se a data não tiver curva disponível
        return pd.DataFrame()

    texto = resp.text
    linhas = texto.splitlines()

    # Procura a parte que começa com "Vertices;ETTJ IPCA;ETTJ PREF;Inflação Implícita"
    inicio = None
    for i, linha in enumerate(linhas):
        if linha.startswith("Vertices;"):
            inicio = i
            break

    if inicio is None:
        print("[ANBIMA] Não encontrei a seção de vértices na Curva Zero.")
        return pd.DataFrame()

    csv_curvas = "\n".join(linhas[inicio:])

    df = pd.read_csv(
        io.StringIO(csv_curvas),
        sep=";",
        encoding="latin-1",
        decimal=",",
        thousands=".",
    )

    # Renomeia colunas para algo mais amigável
    df = df.rename(
        columns={
            "Vertices": "PRAZO",
            "ETTJ PREF": "TAXA_PREF",
            "ETTJ IPCA": "TAXA_IPCA",
        }
    )

    # -------------------------------
    # Limpa a coluna PRAZO:
    # - transforma em string
    # - tira espaços
    # - remove ponto de milhar (ex: "1.008" -> "1008")
    # - mantém só linhas em que PRAZO é numérico
    # -------------------------------
    serie_prazo = df["PRAZO"].astype(str).str.strip()
    serie_prazo = serie_prazo.str.replace(".", "", regex=False)

    mask_numerico = serie_prazo.str.fullmatch(r"\d+")
    df = df[mask_numerico].copy()
    df["PRAZO"] = serie_prazo[mask_numerico].astype(int)

    return df


def _baixar_csv_anbima(tipo: str, data: datetime) -> pd.DataFrame:
    """
    Interface antiga, mas agora usando a Curva Zero nova.

    tipo:
        - "pref" -> curva prefixada (usa TAXA_PREF)
        - "ipca" -> curva IPCA (usa TAXA_IPCA)
        - "di"   -> por enquanto não implementamos via ANBIMA (devolve vazio)
    """
    # Por enquanto não buscamos DI soberano via ANBIMA aqui.
    if tipo == "di":
        print("[ANBIMA] Download de DI via ANBIMA desabilitado neste módulo.")
        return pd.DataFrame()

    df_curvas = _baixar_curvas_zero(data)

    if df_curvas.empty:
        return pd.DataFrame()

    if tipo == "pref":
        df_pref = df_curvas[["PRAZO", "TAXA_PREF"]].copy()
        df_pref = df_pref.rename(columns={"TAXA_PREF": "TAXA"})
        return df_pref

    if tipo == "ipca":
        df_ipca = df_curvas[["PRAZO", "TAXA_IPCA"]].copy()
        df_ipca = df_ipca.rename(columns={"TAXA_IPCA": "TAXA"})
        return df_ipca

    raise ValueError("Tipo inválido para download ANBIMA (use 'di', 'pref' ou 'ipca').")


def _append_historico(df_new: pd.DataFrame, path_csv: str) -> None:
    """
    Anexa histórico no CSV local sem duplicar datas/vértices.
    Espera colunas:
        - data_ref (date ou string)
        - PRAZO   (dias úteis até o vencimento)
    """
    if df_new.empty:
        return

    if "data_ref" not in df_new.columns:
        raise ValueError("df_new precisa ter coluna 'data_ref' antes de salvar o histórico.")

    try:
        df_old = pd.read_csv(path_csv)
    except FileNotFoundError:
        df_old = pd.DataFrame()

    df_full = pd.concat([df_old, df_new], ignore_index=True)

    if "data_ref" in df_full.columns:
        df_full["data_ref"] = pd.to_datetime(df_full["data_ref"]).dt.date

    if "PRAZO" not in df_full.columns:
        raise ValueError("Coluna 'PRAZO' não encontrada no histórico ANBIMA.")

    df_full.drop_duplicates(
        subset=["data_ref", "PRAZO"],
        keep="last",
        inplace=True,
    )

    df_full.to_csv(path_csv, index=False, encoding="utf-8-sig")


# =============================================================================
# HELPER PARA CONVERSÃO DA COLUNA TAXA
# =============================================================================


def _converter_coluna_taxa(df: pd.DataFrame) -> None:
    """
    Converte a coluna 'TAXA' para float, aceitando:
      - formato brasileiro:  '13,2697'  ou '1.234,56'
      - formato com ponto:   '13.2697'
    """
    if "TAXA" not in df.columns:
        return

    s = df["TAXA"].astype(str).str.strip()

    # valores com vírgula -> formato brasileiro
    mask_com_virgula = s.str.contains(",", regex=False)

    s_conv = s.copy()
    s_conv[mask_com_virgula] = (
        s_conv[mask_com_virgula]
        .str.replace(".", "", regex=False)   # remove milhar
        .str.replace(",", ".", regex=False)  # vírgula -> ponto
    )

    df["TAXA"] = pd.to_numeric(s_conv, errors="coerce")


# =============================================================================
# DOWNLOAD DIÁRIO
# =============================================================================


def atualizar_todas_as_curvas(data: datetime | None = None) -> None:
    """
    Baixa Prefixada e IPCA da data escolhida (ou hoje)
    e atualiza o histórico local.

    - Cria/atualiza:
        data/curvas_anbima/curva_di.csv      (ainda vazio, por enquanto)
        data/curvas_anbima/curva_prefixada.csv
        data/curvas_anbima/curva_ipca.csv
    """
    if data is None:
        data = datetime.now()

    data_ref = data.date()

    # DI fica desabilitado por enquanto
    df_di = _baixar_csv_anbima("di", data)
    df_pref = _baixar_csv_anbima("pref", data)
    df_ipca = _baixar_csv_anbima("ipca", data)

    if not df_di.empty:
        df_di["data_ref"] = data_ref
        _append_historico(df_di, PATH_DI)

    if not df_pref.empty:
        df_pref["data_ref"] = data_ref
        _append_historico(df_pref, PATH_PREF)

    if not df_ipca.empty:
        df_ipca["data_ref"] = data_ref
        _append_historico(df_ipca, PATH_IPCA)


# =============================================================================
# EXTRAÇÃO DOS VÉRTICES IMPORTANTES
# =============================================================================


def _extrair_vertice(df: pd.DataFrame, anos: int) -> float | None:
    """
    Pega o vértice mais próximo do prazo desejado (ano em número inteiro),
    usando a coluna PRAZO (dias úteis).

    Aqui nós garantimos que a coluna TAXA seja numérica,
    mesmo que tenha vindo como texto com vírgula.
    """
    if df.empty:
        return None

    if "PRAZO" not in df.columns or "TAXA" not in df.columns:
        return None

    df = df.copy()

    # Normaliza TAXA aqui também (camada extra de segurança)
    s = df["TAXA"].astype(str).str.strip()
    mask_com_virgula = s.str.contains(",", regex=False)

    s_conv = s.copy()
    s_conv[mask_com_virgula] = (
        s_conv[mask_com_virgula]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    df["TAXA"] = pd.to_numeric(s_conv, errors="coerce")

    # se tudo virou NaN, não tem o que fazer
    if df["TAXA"].notna().sum() == 0:
        return None

    df["anos"] = df["PRAZO"] / 252

    alvo = anos
    df["diff"] = (df["anos"] - alvo).abs()
    df = df.sort_values("diff")

    valor = df.iloc[0]["TAXA"]
    return float(valor) if pd.notnull(valor) else None


# =============================================================================
# CURVA DE HOJE (NÍVEIS)
# =============================================================================


def montar_curva_anbima_hoje() -> pd.DataFrame:
    """
    Retorna uma tabela consolidada com:
        - Juro nominal (prefixado / MNP)
        - Juro real (IPCA+ / MSN)
        - Breakeven
        - Para vértices de 2, 5, 10, 20 anos

    Usa os CSVs históricos locais (curva_prefixada.csv e curva_ipca.csv)
    e filtra para a data mais recente disponível, idealmente o dia atual.
    """
    hoje = datetime.now().date()

    try:
        df_pref = pd.read_csv(PATH_PREF)
        df_ipca = pd.read_csv(PATH_IPCA)
    except FileNotFoundError:
        return pd.DataFrame()

    if df_pref.empty or df_ipca.empty:
        return pd.DataFrame()

    # normaliza datas e TAXA
    for df in (df_pref, df_ipca):
        if "data_ref" not in df.columns:
            return pd.DataFrame()
        df["data_ref"] = pd.to_datetime(df["data_ref"]).dt.date
        _converter_coluna_taxa(df)

    data_disp_pref = df_pref["data_ref"].max()
    data_disp_ipca = df_ipca["data_ref"].max()
    data_max = min(data_disp_pref, data_disp_ipca)

    data_ref = hoje if hoje <= data_max else data_max

    df_pref_dia = df_pref[df_pref["data_ref"] == data_ref]
    df_ipca_dia = df_ipca[df_ipca["data_ref"] == data_ref]

    if df_pref_dia.empty and df_ipca_dia.empty:
        return pd.DataFrame()

    vertices = [2, 5, 10, 20]
    dados = []

    for v in vertices:
        nominal = _extrair_vertice(df_pref_dia, v)
        real = _extrair_vertice(df_ipca_dia, v)
        breakeven = nominal - real if (nominal is not None and real is not None) else None

        dados.append(
            {
                "Vértice (anos)": v,
                "Juro Nominal (%)": nominal,
                "Juro Real (%)": real,
                "Breakeven (%)": breakeven,
            }
        )

    df_out = pd.DataFrame(dados)

    def fmt(x: float | None) -> str:
        return f"{x:.3f}%" if pd.notnull(x) else "-"

    for col in ["Juro Nominal (%)", "Juro Real (%)", "Breakeven (%)"]:
        df_out[col] = df_out[col].apply(fmt)

    return df_out


# =============================================================================
# HISTÓRICO PARA ABERTURA / FECHAMENTO
# =============================================================================


def montar_curva_anbima_variacoes(anos: int) -> pd.DataFrame:
    """
    Retorna um histórico com:
        - Hoje
        - D-1
        - 1 semana
        - 1 mês
        - Início do ano
        - 12 meses
        - Níveis em % (nominal, real e breakeven)

    Apenas para um único vértice (anos = 2, 5, 10, 20...),
    usando as curvas prefixada (MNP) e IPCA+ (MSN).
    """
    try:
        df_pref = pd.read_csv(PATH_PREF)
        df_ipca = pd.read_csv(PATH_IPCA)
    except FileNotFoundError:
        return pd.DataFrame()

    if df_pref.empty or df_ipca.empty:
        return pd.DataFrame()

    # normaliza datas e TAXA
    for df in (df_pref, df_ipca):
        if "data_ref" not in df.columns:
            return pd.DataFrame()
        df["data_ref"] = pd.to_datetime(df["data_ref"]).dt.date
        _converter_coluna_taxa(df)

    hoje = df_pref["data_ref"].max()
    if pd.isna(hoje):
        return pd.DataFrame()

    datas = {
        "Hoje": hoje,
        "D-1": hoje - timedelta(days=1),
        "1 semana": hoje - timedelta(days=7),
        "1 mês": hoje - timedelta(days=30),
        "Ano": datetime(hoje.year, 1, 1).date(),
        "12 meses": hoje - timedelta(days=365),
    }

    def get_taxa(df: pd.DataFrame, dt) -> float | None:
        df2 = df[df["data_ref"] <= dt]
        if df2.empty:
            return None
        return _extrair_vertice(df2, anos)

    linhas = []

    for nome, dt in datas.items():
        nominal = get_taxa(df_pref, dt)
        real = get_taxa(df_ipca, dt)
        breakeven = nominal - real if (nominal is not None and real is not None) else None

        linhas.append(
            {
                "Data": nome,
                "Juro Nominal (%)": nominal,
                "Juro Real (%)": real,
                "Breakeven (%)": breakeven,
            }
        )

    df_out = pd.DataFrame(linhas)

    def to_fmt(x: float | None) -> str:
        return f"{x:.3f}%" if pd.notnull(x) else "-"

    for col in ["Juro Nominal (%)", "Juro Real (%)", "Breakeven (%)"]:
        df_out[col] = df_out[col].apply(to_fmt)

    return df_out
