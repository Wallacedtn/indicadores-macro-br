# curvas_anbima.py
# -*- coding: utf-8 -*-

import os
import io
import re
from datetime import datetime, timedelta, date
from typing import Optional, List

import pandas as pd
import requests
import logging
logging.basicConfig(level=logging.WARNING)


# =============================================================================
# CONFIGURAÇÃO DE PASTAS / ARQUIVOS
# =============================================================================

BASE_DIR = "data/curvas_anbima"
os.makedirs(BASE_DIR, exist_ok=True)

# Arquivo único com toda a informação de curva:
# - data_curva: data efetiva da ETTJ (data da curva ANBIMA)
# - data_ref  : dia em que o app rodou/baixou a curva
# - PRAZO_DU  : prazo em dias úteis
# - TAXA_PREF : juro nominal (% a.a.)
# - TAXA_IPCA : juro real (% a.a.)
PATH_FULL = os.path.join(BASE_DIR, "curvas_anbima_full.csv")


# =============================================================================
# HELPERS INTERNOS
# =============================================================================


def _log(msg: str, level="debug") -> None:
    if level == "error":
        logging.error(msg)
    elif level == "warning":
        logging.warning(msg)
    else:
        logging.debug(msg)



def _converter_coluna_taxa_generica(serie: pd.Series) -> pd.Series:
    """Converte série de taxas em float, aceitando formatos:
    - '13,2697'
    - '1.234,56'
    - '13.2697'
    """
    s = serie.astype(str).str.strip()

    # valores com vírgula -> remover ponto de milhar e trocar vírgula por ponto
    mask_com_virgula = s.str.contains(",", regex=False)
    s_conv = s.copy()
    s_conv[mask_com_virgula] = (
        s_conv[mask_com_virgula]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    return pd.to_numeric(s_conv, errors="coerce")


def _extrair_data_curva(texto: str) -> Optional[date]:
    """Tenta extrair a data efetiva da curva (data da ETTJ)
    das primeiras linhas do arquivo da ANBIMA.

    Não confiamos em 'hoje' como data da curva, porque o endpoint
    CZ-down.asp sempre retorna a ÚLTIMA curva disponível.
    """
    for linha in texto.splitlines()[:10]:
        # procura padrão DD/MM/AAAA
        match = re.search(r"(\d{2}/\d{2}/\d{4})", linha)
        if match:
            try:
                dt = datetime.strptime(match.group(1), "%d/%m/%Y").date()
                return dt
            except ValueError:
                continue
    return None


def _baixar_curva_zero_ultima() -> pd.DataFrame:
    """Baixa a Curva Zero ANBIMA (última disponível) e devolve
    DataFrame com colunas numéricas:

        - data_curva  (date)
        - PRAZO_DU    (int, dias úteis)
        - TAXA_PREF   (float, % a.a.)
        - TAXA_IPCA   (float, % a.a.)
    """
    url = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    _log(f"Baixando Curva Zero (última disponível) de {url}", level="debug")

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:  # noqa: BLE001
        _log(f"Erro HTTP ao baixar Curva Zero: {e}")
        return pd.DataFrame()

    _log(f"Resposta HTTP {resp.status_code} para Curva Zero", level="debug")

    texto = resp.text
    if not texto.strip():
        _log("Corpo da resposta vazio (nenhum texto).")
        return pd.DataFrame()

    # salva resposta crua para eventual debug
    try:
        os.makedirs(BASE_DIR, exist_ok=True)
        debug_path = os.path.join(BASE_DIR, "debug_curva_zero_raw.csv")
        with open(debug_path, "w", encoding="latin-1") as f:
            f.write(texto)
        _log(f"Resposta crua salva em {debug_path}", level="debug")
    except Exception as e:  # noqa: BLE001
        _log(f"Erro ao gravar arquivo de debug: {e}")

    # tenta extrair a data efetiva da curva a partir do texto
    data_curva = _extrair_data_curva(texto)
    if data_curva is None:
        # fallback: usa hoje como aproximação (não ideal, mas melhor que nada)
        data_curva = datetime.now().date()
        _log(
            "Não foi possível identificar a data da curva no arquivo; "
            "usando a data de hoje como 'data_curva'."
        )
    else:
        _log(f"Data da curva identificada como {data_curva.strftime('%d/%m/%Y')}", level="debug")

    # Lê os vértices da ETTJ:
    # a estrutura usual é:
    # - primeiras linhas: cabeçalho / parâmetros
    # - linhas seguintes: vértices (prazo em DU e taxas)
    try:
        df_vertices = pd.read_csv(
            io.StringIO(texto),
            sep=";",
            header=None,
            encoding="latin-1",
            skiprows=5,   # pula cabeçalho e parâmetros
            nrows=69,     # número típico de vértices
            names=["PRAZO", "TAXA_IPCA", "TAXA_PREF", "INFLACAO_IMPL"],
        )
    except Exception as e:  # noqa: BLE001
        _log(f"Erro ao ler CSV da Curva Zero: {e}")
        return pd.DataFrame()

    # Limpa a coluna de PRAZO (pode vir como '1.008', etc.)
    prazos_raw = df_vertices["PRAZO"].astype(str).str.strip()
    prazos_raw = prazos_raw.str.replace(".", "", regex=False)

    mask_num = prazos_raw.str.fullmatch(r"\d+")
    df = df_vertices[mask_num].copy()
    df["PRAZO_DU"] = prazos_raw[mask_num].astype(int)

    # Converte taxas
    df["TAXA_PREF"] = _converter_coluna_taxa_generica(df["TAXA_PREF"])
    df["TAXA_IPCA"] = _converter_coluna_taxa_generica(df["TAXA_IPCA"])

    df = df[["PRAZO_DU", "TAXA_PREF", "TAXA_IPCA"]].copy()
    df = df.dropna(subset=["PRAZO_DU", "TAXA_PREF", "TAXA_IPCA"])

    if df.empty:
        _log("Curva Zero lida, mas sem linhas válidas após limpeza.")
        return pd.DataFrame()

    df = df.sort_values("PRAZO_DU").reset_index(drop=True)
    df["data_curva"] = data_curva

    # Reordena colunas para deixar data_curva primeiro
    cols = ["data_curva", "PRAZO_DU", "TAXA_PREF", "TAXA_IPCA"]
    df = df[cols]

    return df


def _append_historico_full(df_new: pd.DataFrame) -> None:
    """Anexa novas observações ao arquivo de histórico único,
    garantindo que não haja duplicata por (data_curva, PRAZO_DU).
    """
    if df_new is None or df_new.empty:
        return

    # Carrega histórico antigo, se existir
    try:
        df_old = pd.read_csv(PATH_FULL, parse_dates=["data_curva"])
        df_old["data_curva"] = df_old["data_curva"].dt.date
    except FileNotFoundError:
        df_old = pd.DataFrame()

    # Concatena
    df_full = pd.concat([df_old, df_new], ignore_index=True)

    # Garante tipos
    if "data_curva" in df_full.columns:
        df_full["data_curva"] = pd.to_datetime(df_full["data_curva"]).dt.date

    # Remove duplicatas por data_curva + PRAZO_DU
    if {"data_curva", "PRAZO_DU"}.issubset(df_full.columns):
        df_full.drop_duplicates(
            subset=["data_curva", "PRAZO_DU"],
            keep="last",
            inplace=True,
        )

    df_full.to_csv(PATH_FULL, index=False, encoding="utf-8-sig")
    _log(
        f"Histórico atualizado em {PATH_FULL} "
        f"({len(df_full)} linhas no total).", level="debug"
    )


# =============================================================================
# API PÚBLICA DO MÓDULO
# =============================================================================


def atualizar_todas_as_curvas(data: Optional[datetime] = None) -> None:
    """Atualiza o histórico local de curvas ANBIMA (prefixada e IPCA+).

    Observações importantes:
    - O parâmetro `data` aqui serve apenas como \"data de referência\" do download.
      A data efetiva da curva (data_curva) vem do próprio arquivo CZ da ANBIMA.
    - O endpoint CZ-down.asp SEMPRE devolve a última curva disponível,
      não necessariamente a curva do dia em que o código está rodando.
    """
    if data is None:
        data = datetime.now()

    data_ref = data.date()
    _log(f"Iniciando atualização de curvas ANBIMA (data_ref={data_ref})")

    df_curva = _baixar_curva_zero_ultima()
    if df_curva.empty:
        _log("Nenhuma nova curva ANBIMA foi adicionada (DataFrame vazio).")
        return

    # Adiciona coluna de data_ref (dia do download)
    df_curva = df_curva.copy()
    df_curva["data_ref"] = data_ref

    _append_historico_full(df_curva)


# =============================================================================
# EXTRAÇÃO DE VÉRTICES
# =============================================================================


def _extrair_vertice_dia(
    df_dia: pd.DataFrame,
    anos: int,
    coluna_taxa: str,
) -> Optional[float]:
    """
    Extrai a taxa para um vértice específico em anos,
    usando interpolação linear em dias úteis.

    - Se existir PRAZO_DU exato -> usa.
    - Se existir intervalo envolvendo o prazo -> interpola.
    - Se não houver intervalo -> usa o mais próximo (fallback).
    """
    if df_dia.empty or "PRAZO_DU" not in df_dia.columns:
        return None

    df = df_dia.copy()
    df[coluna_taxa] = pd.to_numeric(df[coluna_taxa], errors="coerce")

    if df[coluna_taxa].notna().sum() == 0:
        return None

    # alvo em dias úteis (aprox. 252 DU por ano)
    alvo_du = int(round(anos * 252))

    df = df.sort_values("PRAZO_DU")

    # 1) Se existir prazo exatamente igual
    if alvo_du in df["PRAZO_DU"].values:
        return float(df.loc[df["PRAZO_DU"] == alvo_du, coluna_taxa].iloc[0])

    # 2) Buscar intervalo para interpolação
    antes = df[df["PRAZO_DU"] < alvo_du].tail(1)
    depois = df[df["PRAZO_DU"] > alvo_du].head(1)

    if not antes.empty and not depois.empty:
        # Interpolação linear em PRAZO_DU
        x0 = antes["PRAZO_DU"].iloc[0]
        x1 = depois["PRAZO_DU"].iloc[0]
        y0 = antes[coluna_taxa].iloc[0]
        y1 = depois[coluna_taxa].iloc[0]

        y = y0 + (y1 - y0) * ((alvo_du - x0) / (x1 - x0))
        return float(y)

    # 3) Fallback: usa o mais próximo (somente nos extremos da curva)
    df["diff"] = (df["PRAZO_DU"] - alvo_du).abs()
    df = df.sort_values("diff")
    valor = df.iloc[0][coluna_taxa]
    return float(valor) if pd.notnull(valor) else None


def _carregar_historico_full() -> pd.DataFrame:
    """Carrega o histórico completo de curvas, se existir."""
    try:
        df = pd.read_csv(PATH_FULL, parse_dates=["data_curva"])
    except FileNotFoundError:
        return pd.DataFrame()

    if df.empty:
        return df

    df["data_curva"] = df["data_curva"].dt.date
    if "data_ref" in df.columns:
        df["data_ref"] = pd.to_datetime(df["data_ref"]).dt.date
    return df


# =============================================================================
# CURVA DE HOJE (NÍVEIS POR VÉRTICE)
# =============================================================================


def montar_curva_anbima_hoje() -> pd.DataFrame:
    """
    Retorna uma tabela com:
        - Data da curva (data_curva)
        - Vértice (anos)     [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30]
        - Juro Nominal (%)
        - Juro Real (%)
        - Breakeven (%)

    Todos os valores são numéricos. A formatação com '%' fica para a camada
    de apresentação (Streamlit).
    """
    df_hist = _carregar_historico_full()
    if df_hist.empty:
        return pd.DataFrame()

    data_mais_recente = df_hist["data_curva"].max()
    if pd.isna(data_mais_recente):
        return pd.DataFrame()

    df_dia = df_hist[df_hist["data_curva"] == data_mais_recente].copy()

    # mesmos vértices que você colocou no selectbox
    vertices = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30]
    linhas: List[dict] = []

    for v in vertices:
        nominal = _extrair_vertice_dia(df_dia, v, "TAXA_PREF")
        real = _extrair_vertice_dia(df_dia, v, "TAXA_IPCA")
        breakeven = None
        if nominal is not None and real is not None:
            breakeven = nominal - real

        linhas.append(
            {
                "Data da curva": data_mais_recente,
                "Vértice (anos)": v,
                "Juro Nominal (%)": nominal,
                "Juro Real (%)": real,
                "Breakeven (%)": breakeven,
            }
        )

    df_out = pd.DataFrame(linhas)
    return df_out



# =============================================================================
# HISTÓRICO PARA ABERTURA / FECHAMENTO (POR VÉRTICE)
# =============================================================================


def montar_curva_anbima_variacoes(anos: int) -> pd.DataFrame:
    """Retorna um quadro estilo Focus com a curva ANBIMA em um vértice
    específico (anos), para diferentes horizontes de comparação:

        - Hoje       (última data de curva disponível)
        - D-1        (curva anterior, se existir)
        - 1 semana   (última curva com data_curva <= hoje-7dias)
        - 1 mês      (<= hoje-30dias)
        - Ano        (primeira curva do ano ou a mais próxima após 1/jan)
        - 12 meses   (<= hoje-365dias)

    As colunas retornadas são NÚMEROS (floats):

        - Data
        - Juro Nominal (%)
        - Juro Real (%)
        - Breakeven (%)

    A formatação final com '%' fica a cargo da camada de apresentação
    (Streamlit, etc.).
    """
    df_hist = _carregar_historico_full()
    if df_hist.empty:
        return pd.DataFrame()

    if "data_curva" not in df_hist.columns:
        return pd.DataFrame()

    hoje = df_hist["data_curva"].max()
    if pd.isna(hoje):
        return pd.DataFrame()

    # dicionário de datas-alvo para comparação
    datas_alvo = {
        "Hoje": hoje,
        "D-1": hoje - timedelta(days=1),
        "1 semana": hoje - timedelta(days=7),
        "1 mês": hoje - timedelta(days=30),
        "Ano": date(hoje.year, 1, 1),
        "12 meses": hoje - timedelta(days=365),
    }

    linhas: List[dict] = []

    for rotulo, dt_alvo in datas_alvo.items():
        # Seleciona a última data_curva <= dt_alvo
        mask = df_hist["data_curva"] <= dt_alvo
        df_cand = df_hist[mask]

        if df_cand.empty:
            nominal = None
            real = None
            breakeven = None
        else:
            data_ref = df_cand["data_curva"].max()
            df_dia = df_cand[df_cand["data_curva"] == data_ref]

            nominal = _extrair_vertice_dia(df_dia, anos, "TAXA_PREF")
            real = _extrair_vertice_dia(df_dia, anos, "TAXA_IPCA")
            breakeven = None
            if nominal is not None and real is not None:
                breakeven = nominal - real

        linhas.append(
            {
                "Data": rotulo,
                "Juro Nominal (%)": nominal,
                "Juro Real (%)": real,
                "Breakeven (%)": breakeven,
            }
        )

    df_out = pd.DataFrame(linhas)
    return df_out


# =============================================================================
# HELPERS OPCIONAIS DE FORMATAÇÃO
# =============================================================================


def formatar_percentuais(
    df: pd.DataFrame,
    colunas: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Devolve uma cópia do DataFrame com as colunas indicadas formatadas
    como string \"X.XXX%\" (3 casas decimais). Útil para exibir no Streamlit.

    Exemplo de uso:
        df = montar_curva_anbima_hoje()
        df_fmt = formatar_percentuais(
            df,
            colunas=[\"Juro Nominal (%)\", \"Juro Real (%)\", \"Breakeven (%)\"],
        )
    """
    if colunas is None:
        # por padrão, tenta aplicar nas 3 colunas padrão se existirem
        colunas = [
            "Juro Nominal (%)",
            "Juro Real (%)",
            "Breakeven (%)",
        ]

    df_out = df.copy()

    for col in colunas:
        if col not in df_out.columns:
            continue

        def _fmt(x: Optional[float]) -> str:
            if x is None or pd.isna(x):
                return "-"
            return f"{x:.3f}%"

        df_out[col] = df_out[col].apply(_fmt)

    return df_out


if __name__ == "__main__":
    # Pequeno teste rápido de linha de comando.
    atualizar_todas_as_curvas()
    print(montar_curva_anbima_hoje())
    print(montar_curva_anbima_variacoes(anos=5))
