# di_futuro_b3.py
# -*- coding: utf-8 -*-

import os
import requests
import pandas as pd
from datetime import datetime

# ============================================================
# CONFIG BÁSICA
# ============================================================

API_B3_DI1 = "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation/DI1"

# arquivo onde vamos salvar o histórico
HIST_DIR = "data/di_futuro"
HIST_PATH = os.path.join(HIST_DIR, "di1_historico.csv")

# cabeçalhos para imitar um navegador
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://www.b3.com.br",
    "Referer": "https://www.b3.com.br/",
}


# ============================================================
# HELPER PARA BUSCAR JSON
# ============================================================

def _get_json(url: str, timeout: int = 30) -> dict:
    """Faz GET na API da B3 e devolve o JSON já convertido."""
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


# ============================================================
# BAIXAR SNAPSHOT DO DI FUTURO (TODOS OS CONTRATOS)
# ============================================================

def baixar_snapshot_di_futuro() -> pd.DataFrame:
    """
    Baixa TODOS os contratos DI1 da B3 no momento da chamada.

    Estrutura do JSON (simplificada):

        {
          "BizSts": {...},
          "Msg": {...},
          "Scty": [
             {
               "symb": "DI1J30",
               "asset": {
                   "AsstSummry": {
                       "mtrtyCode": "2030-04-01",
                       "opnCtrcts": 42343,
                       "traddCtrctsQty": 62
                   }
               },
               "SctyQtn": {
                   "curPrc": 13.03,
                   "prvsDayAdjstmntPric": 13.039,
                   ...
               }
             },
             ...
          ]
        }
    """
    dados = _get_json(API_B3_DI1)

    contratos_raw = dados.get("Scty", [])

    if not contratos_raw:
        # DataFrame vazio, para o chamador decidir o que fazer
        return pd.DataFrame()

    linhas = []
    data_hoje = datetime.now().date()

    for item in contratos_raw:
        scty_qtn = item.get("SctyQtn", {})              # cotação
        asset = item.get("asset", {})
        asst_summary = asset.get("AsstSummry", {})      # resumo do ativo

        ticker = item.get("symb")                       # ex.: DI1J30

        venc_raw = asst_summary.get("mtrtyCode")
        try:
            vencimento = (
                datetime.strptime(venc_raw, "%Y-%m-%d").date()
                if venc_raw else None
            )
        except Exception:
            vencimento = None

        taxa = scty_qtn.get("curPrc")
        variacao_bps = scty_qtn.get("prcFlcn")
        ajuste = scty_qtn.get("prvsDayAdjstmntPric")
        pu = scty_qtn.get("curPrc")

        volume = asst_summary.get("traddCtrctsQty")
        open_interest = asst_summary.get("opnCtrcts")
        ultimo_preco = scty_qtn.get("curPrc")

        if not ticker:
            continue

        linhas.append(
            {
                "data": data_hoje,
                "ticker": ticker,
                "vencimento": vencimento,
                "taxa": taxa,
                "variacao_bps": variacao_bps,
                "ajuste": ajuste,
                "pu": pu,
                "volume": volume,
                "open_interest": open_interest,
                "ultimo_preco": ultimo_preco,
            }
        )

    df = pd.DataFrame(linhas)
    return df


# ============================================================
# ATUALIZAR HISTÓRICO EM CSV
# ============================================================

def atualizar_historico_di_futuro(caminho: str = HIST_PATH) -> pd.DataFrame:
    """
    Atualiza o arquivo di1_historico.csv com o snapshot diário da B3.

    - Se o arquivo não existir, cria.
    - Se existir e tiver dados, concatena apenas datas novas.
    - Remove colunas completamente vazias (evita FutureWarning).
    - Retorna o DataFrame final.
    """
    df_novo = baixar_snapshot_di_futuro()

    # Garante que a pasta existe
    os.makedirs(os.path.dirname(caminho), exist_ok=True)

    # Se não veio nada da B3 -> devolve histórico antigo ou DF vazio
    if df_novo is None or df_novo.empty:
        if os.path.exists(caminho) and os.path.getsize(caminho) > 0:
            df_old = pd.read_csv(caminho, parse_dates=["data"])
            df_old["data"] = df_old["data"].dt.date
            return df_old

        # Se não existe histórico, cria DF vazio completo
        colunas = [
            "data", "ticker", "vencimento", "taxa", "variacao_bps",
            "ajuste", "pu", "volume", "open_interest", "ultimo_preco"
        ]
        return pd.DataFrame(columns=colunas)

    # Carrega histórico antigo (se existir)
    if os.path.exists(caminho) and os.path.getsize(caminho) > 0:
        df_old = pd.read_csv(caminho, parse_dates=["data"])
        df_old["data"] = df_old["data"].dt.date
    else:
        # histórico inexistente ou vazio → cria vazio com as mesmas colunas
        df_old = pd.DataFrame(columns=df_novo.columns)

    # Converte a data do novo snapshot
    df_novo["data"] = pd.to_datetime(df_novo["data"]).dt.date

    # 🔥 REMOÇÃO CRÍTICA PARA ACABAR COM O FUTUREWARNING 🔥
    df_old = df_old.dropna(axis=1, how="all")
    df_novo = df_novo.dropna(axis=1, how="all")

    # Agora garantimos que AMBOS têm as mesmas colunas
    colunas_final = sorted(set(df_old.columns).union(set(df_novo.columns)))
    df_old = df_old.reindex(columns=colunas_final)
    df_novo = df_novo.reindex(columns=colunas_final)

    # Concatena com segurança (sem warnings)
    df = pd.concat([df_old, df_novo], ignore_index=True)

    # Remove duplicatas (mesma data + mesmo ticker)
    df = df.drop_duplicates(subset=["data", "ticker"], keep="last")

    # Salva
    df.to_csv(caminho, index=False)

    return df


# ============================================================
# CARREGAR HISTÓRICO
# ============================================================

def carregar_historico_di_futuro(caminho: str = HIST_PATH) -> pd.DataFrame:
    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Arquivo histórico não encontrado: {caminho}")
    df = pd.read_csv(caminho, parse_dates=["data"])
    df["data"] = df["data"].dt.date
    return df


# ============================================================
# EXECUÇÃO DIRETA (TESTE RÁPIDO)
# ============================================================

if __name__ == "__main__":
    df_hist = atualizar_historico_di_futuro()
    print(df_hist.head())
    print(df_hist.tail())
    print("Histórico atualizado com sucesso.")
