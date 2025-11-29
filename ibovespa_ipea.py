# ibovespa_ipea.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import requests
import urllib3

# O IPEA está com problema de certificado SSL.
# Esta linha desativa o aviso de "InsecureRequestWarning"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ============================================================
# CONFIG BÁSICA
# ============================================================

IPEA_BASE_URL = "https://www.ipeadata.gov.br/api/odata4"
IBOV_SERCODIGO = "GM366_IBVSP366"

# Pasta / arquivo onde vamos salvar o histórico local
HIST_DIR = "data/curto_prazo"
HIST_PATH = os.path.join(HIST_DIR, "ibovespa_ipea.csv")


def baixar_serie_ibovespa(
    timeout: Tuple[int, int] = (5, 60),
    tentativas: int = 3,
) -> pd.DataFrame:
    """
    Baixa a série completa do Ibovespa no Ipeadata, com retry.

    - timeout: (tempo de conexão, tempo de leitura), em segundos.
    - tentativas: quantas vezes tenta antes de desistir.

    Retorna um DataFrame com colunas:
        - data (datetime.date)
        - valor (float)
    """
    url = f"{IPEA_BASE_URL}/ValoresSerie(SERCODIGO='{IBOV_SERCODIGO}')"

    ultima_exc: Optional[Exception] = None

    for tentativa in range(1, tentativas + 1):
        try:
            print(
                f"[Ibovespa IPEA] Tentativa {tentativa}/{tentativas} "
                f"(timeout={timeout})..."
            )
            resp = requests.get(url, timeout=timeout, verify=False)
            resp.raise_for_status()
            payload = resp.json()
            valores = payload.get("value", [])

            if not valores:
                raise ValueError(
                    "Ipeadata retornou lista vazia para o Ibovespa."
                )

            registros = []
            for item in valores:
                data_str = item.get("VALDATA")
                valor = item.get("VALVALOR")
                if not data_str or valor is None:
                    continue
                # VALDATA vem no formato 'YYYY-MM-DDT00:00:00'
                registros.append((data_str[:10], float(valor)))

            if not registros:
                raise ValueError(
                    "Não há registros válidos do Ibovespa no Ipeadata."
                )

            df = pd.DataFrame(registros, columns=["data", "valor"])
            df["data"] = pd.to_datetime(df["data"]).dt.date
            df = df.sort_values("data").reset_index(drop=True)
            return df

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as e:
            # Erros de timeout / conexão: tenta de novo
            ultima_exc = e
            print(
                f"[Ibovespa IPEA] Falha de rede (tentativa "
                f"{tentativa}/{tentativas}): {e}"
            )
            if tentativa == tentativas:
                print("[Ibovespa IPEA] Todas as tentativas falharam.")
                raise

        except requests.exceptions.RequestException as e:
            # Erros HTTP 4xx/5xx ou outros problemas de request
            print(f"[Ibovespa IPEA] Erro na requisição: {e}")
            raise

    # Segurança: se chegar aqui sem retorno
    if ultima_exc:
        raise ultima_exc
    raise RuntimeError("Falha inesperada ao baixar série do Ibovespa.")


def atualizar_historico_ibovespa(caminho: str = HIST_PATH) -> pd.DataFrame:
    """
    Atualiza o arquivo ibovespa_ipea.csv com a série do Ipeadata.

    - Se o arquivo não existir, cria com a série inteira.
    - Se existir, concatena e remove duplicatas por data.
    - Retorna o DataFrame final ordenado por data.
    """
    df_novo = baixar_serie_ibovespa()

    # Garante que a pasta existe
    os.makedirs(os.path.dirname(caminho), exist_ok=True)

    # Carrega histórico antigo (se existir)
    if os.path.exists(caminho) and os.path.getsize(caminho) > 0:
        df_old = pd.read_csv(caminho, parse_dates=["data"])
        df_old["data"] = df_old["data"].dt.date
    else:
        df_old = pd.DataFrame(columns=df_novo.columns)

    # Garante que ambos têm as mesmas colunas
    colunas_final = sorted(set(df_old.columns).union(set(df_novo.columns)))
    df_old = df_old.reindex(columns=colunas_final)
    df_novo = df_novo.reindex(columns=colunas_final)

    frames = []
    for f in (df_old, df_novo):
        if f is None or f.empty:
            continue
        frames.append(f)

    if frames:
        df = pd.concat(frames, ignore_index=True)
    else:
        df = pd.DataFrame(columns=df_novo.columns)

    # Remove duplicatas por data (fica com o último registro daquela data)
    df["data"] = pd.to_datetime(df["data"]).dt.date
    df = df.drop_duplicates(subset=["data"], keep="last")
    df = df.sort_values("data").reset_index(drop=True)

    # Salva em disco
    df.to_csv(caminho, index=False, encoding="utf-8")

    return df


def carregar_historico_ibovespa(caminho: str = HIST_PATH) -> pd.DataFrame:
    """
    Carrega o histórico local do Ibovespa salvo em CSV.
    """
    if not os.path.exists(caminho) or os.path.getsize(caminho) == 0:
        raise FileNotFoundError(f"Histórico do Ibovespa não encontrado em {caminho}")

    df = pd.read_csv(caminho, parse_dates=["data"])
    df["data"] = df["data"].dt.date
    df = df.sort_values("data").reset_index(drop=True)
    return df


if __name__ == "__main__":
    df_hist = atualizar_historico_ibovespa()
    print(df_hist.head())
    print(df_hist.tail())
