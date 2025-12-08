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
    confianca_industria_referencia: Optional[str] = None

    desemprego_pnad: Optional[float] = None
    desemprego_delta_pp_12m: Optional[float] = None
    desemprego_pnad_12m_atras: Optional[float] = None
    desemprego_pnad_24m_atras: Optional[float] = None
    desemprego_pnad_referencia: Optional[str] = None


    # ----- Risco país -----
    cds_5y: Optional[float] = None               # pontos-base
    cds_5y_delta_pb_12m: Optional[float] = None  # variação em 12m

    # ----- Fiscal / setor externo -----
    divida_bruta_pct_pib: Optional[float] = None           # nível atual (% PIB)
    divida_bruta_delta_pp_12m: Optional[float] = None      # (agora) variação m/m em p.p.
    divida_bruta_pct_pib_12m_atras: Optional[float] = None # nível há 12m
    divida_bruta_pct_pib_24m_atras: Optional[float] = None # nível há 24m
    divida_bruta_referencia: Optional[str] = None          # "mm/aaaa"

    # ----- Resultado Primário – Governo Central (valores nominais) -----
    primario_mes_real_bi: Optional[float] = None            # mês, R$ bi NOMINAIS
    primario_mes_delta_real_bi_aa: Optional[float] = None   # delta vs mesmo mês a/a, R$ bi
    receita_real_var_aa_pct: Optional[float] = None         # var nominal a/a (%) do primário do mês
    despesa_real_var_aa_pct: Optional[float] = None         # reservado p/ futuro
    primario_ano_real_bi: Optional[float] = None            # acumulado no ano até o mês (R$ bi)
    primario_ano_real_bi_prev: Optional[float] = None       # acumulado no ano até o mesmo mês do ano anterior (R$ bi)
    primario_ano_real_bi: Optional[float] = None            # saldo 12m nominal (R$ bi)
    primario_ano_real_bi_prev: Optional[float] = None       # saldo 12m nominal 12m a
    primario_referencia: Optional[str] = None               # ex.: "10/2025"


    # ----- Receita e despesa do Governo Central (nominais) -----
    receita_mes_bi: Optional[float] = None
    receita_mes_delta_bi_aa: Optional[float] = None
    receita_mes_var_aa_pct: Optional[float] = None

    despesa_mes_bi: Optional[float] = None
    despesa_mes_delta_bi_aa: Optional[float] = None
    despesa_mes_var_aa_pct: Optional[float] = None


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

def _carregar_desemprego_pnad() -> tuple[
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[str],
]:
    """
    Desemprego – PNAD Contínua (tabela 6381 / SIDRA).

    Retorna:
    - taxa_atual (%)
    - delta_pp_aa  (atual - mesmo trimestre móvel do ano anterior, em p.p.)
    - taxa_12m     (mesmo trimestre móvel, 1 ano atrás)
    - taxa_24m     (mesmo trimestre móvel, 2 anos atrás)
    - referencia   (string para badge, ex.: 'TRI ATÉ OUT/2025')
    """
    try:
        resp = requests.get(PNAD_TAXA_DESOCUPACAO_URL, timeout=30)
        resp.raise_for_status()
        dados = resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.error("Erro ao baixar PNAD (tabela 6381): %s", exc)
        return None, None, None, None, None

    # Primeira linha é cabeçalho -> pula com [1:]
    df = pd.DataFrame(dados[1:])

    if df.empty:
        return None, None, None, None, None

    # Valor da variável vem na coluna 'V' (string com vírgula)
    df["valor"] = df["V"].str.replace(",", ".", regex=False).astype(float)

    # D3N: nome do período (ex.: 'ago-set-out 2025')
    # D3C: código do período
    df["periodo_label"] = df["D3N"]
    df["periodo_codigo"] = df["D3C"].astype(int)

    # Ano = últimos 4 caracteres do label
    df["ano"] = df["periodo_label"].str[-4:].astype(int)
    # "chave" do trimestre (sem o ano), ex.: 'ago-set-out'
    df["tri_key"] = df["periodo_label"].str[:-5].str.strip()

    # Ordena do mais antigo para o mais recente
    df = df.sort_values(["ano", "periodo_codigo"]).reset_index(drop=True)

    ultimo = df.iloc[-1]
    taxa_atual = float(ultimo["valor"])
    ano_atual = int(ultimo["ano"])
    tri_atual = str(ultimo["tri_key"])

    # --- buscar o mesmo trimestre móvel 12m atrás (ano-1) ---
    linha_12m = df[(df["ano"] == ano_atual - 1) & (df["tri_key"] == tri_atual)]
    taxa_12m = float(linha_12m.iloc[-1]["valor"]) if not linha_12m.empty else None

    # --- buscar o mesmo trimestre móvel 24m atrás (ano-2) ---
    linha_24m = df[(df["ano"] == ano_atual - 2) & (df["tri_key"] == tri_atual)]
    taxa_24m = float(linha_24m.iloc[-1]["valor"]) if not linha_24m.empty else None

    # delta em p.p. versus mesmo trimestre do ano anterior
    delta_pp_aa: Optional[float] = None
    if taxa_12m is not None:
        delta_pp_aa = round(taxa_atual - taxa_12m, 2)

    # Badge estilo "TRI ATÉ OUT/2025"
    try:
        partes_tri = tri_atual.split("-")  # ex.: ['ago', 'set', 'out']
        ult_mes = partes_tri[-1].lower()
        mapa_mes = {
            "jan": "JAN", "fev": "FEV", "mar": "MAR", "abr": "ABR",
            "mai": "MAI", "jun": "JUN", "jul": "JUL", "ago": "AGO",
            "set": "SET", "out": "OUT", "nov": "NOV", "dez": "DEZ",
        }
        mes_fmt = mapa_mes.get(ult_mes, ult_mes.upper())
        referencia = f"TRI ATÉ {mes_fmt}/{ano_atual}"
    except Exception:  # se der qualquer BO, usa o texto original
        referencia = str(ultimo["periodo_label"])

    return taxa_atual, delta_pp_aa, taxa_12m, taxa_24m, referencia


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

URL_TESOURO_RECEITA_LIQ = (
    "https://series-temporais-externo-frontend.tesouro.gov.br/"
    "backend-series-temporais/rest/Public/SerieGrafico/Download/7960"
)

URL_TESOURO_DESPESA_TOTAL = (
    "https://series-temporais-externo-frontend.tesouro.gov.br/"
    "backend-series-temporais/rest/Public/SerieGrafico/Download/7970"
)

PNAD_TAXA_DESOCUPACAO_URL = (
    "https://apisidra.ibge.gov.br/values/"
    "t/6381/n1/1/v/4099/p/all/d/v4099%201?formato=json"
)


def _carregar_resultado_primario_real_ipea_style() -> Tuple[
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[str],   # 6) mês de referência, ex.: "10/2025"
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
    # 1) Baixa a série do Tesouro
    # ---------------------------
    try:
        resp = requests.get(URL_TESOURO_RESULTADO_PRIMARIO, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning(
            "Erro ao baixar série de resultado primário do Tesouro (%s): %s",
            URL_TESOURO_RESULTADO_PRIMARIO,
            exc,
        )
        return (None, None, None, None, None, None, None)


    try:
        # deixa o pandas descobrir o separador (; , ou tab)
        df_prim = pd.read_csv(
            BytesIO(resp.content),
            sep=None,
            engine="python",
            encoding="latin-1",
        )
    except Exception:
        logger.exception(
            "Erro ao ler arquivo de resultado primário (CSV Tesouro)."
        )
        return (None, None, None, None, None, None, None)

    if df_prim.empty:
        logger.error("CSV do Tesouro veio vazio ou ilegível.")
        return (None, None, None, None, None, None, None)
    # ---------------------------
    # 2) Seleciona colunas certas (Data / Valor)
    # ---------------------------
    df_prim.columns = [c.strip() for c in df_prim.columns]

    if "Data" not in df_prim.columns or "Valor" not in df_prim.columns:
        logger.error(
            "CSV do Tesouro sem colunas 'Data'/'Valor': %s",
            list(df_prim.columns),
        )
        return (None, None, None, None, None, None, None)

    df_prim = df_prim[["Data", "Valor"]].copy()
    df_prim.rename(
        columns={"Data": "data", "Valor": "valor_milhoes"},
        inplace=True,
    )

    # normaliza texto bruto da coluna de valores
    val_raw = df_prim["valor_milhoes"].astype(str).str.strip()

    # trata formato com vírgula/ponto (suporta tanto '36526,68' quanto '36526.68')
    tem_virgula = val_raw.str.contains(",", regex=False)
    tem_ponto   = val_raw.str.contains(r"\.", regex=True)

    # caso 1: tem vírgula e ponto -> típico "36.526,68" (ponto milhar, vírgula decimal)
    mask_milhar = tem_virgula & tem_ponto
    val_norm = val_raw.copy()
    val_norm[mask_milhar] = (
        val_norm[mask_milhar]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    # caso 2: só vírgula -> "36526,68" (sem pontuação de milhar)
    mask_so_virgula = tem_virgula & ~tem_ponto
    val_norm[mask_so_virgula] = val_norm[mask_so_virgula].str.replace(
        ",", ".", regex=False
    )

    # demais casos (já com ponto decimal ou número limpo) ficam como estão
    df_prim["valor_milhoes"] = val_norm


    # ---------------------------
    # 3) Converte tipos
    # ---------------------------
    # tira espaços
    df_prim["data"] = df_prim["data"].astype(str).str.strip()

    # datas no formato dd/mm/aaaa (01/10/2025 etc.)
    df_prim["data"] = pd.to_datetime(
        df_prim["data"],
        format="%d/%m/%Y",
        errors="coerce",
    )

    # converte de fato para número (milhões)
    df_prim["valor_milhoes"] = pd.to_numeric(
        df_prim["valor_milhoes"],
        errors="coerce",
    )

    # remove só linhas realmente inválidas
    df_prim = df_prim.dropna(subset=["data", "valor_milhoes"])
    if df_prim.empty:
        logger.error(
            "Após conversão, nenhuma linha válida em data/valor_milhoes."
        )
        return (None, None, None, None, None, None, None)
    
    df_prim = df_prim.sort_values("data")

    # ---------------------------
    # 4) Converte para R$ bi NOMINAIS
    # ---------------------------
    df_prim["valor_bi"] = df_prim["valor_milhoes"] / 1000.0

    # ---------------------------
    # 5) Calcula métricas nominais
    # ---------------------------
    ult = df_prim.iloc[-1]
    data_ult = ult["data"]
    prim_mes_bi = float(ult["valor_bi"])
    
    # mês de referência para o card (formato curto mm/aa, ex.: "10/25")
    if pd.notna(data_ult):
        prim_ref_str = data_ult.strftime("%m/%y")
    else:
        prim_ref_str = None



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

    # acumulado no ano-calendário (jan até o mês de referência)
    ano_ref = data_ult.year
    mes_ref = data_ult.month

    # ano atual: de janeiro até o mês de referência
    mask_ano_atual = (
        (df_prim["data"].dt.year == ano_ref)
        & (df_prim["data"].dt.month <= mes_ref)
    )
    serie_ano_atual = df_prim.loc[mask_ano_atual, "valor_bi"]

    if serie_ano_atual.empty:
        prim_12m_bi = None  # vamos reutilizar o campo como "acum_ano"
    else:
        prim_12m_bi = float(serie_ano_atual.sum())

    # ano anterior: de janeiro até o MESMO mês do ano anterior
    mask_ano_aa = (
        (df_prim["data"].dt.year == ano_ref - 1)
        & (df_prim["data"].dt.month <= mes_ref)
    )
    serie_ano_aa = df_prim.loc[mask_ano_aa, "valor_bi"]

    if serie_ano_aa.empty:
        prim_12m_bi_prev = None
    else:
        prim_12m_bi_prev = float(serie_ano_aa.sum())

    # ---------------------------
    # 6) Retorno nos 6 campos esperados
    # ---------------------------
    return (
        prim_mes_bi,        # 0) mês, R$ bi NOMINAL
        delta_bi_aa,        # 1) delta R$ bi vs mesmo mês a/a
        var_aa_pct,         # 2) var nominal a/a do mês (%)
        None,               # 3) reservado p/ futuro
        prim_12m_bi,        # 4) saldo 12m nominal (R$ bi)
        prim_12m_bi_prev,   # 5) saldo 12m 12m atrás (R$ bi)
        prim_ref_str,       # 6) mês de referência, "10/2025"
    )


def _carregar_receita_despesa_nominal() -> Tuple[
    Optional[float], Optional[float], Optional[float],
    Optional[float], Optional[float], Optional[float],
]:
    """
    Lê duas séries do Tesouro:
      - Receita líquida (10.01.2, URL_TESOURO_RECEITA_LIQ)
      - Despesa total (10.03.1, URL_TESOURO_DESPESA_TOTAL)

    e devolve, nessa ordem:
      0) receita_mes_bi        -> mês em R$ bi
      1) receita_delta_bi_aa   -> delta em R$ bi vs mesmo mês do ano anterior
      2) receita_var_aa_pct    -> variação a/a em %
      3) despesa_mes_bi        -> mês em R$ bi
      4) despesa_delta_bi_aa   -> delta em R$ bi vs mesmo mês do ano anterior
      5) despesa_var_aa_pct    -> variação a/a em %
    """

    def _processar_serie(url: str) -> Tuple[
        Optional[float], Optional[float], Optional[float]
    ]:
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            logger.error("Erro ao baixar série do Tesouro (%s): %s", url, exc)
            return None, None, None

        try:
            df = pd.read_csv(
                BytesIO(resp.content),
                sep=";",
                encoding="latin-1",
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Erro ao ler CSV do Tesouro (%s): %s", url, exc)
            return None, None, None

        if df.empty or df.shape[1] < 2:
            return None, None, None

        col_data = df.columns[0]
        col_val = df.columns[1]

        df = (
            df[[col_data, col_val]]
            .rename(columns={col_data: "data", col_val: "valor_milhoes"})
        )

        df["data"] = pd.to_datetime(
            df["data"].astype(str).str.strip(),
            dayfirst=True,
            errors="coerce",
        )
        df["valor_milhoes"] = pd.to_numeric(
            df["valor_milhoes"],
            errors="coerce",
        )

        df = df.dropna(subset=["data", "valor_milhoes"]).sort_values("data")
        if df.empty:
            return None, None, None

        # converte para R$ bi
        df["valor_bi"] = df["valor_milhoes"] / 1000.0

        # último dado (mês mais recente)
        ult = df.iloc[-1]
        val_bi = float(ult["valor_bi"])

        # mesmo mês do ano anterior (assumindo série mensal contínua)
        if len(df) > 12:
            aa = df.iloc[-13]
            val_bi_aa = float(aa["valor_bi"])
            delta_bi_aa = val_bi - val_bi_aa
            var_aa_pct = (
                (val_bi / val_bi_aa - 1.0) * 100.0 if val_bi_aa != 0 else None
            )
        else:
            delta_bi_aa = None
            var_aa_pct = None

        return val_bi, delta_bi_aa, var_aa_pct

    # Receita líquida (10.01.2)
    receita_mes_bi, receita_delta_bi_aa, receita_var_aa = _processar_serie(
        URL_TESOURO_RECEITA_LIQ
    )

    # Despesa total (10.03.1)
    despesa_mes_bi, despesa_delta_bi_aa, despesa_var_aa = _processar_serie(
        URL_TESOURO_DESPESA_TOTAL
    )

    return (
        receita_mes_bi,
        receita_delta_bi_aa,
        receita_var_aa,
        despesa_mes_bi,
        despesa_delta_bi_aa,
        despesa_var_aa,
    )

def carregar_pnad_desemprego_br() -> pd.DataFrame:
    """
    Baixa a taxa de desocupação da PNAD Contínua (Brasil, 14+ anos)
    da tabela 6381 do SIDRA.

    Retorna um DataFrame com:
      - periodo_label: nome do trimestre móvel (ex.: 'ago-set-out 2025')
      - valor: taxa de desocupação em %
      - ano: ano do período
      - periodo_codigo: código do período (para ordenar)
    """
    resp = requests.get(PNAD_TAXA_DESOCUPACAO_URL, timeout=30)
    resp.raise_for_status()
    dados = resp.json()

    # Primeira linha é cabeçalho -> pula com [1:]
    df = pd.DataFrame(dados[1:])

    # Valor da variável vem na coluna 'V' (string com vírgula)
    df["valor"] = df["V"].str.replace(",", ".", regex=False).astype(float)

    # No SIDRA:
    # - D3N = nome do período (trimestre móvel)
    # - D3C = código do período
    df["periodo_label"] = df["D3N"]
    df["periodo_codigo"] = df["D3C"].astype(int)

    # Ano = últimos 4 caracteres do label (ex.: '2025')
    df["ano"] = df["periodo_label"].str[-4:].astype(int)

    # Ordena do mais antigo pro mais recente
    df = df.sort_values(["ano", "periodo_codigo"]).reset_index(drop=True)

    return df


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

    (
        desemp_atual,
        desemp_delta_pp_aa,
        desemp_12m,
        desemp_24m,
        desemp_ref,
    ) = _carregar_desemprego_pnad()


    # resultado primário
    (
        primario_mes_real_bi,
        primario_mes_delta_real_bi_aa,
        receita_real_var_aa_pct,
        despesa_real_var_aa_pct,
        primario_ano_real_bi,
        primario_ano_real_bi_prev,
        primario_referencia,
    ) = _carregar_resultado_primario_real_ipea_style()


    (
        receita_mes_bi,
        receita_mes_delta_bi_aa,
        receita_mes_var_aa_pct,
        despesa_mes_bi,
        despesa_mes_delta_bi_aa,
        despesa_mes_var_aa_pct,
    ) = _carregar_receita_despesa_nominal()


    return DadosMacroFiscalBr(
        # IBC-Br
        ibcbr_nivel=ibc_nivel,
        ibcbr_var_mom=ibc_var_mom,
        ibcbr_var_aa=ibc_var_aa,
        ibcbr_referencia=ibc_ref,
        ibcbr_var_3m_dessaz=ibc_var_3m,

        # Mercado de trabalho – PNAD Contínua
        desemprego_pnad=desemp_atual,
        desemprego_delta_pp_12m=desemp_delta_pp_aa,
        desemprego_pnad_12m_atras=desemp_12m,
        desemprego_pnad_24m_atras=desemp_24m,
        desemprego_pnad_referencia=desemp_ref,


        # Dívida Bruta GG
        divida_bruta_pct_pib=div_nivel,
        
        # apesar do nome *_12m, AGORA esse campo guarda Δ m/m em p.p.
        divida_bruta_delta_pp_12m=div_delta_mom,
        divida_bruta_pct_pib_12m_atras=div_12m,
        divida_bruta_pct_pib_24m_atras=div_24m,
        divida_bruta_referencia=div_ref,

        # Resultado Primário – Governo Central (mês, valores correntes)
        primario_mes_real_bi=primario_mes_real_bi,
        primario_mes_delta_real_bi_aa=primario_mes_delta_real_bi_aa,
        receita_real_var_aa_pct=receita_real_var_aa_pct,   # var a/a do primário do mês
        despesa_real_var_aa_pct=despesa_real_var_aa_pct,   # (por enquanto fica None)
        primario_ano_real_bi=primario_ano_real_bi,
        primario_ano_real_bi_prev=primario_ano_real_bi_prev,
        primario_referencia=primario_referencia,


        # Receita e despesa (nominais)
        receita_mes_bi=receita_mes_bi,
        receita_mes_delta_bi_aa=receita_mes_delta_bi_aa,
        receita_mes_var_aa_pct=receita_mes_var_aa_pct,
        despesa_mes_bi=despesa_mes_bi,
        despesa_mes_delta_bi_aa=despesa_mes_delta_bi_aa,
        despesa_mes_var_aa_pct=despesa_mes_var_aa_pct,

    )


