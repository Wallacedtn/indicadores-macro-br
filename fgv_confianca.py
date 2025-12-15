# fgv_confianca.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from datetime import datetime, date
from typing import Optional, Dict, Tuple, List
from io import BytesIO
import re
import unicodedata
import subprocess
import shutil
import time as _time

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "sondagens_fgv"
BASE_URL = "https://portalibre.fgv.br"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)

_MESES = {
    "janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4, "maio": 5, "junho": 6,
    "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
}

# TERM IDs (taxonomia FGV IBRE)
# ICC: 111 | ICI: 108 | ICS: 109 | ICOM: 110 | ICST: 165 | ICE: 115
FGV_INDICES = {
    "ICC": {"term_id": 111, "slug_prefix": "icc-de-",  "nome": "Confiança do Consumidor (ICC)"},
    "ICI": {"term_id": 108, "slug_prefix": "ici-de-",  "nome": "Confiança da Indústria (ICI)"},
    "ICS": {"term_id": 109, "slug_prefix": "ics-de-",  "nome": "Confiança de Serviços (ICS)"},
    "ICOM":{"term_id": 110, "slug_prefix": "icom-de-", "nome": "Confiança do Comércio (ICOM)"},
    "ICST":{"term_id": 165, "slug_prefix": "icst-de-", "nome": "Confiança da Construção (ICST)"},
    "ICE": {"term_id": 115, "slug_prefix": "ice-de-",  "nome": "Confiança Empresarial (ICE)"},
}

CONSOLIDADO_PATH = DATA_DIR / "sondagens_fgv_consolidado.csv"


def rebuild_sondagens_fgv_consolidado() -> pd.DataFrame:
    """Recria um CSV único com TODAS as sondagens (ICC/ICI/ICS/ICOM/ICST/ICE).

    Saída: data/sondagens_fgv/sondagens_fgv_consolidado.csv
    Colunas (mínimo): sigla, nome, mes_ref, data_divulgacao, pontos, delta_pts, url, titulo
    """
    frames: List[pd.DataFrame] = []
    for sig, meta in FGV_INDICES.items():
        p = DATA_DIR / f"{sig.lower()}_fgv.csv"
        if not p.exists() or p.stat().st_size == 0:
            continue

        try:
            df = pd.read_csv(p)
        except Exception:
            continue

        if df is None or df.empty:
            continue

        # garante colunas mínimas
        for c in ["mes_ref", "data_divulgacao", "pontos", "delta_pts", "url", "titulo"]:
            if c not in df.columns:
                df[c] = ""

        df = df[["mes_ref", "data_divulgacao", "pontos", "delta_pts", "url", "titulo"]].copy()
        df.insert(0, "nome", meta.get("nome", sig))
        df.insert(0, "sigla", sig)
        frames.append(df)

    out_cols = ["sigla", "nome", "mes_ref", "data_divulgacao", "pontos", "delta_pts", "url", "titulo"]
    if not frames:
        df_out = pd.DataFrame(columns=out_cols)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(CONSOLIDADO_PATH, index=False)
        return df_out

    df_all = pd.concat(frames, ignore_index=True)

    # limpeza + dedup
    df_all["mes_ref"] = df_all["mes_ref"].astype(str).str.strip()
    df_all = df_all[df_all["mes_ref"].str.match(r"^\d{4}-\d{2}$", na=False)].copy()

    df_all["pontos"] = pd.to_numeric(df_all["pontos"], errors="coerce")
    df_all["delta_pts"] = pd.to_numeric(df_all["delta_pts"], errors="coerce")
    df_all["_dt"] = pd.to_datetime(df_all["data_divulgacao"], errors="coerce")

    df_all = (
        df_all.sort_values(["sigla", "mes_ref", "_dt"], na_position="last")
        .drop_duplicates(subset=["sigla", "mes_ref"], keep="last")
        .drop(columns=["_dt"])
    )

    # ordena final
    df_all = df_all.sort_values(["sigla", "mes_ref"]).reset_index(drop=True)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(CONSOLIDADO_PATH, index=False)
    print(f"[FGV] Consolidado atualizado em {CONSOLIDADO_PATH} ({len(df_all)} linhas)")
    return df_all


def _carregar_sondagem(sigla: str) -> pd.DataFrame:
    """Carrega histórico para 1 sigla a partir do consolidado (preferência) ou do CSV individual."""
    sigla = sigla.upper()

    # 1) tenta consolidado (preferência)
    if CONSOLIDADO_PATH.exists() and CONSOLIDADO_PATH.stat().st_size > 0:
        try:
            dfc = pd.read_csv(CONSOLIDADO_PATH)
            if dfc is not None and not dfc.empty and "sigla" in dfc.columns:
                dfc = dfc[dfc["sigla"].astype(str).str.upper() == sigla].copy()
                if not dfc.empty:
                    return dfc
        except Exception:
            pass

    # 2) fallback no arquivo individual
    csv_path = DATA_DIR / f"{sigla.lower()}_fgv.csv"
    if csv_path.exists() and csv_path.stat().st_size > 0:
        try:
            return pd.read_csv(csv_path)
        except Exception:
            return pd.DataFrame()

    return pd.DataFrame()



def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _curl_bytes(url: str) -> bytes:
    curl = shutil.which("curl.exe") or shutil.which("curl")
    if not curl:
        raise RuntimeError("curl.exe não encontrado no PATH (necessário para fallback TLS).")

    r = subprocess.run(
        [
            curl, "-L", "--compressed",
            "-H", f"User-Agent: {UA}",
            "-H", "Accept-Language: pt-BR,pt;q=0.9,en;q=0.8",
            url,
        ],
        capture_output=True,
        check=False,
    )

    if r.returncode != 0:
        err = (r.stderr or b"").decode("utf-8", errors="replace")
        raise RuntimeError(f"curl falhou (code={r.returncode}): {err}")

    return r.stdout or b""


def _fetch_text(url: str) -> str:
    # tenta requests; se SSL falhar, usa curl.exe (TLS do Windows)
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": UA, "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"},
            timeout=25,
        )
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.SSLError:
        return _curl_bytes(url).decode("utf-8", errors="replace")


def _fetch_bytes(url: str) -> bytes:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": UA, "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"},
            timeout=25,
        )
        resp.raise_for_status()
        return resp.content
    except requests.exceptions.SSLError:
        return _curl_bytes(url)


def _parse_title(html: str) -> str:
    m = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    t = re.sub(r"\s+", " ", m.group(1)).strip()
    t = t.replace(" | Portal IBRE", "").replace(" | FGV IBRE", "").strip()
    return t


def _parse_date_divulgacao(html: str) -> Optional[date]:
    # dd/mm/yyyy - hh:mm
    m = re.search(r"(\d{2}/\d{2}/\d{4})\s*-\s*\d{2}:\d{2}", html)
    if m:
        try:
            return datetime.strptime(m.group(1), "%d/%m/%Y").date()
        except Exception:
            pass

    # qualquer dd/mm/yyyy
    m2 = re.search(r"(\d{2}/\d{2}/\d{4})", html)
    if m2:
        try:
            return datetime.strptime(m2.group(1), "%d/%m/%Y").date()
        except Exception:
            pass

    # json-ld / meta
    m3 = re.search(r'datePublished[^0-9]*(\d{4}-\d{2}-\d{2})', html)
    if m3:
        try:
            return datetime.strptime(m3.group(1), "%Y-%m-%d").date()
        except Exception:
            pass

    m4 = re.search(r'content="(\d{4}-\d{2}-\d{2})T', html)
    if m4:
        try:
            return datetime.strptime(m4.group(1), "%Y-%m-%d").date()
        except Exception:
            pass

    return None


def _parse_mes_ref_from_title_or_slug(sigla: str, title: str, url: str) -> Optional[str]:
    # ex: "ICC de novembro de 2025" ou slug ".../icc-de-novembro-de-2025"
    sig = sigla.lower()
    t = _strip_accents(title.lower()).replace("ç", "c")

    m = re.search(rf"{sig}\s+de\s+([a-z]+)\s+de\s+(\d{{4}})", t)
    if m:
        mes_nome = m.group(1)
        ano = int(m.group(2))
        mes_num = _MESES.get(mes_nome)
        if mes_num:
            return f"{ano:04d}-{mes_num:02d}"

    slug = url.rstrip("/").split("/")[-1]
    slug = _strip_accents(slug.lower()).replace("ç", "c")
    m2 = re.search(rf"{sig}-de-([a-z]+)-de-(\d{{4}})", slug)
    if m2:
        mes_nome = m2.group(1)
        ano = int(m2.group(2))
        mes_num = _MESES.get(mes_nome)
        if mes_num:
            return f"{ano:04d}-{mes_num:02d}"

    return None


def _html_to_text(html: str) -> str:
    txt = re.sub(r"<[^>]+>", " ", html)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _parse_valor_e_delta_from_text(text: str) -> Tuple[Optional[float], Optional[float]]:
    tn = _strip_accents(text.lower()).replace("ç", "c")

    def _to_float(s: str) -> Optional[float]:
        try:
            return float(s.replace(",", "."))
        except Exception:
            return None

    # NIVEL: aceita "para 89,5 pontos" e "em 89,5 pontos" (com ou sem decimal)
    valor: Optional[float] = None
    m_val = re.search(r"(?:para|em)\s+(\d{1,3}(?:[.,]\d)?)\s+pontos?", tn)
    if m_val:
        valor = _to_float(m_val.group(1))

    # DELTA: cobre "recuou 0,1", "ao recuar 0,1", "avancou", "ao avancar", etc.
    delta: Optional[float] = None

    m_del = re.search(
        r"(?:ao\s+)?(subiu|subir|avancou|avancar|aumentou|aumentar|caiu|cair|recuou|recuar|cedeu|ceder)"
        r"\s+(?:em\s+|de\s+)?(\d{1,3}(?:[.,]\d)?)\s+pontos?",
        tn
    )
    if m_del:
        v = _to_float(m_del.group(2))
        if v is not None:
            verbo = m_del.group(1)
            if verbo in ("caiu", "cair", "recuou", "recuar", "cedeu", "ceder"):
                v = -v
            delta = v

    # estabilidade com palavra no meio ("ficou praticamente estável")
    if delta is None:
        if re.search(r"(ficou|permaneceu|manteve\s*-?se)(?:\s+\w+){0,2}\s+estavel", tn):
            delta = 0.0


    return valor, delta


def _calc_delta_from_history(df_old: pd.DataFrame, mes_ref: str, nivel_atual: float) -> Optional[float]:
    """Calcula delta (pontos) pelo histórico salvo, quando o release não informa a variação.

    Usa o último 'pontos' disponível do mês imediatamente anterior no CSV (ordenado por mes_ref).
    Retorna None se não houver histórico suficiente.
    """
    if df_old is None or df_old.empty:
        return None

    dfh = df_old.copy()
    if "mes_ref" not in dfh.columns or "pontos" not in dfh.columns:
        return None

    dfh["mes_ref"] = dfh["mes_ref"].astype(str).str.strip()
    dfh = dfh[dfh["mes_ref"].str.match(r"^\d{4}-\d{2}$", na=False)].copy()
    if dfh.empty:
        return None

    dfh["pontos"] = pd.to_numeric(dfh["pontos"], errors="coerce")
    dfh = dfh.dropna(subset=["pontos"]).copy()
    if dfh.empty:
        return None

    # remove o próprio mês (se já existir no histórico)
    dfh = dfh[dfh["mes_ref"] != str(mes_ref)].copy()
    if dfh.empty:
        return None

    # pega o último mês disponível no histórico
    dfh = dfh.sort_values("mes_ref")
    prev_pontos = dfh.iloc[-1]["pontos"]
    try:
        return float(nivel_atual) - float(prev_pontos)
    except Exception:
        return None

def _extract_pdf_url_from_release_html(html: str) -> Optional[str]:
    matches = re.findall(r'href="([^"]+\.pdf[^"]*)"', html, flags=re.IGNORECASE)
    if not matches:
        return None

    def score(u: str) -> int:
        ul = u.lower()
        s = 0
        if "sites/default/files" in ul:
            s += 10
        return -s

    matches = sorted(matches, key=score)
    pdf = matches[0]
    if pdf.startswith("/"):
        return BASE_URL + pdf
    return pdf


def _parse_valor_e_delta_from_pdf(pdf_bytes: bytes) -> Tuple[Optional[float], Optional[float]]:
    try:
        import pdfplumber
    except Exception as e:
        raise RuntimeError("pdfplumber não está instalado. Rode: pip install pdfplumber") from e

    texto = ""
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages[:3]:
            texto += "\n" + (page.extract_text() or "")

    if not texto.strip():
        return None, None

    return _parse_valor_e_delta_from_text(texto)


def _extract_release_urls_from_taxonomy(html: str, sigla: str, slug_prefix: str, max_links: int = 20) -> List[str]:
    # pega links /press-releases/...
    links = re.findall(r'href="(/press-releases/[^"]+)"', html)
    seen = set()
    prioridade = []
    outros = []

    for p in links:
        if p in seen:
            continue
        seen.add(p)

        slug = p.lower()
        if slug_prefix in slug:
            prioridade.append(BASE_URL + p)
        elif sigla.lower() in slug:
            outros.append(BASE_URL + p)

    out = prioridade + outros
    return out[:max_links]


def importar_sondagens_fgv_csv_wide(caminho_csv: str | Path, salvar_individual: bool = False) -> pd.DataFrame:
    """Importa o CSV 'largo' (uma coluna por índice) e gera o consolidado.

    Esse é o seu caso quando você baixa um único CSV do portal da FGV (com Data = mm/aaaa e colunas ICE/ICS/...)
    Ele vira o padrão 'longo' do projeto e alimenta os cards sem depender de scraping.

    - Entrada: CSV separado por ';' (normalmente Windows-1252/latin1) com decimais em vírgula.
    - Saída: data/sondagens_fgv/sondagens_fgv_consolidado.csv (e, opcionalmente, os {sigla}_fgv.csv)
    """
    caminho_csv = Path(caminho_csv)

    if (not caminho_csv.exists()) or (caminho_csv.stat().st_size == 0):
        raise FileNotFoundError(f"CSV não encontrado ou vazio: {caminho_csv}")

    # leitura robusta (portal costuma exportar com ';' e encoding latin1)
    dfw = pd.read_csv(caminho_csv, sep=";", encoding="latin1")
    if dfw is None or dfw.empty:
        raise ValueError("CSV wide vazio.")

    # coluna de data (mm/aaaa)
    col_data = None
    for c in dfw.columns:
        if str(c).strip().lower() in ("data", "date"):
            col_data = c
            break
    if not col_data:
        # assume primeira coluna
        col_data = dfw.columns[0]

    def _parse_mes_ref(x: object) -> str | None:
        s = str(x).strip()
        m = re.match(r"^(\d{2})/(\d{4})$", s)
        if not m:
            return None
        mm, yyyy = int(m.group(1)), int(m.group(2))
        if 1 <= mm <= 12:
            return f"{yyyy:04d}-{mm:02d}"
        return None

    dfw["mes_ref"] = dfw[col_data].apply(_parse_mes_ref)
    dfw = dfw.dropna(subset=["mes_ref"]).copy()

    def _sigla_from_col(col: str) -> str | None:
        c = _strip_accents(str(col)).lower()
        if "empresarial" in c:
            return "ICE"
        if "servic" in c:
            return "ICS"
        if "varejo" in c or "comerc" in c:
            return "ICOM"
        if "constr" in c:
            return "ICST"
        if "consumidor" in c:
            return "ICC"
        if "industr" in c:
            return "ICI"
        # fallback por siglas explícitas
        for sig in FGV_INDICES.keys():
            if sig.lower() in c:
                return sig
        return None

    long_frames: List[pd.DataFrame] = []

    for col in dfw.columns:
        if col in (col_data, "mes_ref"):
            continue

        sig = _sigla_from_col(col)
        if not sig:
            continue

        s = dfw[["mes_ref", col]].copy()
        s.rename(columns={col: "pontos"}, inplace=True)

        # limpa números ("104,800" -> 104.8) e trata '-' como NaN
        s["pontos"] = (
            s["pontos"]
            .astype(str)
            .str.replace("-", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        s["pontos"] = pd.to_numeric(s["pontos"], errors="coerce")
        s = s.dropna(subset=["pontos"]).copy()

        if s.empty:
            continue

        s.insert(0, "sigla", sig)
        s.insert(1, "nome", FGV_INDICES[sig]["nome"])
        s["data_divulgacao"] = ""  # não vem no CSV (e você decidiu não usar)
        s["delta_pts"] = s.sort_values("mes_ref")["pontos"].diff()
        s["url"] = ""
        s["titulo"] = ""

        long_frames.append(s[["sigla", "nome", "mes_ref", "data_divulgacao", "pontos", "delta_pts", "url", "titulo"]])

    if not long_frames:
        raise ValueError("Não consegui mapear colunas do CSV wide para siglas (ICE/ICS/ICOM/ICST/ICC/ICI).")

    df_long = pd.concat(long_frames, ignore_index=True)
    df_long = df_long.sort_values(["sigla", "mes_ref"]).reset_index(drop=True)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df_long.to_csv(CONSOLIDADO_PATH, index=False)
    print(f"[FGV] Consolidado gerado a partir do CSV wide: {CONSOLIDADO_PATH} ({len(df_long)} linhas)")

    if salvar_individual:
        for sig in df_long["sigla"].unique():
            d = df_long[df_long["sigla"] == sig].copy()
            d = d[["mes_ref", "data_divulgacao", "pontos", "delta_pts", "url", "titulo"]]
            (DATA_DIR / f"{sig.lower()}_fgv.csv").write_text(d.to_csv(index=False), encoding="utf-8")

    return df_long


def atualizar_fgv_indice(
    sigla: str,
    max_meses: int = 240,
    max_pages: int = 30,
    sleep_s: float = 0.2,
    update_consolidado: bool = True,
) -> pd.DataFrame:
    """Atualiza o CSV individual (data/sondagens_fgv/{sigla}_fgv.csv).

    Observação: o app deve LER do CSV local. Este script é para rodar via tarefa agendada.
    Se update_consolidado=True, também recria o consolidado único ao final.
    """
    sigla = sigla.upper()
    if sigla not in FGV_INDICES:
        raise ValueError(f"Sigla inválida: {sigla}. Use uma de: {list(FGV_INDICES.keys())}")

    term_id = FGV_INDICES[sigla]["term_id"]
    slug_prefix = FGV_INDICES[sigla]["slug_prefix"]
    term_url = f"{BASE_URL}/taxonomy/term/{term_id}"
    csv_path = DATA_DIR / f"{sigla.lower()}_fgv.csv"

    # histórico existente
    if csv_path.exists() and csv_path.stat().st_size > 0:
        try:
            df_old = pd.read_csv(csv_path)
        except Exception:
            df_old = pd.DataFrame()
    else:
        df_old = pd.DataFrame()

    # coleta URLs (paginação do taxonomy)
    all_urls: List[str] = []
    seen_urls = set()

    for page in range(max_pages):
        term_page = term_url if page == 0 else f"{term_url}?page={page}"
        try:
            html_term = _fetch_text(term_page)
        except Exception:
            break

        urls_page = _extract_release_urls_from_taxonomy(
            html_term, sigla=sigla, slug_prefix=slug_prefix, max_links=500
        )

        novos = 0
        for u in urls_page:
            if u not in seen_urls:
                seen_urls.add(u)
                all_urls.append(u)
                novos += 1

        if novos == 0:
            break

        if sleep_s:
            _time.sleep(sleep_s)

    if not all_urls:
        print(f"[FGV/{sigla}] Nenhum /press-releases encontrado. Mantendo CSV anterior.")
        return df_old

    # processa releases e monta linhas
    rows: List[Dict[str, object]] = []
    for url_release in all_urls[:max_meses]:
        try:
            html_rel = _fetch_text(url_release)
            titulo = _parse_title(html_rel)
            data_div = _parse_date_divulgacao(html_rel)
            mes_ref = _parse_mes_ref_from_title_or_slug(sigla, titulo, url_release)

            # fallback mês ref: usa mês da data de divulgação
            if (not mes_ref) and isinstance(data_div, date):
                mes_ref = f"{data_div.year:04d}-{data_div.month:02d}"

            texto_rel = _html_to_text(html_rel)
            nivel, delta = _parse_valor_e_delta_from_text(texto_rel)

            # fallback PDF (quando HTML não traz)
            if nivel is None or delta is None:
                pdf_url = _extract_pdf_url_from_release_html(html_rel)
                if pdf_url:
                    try:
                        pdf_bytes = _fetch_bytes(pdf_url)
                        n_pdf, d_pdf = _parse_valor_e_delta_from_pdf(pdf_bytes)
                        if nivel is None and n_pdf is not None:
                            nivel = n_pdf
                        if delta is None and d_pdf is not None:
                            delta = d_pdf
                    except Exception as e:
                        print(f"[FGV/{sigla}] Falha PDF ({pdf_url}): {e}")

            # precisa ter mês e nível
            if (nivel is None) or (not mes_ref) or (not re.match(r"^\d{4}-\d{2}$", str(mes_ref))):
                continue

            rows.append(
                {
                    "mes_ref": str(mes_ref),
                    "data_divulgacao": data_div.isoformat() if isinstance(data_div, date) else "",
                    "pontos": nivel,
                    "delta_pts": delta,
                    "url": url_release,
                    "titulo": titulo,
                }
            )

            if sleep_s:
                _time.sleep(sleep_s)

        except Exception:
            continue

    if not rows:
        print(f"[FGV/{sigla}] Não consegui extrair nenhum mês válido. Mantendo CSV anterior.")
        return df_old

    df_new = pd.DataFrame(rows)
    df = df_new if (df_old is None or df_old.empty) else pd.concat([df_old, df_new], ignore_index=True)

    # normaliza + dedup por mes_ref (mantém o mais recente por data_divulgacao)
    for c in ["mes_ref", "data_divulgacao", "url", "titulo"]:
        if c not in df.columns:
            df[c] = ""
    if "pontos" not in df.columns:
        df["pontos"] = None
    if "delta_pts" not in df.columns:
        df["delta_pts"] = None

    df["mes_ref"] = df["mes_ref"].astype(str).str.strip()
    df = df[df["mes_ref"].str.match(r"^\d{4}-\d{2}$", na=False)].copy()

    df["pontos"] = pd.to_numeric(df["pontos"], errors="coerce")
    df["delta_pts"] = pd.to_numeric(df["delta_pts"], errors="coerce")
    df["_dt"] = pd.to_datetime(df["data_divulgacao"], errors="coerce")

    df = (
        df.sort_values(["mes_ref", "_dt"], na_position="last")
        .drop_duplicates(subset=["mes_ref"], keep="last")
        .drop(columns=["_dt"])
        .reset_index(drop=True)
    )

    # fallback de delta: se não veio do release, calcula pela diferença m/m do próprio histórico
    df = df.sort_values("mes_ref").reset_index(drop=True)
    delta_calc = df["pontos"].diff()
    df.loc[df["delta_pts"].isna(), "delta_pts"] = delta_calc[df["delta_pts"].isna()]

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    print(f"[FGV/{sigla}] CSV atualizado em {csv_path} ({len(df)} linhas). Último mês: {df['mes_ref'].max()}")

    if update_consolidado:
        rebuild_sondagens_fgv_consolidado()

    return df


def resumo_fgv_indice(sigla: str) -> Dict[str, object]:
    """
    Retorna o último dado do índice (referência mm/aaaa, nível e delta m/m em pts).

    PRIORIDADE:
    1) data/sondagens_fgv/sondagens_fgv_consolidado.csv
    2) data/sondagens_fgv/{sigla}_fgv.csv  (fallback legado)
    """
    sigla = sigla.upper()

    def _fmt_ref(mes_ref: str) -> str:
        if isinstance(mes_ref, str) and re.match(r"^\d{4}-\d{2}$", mes_ref):
            return f"{mes_ref[5:7]}/{mes_ref[0:4]}"
        return "-"

    # ---------- 1) consolidado (preferencial) ----------
    try:
        if "CONSOLIDADO_PATH" in globals() and CONSOLIDADO_PATH.exists() and CONSOLIDADO_PATH.stat().st_size > 0:
            dfc = pd.read_csv(CONSOLIDADO_PATH)
            if not dfc.empty and "sigla" in dfc.columns:
                dfi = dfc[dfc["sigla"].astype(str).str.upper() == sigla].copy()
                if not dfi.empty:
                    dfi["mes_ref"] = dfi["mes_ref"].astype(str)
                    dfi = dfi[dfi["mes_ref"].str.match(r"^\d{4}-\d{2}$", na=False)].copy()
                    dfi["pontos"] = pd.to_numeric(dfi.get("pontos"), errors="coerce")
                    dfi["delta_pts"] = pd.to_numeric(dfi.get("delta_pts"), errors="coerce")

                    dfi = dfi.dropna(subset=["mes_ref", "pontos"]).sort_values("mes_ref")
                    if not dfi.empty:
                        last = dfi.iloc[-1]
                        mes_ref = str(last["mes_ref"])
                        nivel = float(last["pontos"]) if pd.notna(last["pontos"]) else None

                        delta = last.get("delta_pts", None)
                        delta = float(delta) if pd.notna(delta) else None

                        # se delta vier vazio no consolidado, calcula pela diferença do mês anterior
                        if delta is None and len(dfi) >= 2:
                            prev = dfi.iloc[-2]
                            if pd.notna(prev["pontos"]):
                                delta = float(last["pontos"]) - float(prev["pontos"])

                        return {
                            "referencia": _fmt_ref(mes_ref),
                            "nivel": nivel,
                            "delta_pts": delta,
                            "data_divulgacao": None,
                            "url": None,
                        }
    except Exception:
        pass

    # ---------- 2) fallback legado (CSV por sigla) ----------
    csv_path = DATA_DIR / f"{sigla.lower()}_fgv.csv"
    if (not csv_path.exists()) or (csv_path.stat().st_size == 0):
        return {"referencia": "-", "nivel": None, "delta_pts": None, "data_divulgacao": None, "url": None}

    df = pd.read_csv(csv_path)
    if df.empty:
        return {"referencia": "-", "nivel": None, "delta_pts": None, "data_divulgacao": None, "url": None}

    df["mes_ref"] = df["mes_ref"].astype(str)
    df["_dt"] = pd.to_datetime(df.get("data_divulgacao"), errors="coerce")
    df = df.sort_values(["mes_ref", "_dt"], na_position="last")

    last = df.iloc[-1]
    mes_ref = str(last.get("mes_ref", ""))
    nivel = last.get("pontos", None)
    delta = last.get("delta_pts", None)

    nivel = float(nivel) if pd.notna(nivel) else None
    delta = float(delta) if pd.notna(delta) else None

    return {
        "referencia": _fmt_ref(mes_ref),
        "nivel": nivel,
        "delta_pts": delta,
        "data_divulgacao": last.get("data_divulgacao", None),
        "url": last.get("url", None),
    }


