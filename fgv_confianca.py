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

    valor = None
    m_val = re.search(r"para\s+(\d{1,3},\d)\s+pontos", tn)
    if m_val:
        try:
            valor = float(m_val.group(1).replace(",", "."))
        except Exception:
            valor = None

    delta = None
    m_del = re.search(r"(subiu|avancou|aumentou|caiu|recuou|cedeu)\s+(\d{1,3},\d)\s+ponto", tn)
    if m_del:
        try:
            v = float(m_del.group(2).replace(",", "."))
            verbo = m_del.group(1)
            if verbo in ("caiu", "recuou", "cedeu"):
                v = -v
            delta = v
        except Exception:
            delta = None

    return valor, delta


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


def atualizar_fgv_indice(sigla: str) -> pd.DataFrame:
    sigla = sigla.upper()
    if sigla not in FGV_INDICES:
        raise ValueError(f"Sigla inválida: {sigla}. Use uma de: {list(FGV_INDICES.keys())}")

    term_id = FGV_INDICES[sigla]["term_id"]
    slug_prefix = FGV_INDICES[sigla]["slug_prefix"]
    term_url = f"{BASE_URL}/taxonomy/term/{term_id}"
    csv_path = DATA_DIR / f"{sigla.lower()}_fgv.csv"

    # carrega histórico existente
    if csv_path.exists() and csv_path.stat().st_size > 0:
        df_old = pd.read_csv(csv_path)
    else:
        df_old = pd.DataFrame(columns=["mes_ref","data_divulgacao","pontos","delta_pts","url","titulo"])

    html_term = _fetch_text(term_url)
    urls = _extract_release_urls_from_taxonomy(html_term, sigla=sigla, slug_prefix=slug_prefix, max_links=20)
    if not urls:
        print(f"[FGV/{sigla}] Nenhum /press-releases encontrado. Mantendo CSV anterior.")
        return df_old

    # escolhe o melhor candidato (primeiro já é o mensal pela prioridade)
    url_release = urls[0]
    html_rel = _fetch_text(url_release)

    titulo = _parse_title(html_rel)
    data_div = _parse_date_divulgacao(html_rel)
    mes_ref = _parse_mes_ref_from_title_or_slug(sigla, titulo, url_release)

    # fallback mês ref: usa mês da data de divulgação
    if (not mes_ref) and isinstance(data_div, date):
        mes_ref = f"{data_div.year:04d}-{data_div.month:02d}"

    texto_rel = _html_to_text(html_rel)
    nivel, delta = _parse_valor_e_delta_from_text(texto_rel)

    # fallback PDF
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

    # FAIL-SAFE: se não conseguiu nível ou mes_ref válido, não grava
    if (nivel is None) or (not mes_ref) or (not re.match(r"^\d{4}-\d{2}$", str(mes_ref))):
        print(f"[FGV/{sigla}] Falha extração (HTML+PDF). Mantendo CSV anterior.")
        return df_old

    row = {
        "mes_ref": mes_ref,
        "data_divulgacao": data_div.isoformat() if isinstance(data_div, date) else "",
        "pontos": nivel,
        "delta_pts": delta if delta is not None else "",
        "url": url_release,
        "titulo": titulo,
    }
    df_new = pd.DataFrame([row])

    df = df_new if df_old.empty else pd.concat([df_old, df_new], ignore_index=True)

    df["mes_ref"] = df["mes_ref"].astype(str).str.strip()
    df["data_divulgacao"] = df["data_divulgacao"].astype(str).str.strip()
    df["pontos"] = pd.to_numeric(df["pontos"], errors="coerce")
    df["delta_pts"] = pd.to_numeric(df["delta_pts"], errors="coerce")

    df = df[df["mes_ref"].str.match(r"^\d{4}-\d{2}$", na=False)].copy()

    df["_dt"] = pd.to_datetime(df["data_divulgacao"], errors="coerce")
    df = df.sort_values(["mes_ref", "_dt"]).drop_duplicates(subset=["mes_ref"], keep="last").drop(columns=["_dt"])

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)

    print(f"[FGV/{sigla}] CSV atualizado em {csv_path} ({len(df)} linhas). Último mês: {df['mes_ref'].max()}")
    return df


def resumo_fgv_indice(sigla: str) -> Dict[str, object]:
    sigla = sigla.upper()
    csv_path = DATA_DIR / f"{sigla.lower()}_fgv.csv"
    if (not csv_path.exists()) or (csv_path.stat().st_size == 0):
        return {"referencia": "-", "nivel": None, "delta_pts": None, "data_divulgacao": None, "url": None}

    df = pd.read_csv(csv_path)
    if df.empty:
        return {"referencia": "-", "nivel": None, "delta_pts": None, "data_divulgacao": None, "url": None}

    df["mes_ref"] = df["mes_ref"].astype(str)
    df["_dt"] = pd.to_datetime(df["data_divulgacao"], errors="coerce")
    df = df.sort_values(["mes_ref", "_dt"], na_position="last")

    last = df.iloc[-1]
    mes_ref = str(last.get("mes_ref", ""))
    referencia = "-"
    if re.match(r"^\d{4}-\d{2}$", mes_ref):
        referencia = f"{mes_ref[5:7]}/{mes_ref[0:4]}"

    nivel = last.get("pontos", None)
    delta = last.get("delta_pts", None)

    nivel = float(nivel) if pd.notna(nivel) else None
    delta = float(delta) if pd.notna(delta) else None

    return {
        "referencia": referencia,
        "nivel": nivel,
        "delta_pts": delta,
        "data_divulgacao": last.get("data_divulgacao", None),
        "url": last.get("url", None),
    }
