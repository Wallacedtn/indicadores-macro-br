# bloco_curto_prazo_br.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Optional

import streamlit as st

from dados_curto_prazo_br import carregar_dados_curto_prazo_br

# =============================================================================
# PALETA (inspirada em tons escuros + verde limão)
# =============================================================================
ION_DARK_1 = "#0A1A1D"      # fundo primário escuro
ION_DARK_2 = "#12343E"      # fundo secundário / gradiente
ION_LIME = "#B1D335"        # verde-limão (positivo)
ION_GREY = "#D0D8D0"        # cinza claro

ION_TEXT_PRIMARY = "#F9FAFB"
ION_TEXT_MUTED = "#9CA3AF"

ION_DELTA_POS = ION_LIME
ION_DELTA_NEG = "#FF7A8A"   # coral para queda
ION_DELTA_NEU = ION_GREY


# =============================================================================
# ÍCONES SVG GENÉRICOS (desenhados do zero)
# =============================================================================

ICON_PERCENT = """
<div class="ion-icon">
  <svg viewBox="0 0 24 24" width="20" height="20">
    <line x1="5" y1="19" x2="19" y2="5" />
    <circle cx="8" cy="8" r="2.2" />
    <circle cx="16" cy="16" r="2.2" />
  </svg>
</div>
"""

ICON_CHART = """
<div class="ion-icon">
  <svg viewBox="0 0 24 24" width="20" height="20">
    <polyline points="4 16 9 11 13 14 20 7" />
    <polyline points="4 4 4 20 20 20" />
  </svg>
</div>
"""

ICON_DOLLAR = """
<div class="ion-icon">
  <svg viewBox="0 0 24 24" width="20" height="20">
    <path d="M12 2v20" />
    <path d="M17 7c0-2.2-2.2-3-5-3s-5 0.8-5 3 2.2 3 5 3 5 0.8 5 3-2.2 3-5 3-5-0.8-5-3" />
  </svg>
</div>
"""


# =============================================================================
# HELPERS – formatação BR
# =============================================================================

def _us_to_br_str(s: str) -> str:
    """
    Converte string numérica do formato US (1,234.56)
    para formato BR (1.234,56).
    Funciona mesmo quando há prefixos tipo 'R$ '.
    """
    if "," not in s and "." not in s:
        return s

    prefix = ""
    rest = s
    while rest and not (rest[0].isdigit() or rest[0] in "-+"):
        prefix += rest[0]
        rest = rest[1:]

    if not rest:
        return s

    if "," in rest and "." in rest:
        rest = rest.replace(",", "X").replace(".", ",").replace("X", ".")
    elif "," in rest:
        rest = rest.replace(",", ".")
    elif "." in rest:
        rest = rest.replace(".", ",")

    return prefix + rest


def _format_value_br(value: float, fmt_value: str) -> str:
    """Aplica fmt_value e converte o resultado para notação BR."""
    raw = fmt_value.format(value)
    return _us_to_br_str(raw)


def _format_delta_br(value: float, decimals: int) -> str:
    """Formata número de delta com casas decimais em notação BR."""
    raw = f"{value:.{decimals}f}"
    return _us_to_br_str(raw)


# =============================================================================
# CSS
# =============================================================================

def _inject_ion_css_curto_prazo() -> None:
    """Injeta CSS dos cards (chamado em todo rerun)."""
    st.markdown(
        f"""<style>
.ion-card {{
    position: relative;
    background: linear-gradient(135deg, {ION_DARK_1}, {ION_DARK_2});
    border-radius: 16px;
    border: 1px solid {ION_DARK_2};
    padding: 18px 22px;
    margin-bottom: 14px;
    box-shadow: 0 0 0 1px rgba(3, 7, 18, 0.8);
    transition: all 0.18s ease-out;
}}
.ion-card:hover {{
    border-color: {ION_LIME};
    box-shadow: 0 18px 35px rgba(0, 0, 0, 0.6);
    transform: translateY(-2px);
}}

.ion-label {{
    font-size: 0.80rem;
    font-weight: 500;
    color: {ION_TEXT_MUTED};
    opacity: 0.92;
    margin-bottom: 4px;
}}

.ion-value {{
    font-size: 1.95rem;
    font-weight: 650;
    color: {ION_TEXT_PRIMARY};
    margin-bottom: 8px;
    line-height: 1.1;
}}

.ion-delta {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 0.78rem;
    padding: 3px 10px;
    border-radius: 999px;
    font-weight: 600;
}}

.ion-delta-up {{
    background: rgba(60, 179, 113, 0.15);
    color: #7BE27B;
}}

.ion-delta-down {{
    background: rgba(255, 99, 132, 0.10);
    color: #FF7B9C;
}}

.ion-delta-flat {{
    background: rgba(148, 163, 184, 0.08);
    color: {ION_TEXT_MUTED};
}}

.ion-icon {{
    width: 40px;
    height: 40px;
    border-radius: 999px;
    border: 1px solid rgba(177, 211, 53, 0.4);
    display: flex;
    align-items: center;
    justify-content: center;
    margin-bottom: 6px;
}}

.ion-icon svg {{
    width: 20px;
    height: 20px;
    stroke: {ION_LIME};
    fill: none;
    stroke-width: 2;
}}
        </style>""",
        unsafe_allow_html=True,
    )


# =============================================================================
# HELPER – card
# =============================================================================

def metric_card(
    label: str,
    value: Optional[float],
    delta: Optional[float],
    *,
    fmt_value: str = "{:.2f}",
    value_is_pct: bool = False,
    delta_is_pct: bool = False,
    delta_is_pp: bool = False,
    badge: Optional[str] = None,
    icon_html: Optional[str] = None,
) -> None:
    """
    Desenha um card:
      - ícone (opcional)
      - label (título)
      - valor principal (formato BR)
      - delta com seta:
          ▲ verde (delta > 0)
          ▼ coral (delta < 0)
          ↔ cinza (delta == 0)
      - badge opcional (ex.: INTRADAY, VS COPOM)
    """

    # valor principal
    if value is None:
        display_value = "--"
    else:
        display_value = _format_value_br(value, fmt_value)
        if value_is_pct:
            display_value += "%"

    # delta
    if delta is None:
        arrow = ""
        delta_class = "ion-delta-neu"
        delta_txt = ""
    else:
        if delta > 0:
            arrow = "▲"
            delta_class = "ion-delta-pos"
        elif delta < 0:
            arrow = "▼"
            delta_class = "ion-delta-neg"
        else:
            arrow = "↔"
            delta_class = "ion-delta-neu"

        if delta_is_pp:
            delta_txt = _format_delta_br(delta, 2) + " p.p."
        elif delta_is_pct:
            delta_txt = _format_delta_br(delta, 2) + "%"
        else:
            delta_txt = _format_delta_br(delta, 2)

    badge_html = f'<div class="ion-badge">{badge}</div>' if badge else ""
    delta_html = (
        f"<div class='ion-delta {delta_class}'>{arrow} {delta_txt}</div>"
        if delta_txt
        else ""
    )
    icon_block = icon_html or ""

    html = f"""<div class="ion-card">
  {badge_html}
  {icon_block}
  <div class="ion-label">{label}</div>
  <div class="ion-value">{display_value}</div>
  {delta_html}
</div>
"""
    st.markdown(html, unsafe_allow_html=True)


# =============================================================================
# BLOCO PRINCIPAL
# =============================================================================

def render_bloco_curto_prazo_br() -> None:
    """
    Renderiza o bloco “Indicadores de Curto Prazo – Brasil”
    em estilo dashboard, usando dados de dados_curto_prazo_br.
    """
    _inject_ion_css_curto_prazo()

    dados = carregar_dados_curto_prazo_br()
    moeda = dados.moeda_juros
    ativos = dados.ativos_domesticos

    # -------------------- TÍTULO COM ÍCONE GENÉRICO --------------------
    header_html = f"""<div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;">
<div style="width:26px;height:26px;border-radius:999px;
border:1px solid rgba(177,211,53,0.6);
display:flex;align-items:center;justify-content:center;
background:rgba(10,26,29,0.95);">
  <svg viewBox="0 0 24 24" width="16" height="16"
       stroke="{ION_LIME}" stroke-width="2" fill="none">
    <circle cx="12" cy="6" r="3" />
    <path d="M12 9v9" />
    <path d="M7 20h10" />
  </svg>
</div>
<span style="font-size:1.4rem;font-weight:700;color:{ION_TEXT_PRIMARY};">
  Indicadores de Curto Prazo – Brasil
</span>
</div>"""

    st.markdown(header_html, unsafe_allow_html=True)
    st.markdown("### Mercado & Juros")

    # ======================= LINHA 1 ==========================
    col1, col2, col3 = st.columns(3)

    # Selic meta – Δ vs última decisão do Copom (em p.p.)
    with col1:
        selic_atual = getattr(moeda, "selic_meta", None)
        selic_ultima = getattr(moeda, "selic_ultima_decisao", None)
        selic_delta = None
        if selic_atual is not None and selic_ultima is not None:
            selic_delta = selic_atual - selic_ultima

        metric_card(
            "Selic meta",
            selic_atual,
            selic_delta,
            fmt_value="{:.2f}",
            value_is_pct=False,
            delta_is_pp=True,
            badge="vs Copom",
            icon_html=ICON_PERCENT,
        )

    # CDI do dia – Δ p.p. vs dia útil anterior (valor em %)
    with col2:
        cdi_dia = getattr(moeda, "cdi_dia", None)
        cdi_delta = getattr(moeda, "cdi_variacao_dia", None)
        metric_card(
            "CDI do dia",
            cdi_dia,
            cdi_delta,
            fmt_value="{:.5f}",
            value_is_pct=True,
            delta_is_pp=True,
            badge="vs D-1",
            icon_html=ICON_PERCENT,
        )

    # PTAX – dólar – Δ % intraday
    with col3:
        ptax = getattr(moeda, "ptax_fechamento", None)
        ptax_var_dia = getattr(moeda, "ptax_variacao_dia", None)
        metric_card(
            "PTAX – dólar (R$)",
            ptax,
            ptax_var_dia,
            fmt_value="R$ {:.2f}",
            value_is_pct=False,
            delta_is_pct=True,
            badge="intraday",
            icon_html=ICON_DOLLAR,
        )

    st.markdown("&nbsp;", unsafe_allow_html=True)

    # ======================= LINHA 2 ==========================
    col4, col5, col6 = st.columns(3)

    # Ibovespa – pts – Δ % intraday (com centavos)
    with col4:
        ibov_nivel = getattr(ativos, "ibov_nivel", None)
        ibov_var_dia = getattr(ativos, "ibov_var_dia", None)
        metric_card(
            "Ibovespa – pts",
            ibov_nivel,
            ibov_var_dia,
            fmt_value="{:,.2f}",
            value_is_pct=False,
            delta_is_pct=True,
            badge="intraday",
            icon_html=ICON_CHART,
        )

    # DI Futuro ~2 anos – Δ p.p. intraday
    with col5:
        di2_taxa = getattr(ativos, "di_2_anos_taxa", None)
        di2_delta = getattr(ativos, "di_2_anos_delta", None)
        metric_card(
            "DI Futuro ~2 anos (B3)",
            di2_taxa,
            di2_delta,
            fmt_value="{:.2f}",
            value_is_pct=False,
            delta_is_pp=True,
            badge="intraday",
            icon_html=ICON_CHART,
        )

    # DI Futuro ~5 anos – Δ p.p. intraday
    with col6:
        di5_taxa = getattr(ativos, "di_5_anos_taxa", None)
        di5_delta = getattr(ativos, "di_5_anos_delta", None)
        metric_card(
            "DI Futuro ~5 anos (B3)",
            di5_taxa,
            di5_delta,
            fmt_value="{:.2f}",
            value_is_pct=False,
            delta_is_pp=True,
            badge="intraday",
            icon_html=ICON_CHART,
        )


def render_bloco_curto_prazo() -> None:
    """Alias para compatibilidade com chamadas antigas."""
    render_bloco_curto_prazo_br()
