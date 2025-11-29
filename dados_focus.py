# dados_focus.py
# Atualiza os arquivos de cache do Boletim Focus
# (anuais e Top5) em data/expectativas/.

from indicadores_macro_br import (
    _carregar_focus_raw,
    _carregar_focus_top5_raw,
    FOCUS_CACHE_FILE,
    FOCUS_TOP5_CACHE_FILE,
)


def atualizar_cache_focus() -> None:
    print("Atualizando cache do Focus (anuais e Top5)...")

    # Isso baixa da API SÓ quando o CSV ainda não existe
    # ou está ruim, e já salva em data/expectativas/.
    df_anuais = _carregar_focus_raw()
    print(f" - Anuais salvos em: {FOCUS_CACHE_FILE} ({len(df_anuais)} linhas)")

    df_top5 = _carregar_focus_top5_raw()
    print(f" - Top5 salvos em: {FOCUS_TOP5_CACHE_FILE} ({len(df_top5)} linhas)")

    print("Pronto: cache do Focus atualizado em data/expectativas.")


if __name__ == "__main__":
    atualizar_cache_focus()
