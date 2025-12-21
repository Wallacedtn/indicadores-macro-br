# Copilot Instructions for Tesouro Macro Dashboard

## Project Overview
This is a Streamlit-based dashboard for Brazilian macroeconomic indicators. The app aggregates data from multiple sources (BCB, IBGE, ANBIMA, etc.) into a unified interface.

## Architecture
- **Main App**: `indicadores_macro_br.py` - Streamlit app that imports and displays data from modular fetchers
- **Data Modules**: Each `.py` file (e.g., `tesouro_direto.py`, `ipca_ibge.py`) handles one data source
- **Data Storage**: CSVs in `data/` subdirectories, updated via `atualiza_dados_pesados.py`
- **Workflow**: Update CSVs daily with the heavy script, then run Streamlit for the dashboard

## Key Patterns
- **Caching Strategy**: Functions like `carregar_tesouro_bruto()` try local CSV first, fallback to API download and save
- **Update Functions**: Each module has `atualizar_*()` to refresh CSVs (e.g., `atualizar_todas_as_curvas()` in `curvas_anbima.py`)
- **Data Paths**: Use `Path(__file__).resolve().parent / "data" / ...` for relative paths
- **Brazilian Formats**: Dates in DD/MM/YYYY, numbers with comma decimals (use `locale` or custom parsing)
- **Error Handling**: Wrap API calls in try/except, log errors but continue (see `atualiza_dados_pesados.py`)

## Development Workflow
- **Update Data**: Run `python atualiza_dados_pesados.py` to refresh all CSVs
- **Run App**: `streamlit run indicadores_macro_br.py`
- **Add New Indicator**: Create new `.py` module with `carregar_*()` and `atualizar_*()` functions, import in main app
- **Dependencies**: Install from `requirements.txt` (includes `streamlit`, `pandas`, `requests`, etc.)

## Conventions
- **Imports**: Group standard libs, then third-party, then local modules
- **Functions**: Use `@lru_cache` for expensive operations
- **Naming**: Portuguese for domain terms (e.g., `carregar_dados_macro_fiscal_br`), English for code
- **CSV Structure**: Date columns as strings, numeric columns cleaned of non-numeric chars
- **Logging**: Use `logging` with levels, default WARNING

## Examples
- **New Data Fetcher**: Follow `tesouro_direto.py` - define paths, implement cached loader and updater
- **API Integration**: Use `requests.get()` with timeout, handle encoding (often latin-1 for Brazilian sites)
- **Data Processing**: Use pandas for cleaning, convert dates with `pd.to_datetime(df['date'], format='%d/%m/%Y')`

## Key Files
- `indicadores_macro_br.py`: Main dashboard logic
- `atualiza_dados_pesados.py`: Data update orchestrator
- `data/`: CSV storage organized by category (curvas_tesouro/, precos/, etc.)
- `requirements.txt`: All dependencies pinned</content>
<parameter name="filePath">.github/copilot-instructions.md