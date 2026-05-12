# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-dhs** collects data from the [DHS API](http://api.dhsprogram.com/#/api-data.cfm) and creates datasets per country in HDX (national and subnational). The scraper runs monthly and takes around 10 hours, making ~200 reads from DHS and ~1000 read/writes to HDX.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.dhs
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_dhs.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline flows through stages in `__main__.py`:

1. **`get_countries`** — Calls the DHS API to retrieve the list of countries.
2. **`get_tags`** — Fetches available tags/indicators for a given country.
3. **`generate_datasets_and_showcase`** — Constructs national and subnational HDX `Dataset` objects for a given country. Returns `dataset`, `subdataset`, `showcase`.

### Key design points

- **Two datasets per country**: the scraper creates/updates a national and a subnational dataset for each country.
- **Retry logic**: uses `tenacity` to retry on `DownloadError`, `HDXError`, and `ParserError` with up to 5 attempts and a 10-minute wait between retries.
- **`Retrieve`** (`hdx-python-utilities`) abstracts HTTP downloads and supports save/replay via `save=True`/`use_saved=True` — used in tests to replay fixture data from `tests/fixtures/input/`.
- **Static config inside the package**: `config/` lives under `src/hdx/scraper/dhs/config/` so it is installed with the package and located via `script_dir_plus_file`.

### Config files

- `src/hdx/scraper/dhs/config/project_configuration.yaml` — API URL and dataset description template
- `src/hdx/scraper/dhs/config/hdx_dataset_static.yaml` — Static HDX metadata applied to every dataset (license, methodology, source, etc.)

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-dhs` entry.

Optionally set `APIKEY` env var for the DHS API key, or supply it via `~/.extraparams.yaml`.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
