## scripts with uv

This folder uses uv as the source of truth for dependencies.

## First-time setup

Run from this folder:

uv sync

This creates or updates the virtual environment from pyproject.toml and uv.lock.

## Running scripts

Run any script through uv from this folder:

uv run ss_retry_unpacking.py <INGEST_ID>
uv run ss_get_ingest.py <INGEST_ID>
uv run ss_get_bag.py <BAG_ID>
uv run ss_download_bag.py <BAG_ID>

If a script expects credentials or environment variables, set those in your shell before running.

## Running tests

Install test tools:

uv sync --group test

Run tests:

uv run --group test pytest -q

## Notes

wellcome_storage_service is a Python library, not a standalone CLI command.
Use it through scripts that import it.
