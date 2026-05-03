# trainings-sync

CLI tool for syncing training activities between Garmin Connect and a local folder (FIT/GPX/TCX). Strava upload support is also available.

## Setup

### 1. Install Poetry

**macOS**

```bash
brew install pipx
pipx ensurepath
pipx install poetry
```

**Linux**

> TODO: add installation instructions for Linux

**Windows**

> TODO: add installation instructions for Windows

### 2. Create virtual environment and install dependencies

```bash
poetry config virtualenvs.in-project true
poetry install --no-root
```

### 3. Set up pre-commit hooks

```bash
poetry run pre-commit install
poetry run pre-commit install --hook-type commit-msg
```

After that pre commit hooks will run automatically on every commit.

To run all checks manually across all files:

```bash
poetry run pre-commit run --all-files
```

## Usage

Example config and credentials files are in [config_templates/](config_templates/).

```bash
poetry install
trainings-sync --config config_templates/config.json --creds-json config_templates/creds.json
```

| Option | Description |
|---|---|
| `--config PATH` | Path to the JSON config file. Required. |
| `--creds-json PATH` | JSON credentials file. Required for Garmin and Strava connectors. |
| `--creds-keepass PATH` | KeePass database (.kdbx) instead of a JSON file. Master password is read from `KEEPASS_PASSWORD` env var, or prompted from stdin. Not supported with Strava destinations. |
| `--start DATE` | Start date (YYYY-MM-DD). Overrides the value in config. Defaults to `2000-01-01` if not set anywhere. |
| `--end DATE` | End date (YYYY-MM-DD). Overrides the value in config. Defaults to today. |
| `--force` | Re-download activities even if already cached. |

## Contributing

Before requesting a review, make sure the CI pipeline passes on your pull request. Once the pipeline is green, request a review from [@dchernykh1984](https://github.com/dchernykh1984).
