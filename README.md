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

First copy the templates to the ignored `config/` directory:

```bash
mkdir -p config
cp config_templates/*.json config/
```

Then edit files in `config/` and replace placeholder values such as Garmin
email, local folders, JSON credentials, and KeePass database path. Run the CLI
with the local `config/` files, not the templates.

Run with KeePass credentials:

> Before running: in `config/config.keepass.json` set `credential_login` to your Garmin email and `folder` to your local trainings directory. Replace the `--creds-keepass` path with the path to your KeePass database.

```bash
poetry install
poetry run trainings-sync \
  --config config/config.keepass.json \
  --creds-keepass /path/to/keepass.kdbx
```

Run with a local JSON credentials file:

```bash
poetry install
poetry run trainings-sync \
  --config config/config.local-json.json \
  --creds-json config/creds.json
```

Run with Strava as a fallback source (activities missing from Garmin are pulled from Strava):

#### Getting Strava credentials

1. Register your app at [strava.com/settings/api](https://www.strava.com/settings/api) (free, no approval needed). Note your **Client ID** and **Client Secret**.

2. Open the following URL in a browser (replace `YOUR_CLIENT_ID`):
   ```
   https://www.strava.com/oauth/authorize?client_id=YOUR_CLIENT_ID&redirect_uri=http://localhost&response_type=code&scope=activity:read_all
   ```
   Click **Authorize**. The browser redirects to `http://localhost/?...&code=XXXX` — copy the `code` value from the URL.

3. Exchange the code for a refresh token (replace placeholders):
   ```bash
   curl -X POST https://www.strava.com/oauth/token \
     -d client_id=YOUR_CLIENT_ID \
     -d client_secret=YOUR_CLIENT_SECRET \
     -d code=YOUR_CODE \
     -d grant_type=authorization_code
   ```
   Copy `refresh_token` from the response.

4. In `config/config.strava-source.json` set `client_id` to your Client ID.
   In `config/creds.strava-source.json` fill in:
   - Garmin `password` — your Garmin password
   - Strava `login` — your Client Secret
   - Strava `password` — the refresh token from step 3

```bash
poetry install
poetry run trainings-sync \
  --config config/config.strava-source.json \
  --creds-json config/creds.strava-source.json
```

Run with Strava as source and Garmin as destination (activities from Strava are uploaded to Garmin Connect):

> Uses the same credentials file as the Strava fallback source scenario. See [Getting Strava credentials](#getting-strava-credentials) above.

1. In `config/config.strava-to-garmin.json` set `client_id` to your Strava Client ID and `credential_login` to your Garmin email.
   In `config/creds.strava-source.json` fill in Garmin and Strava credentials as described above.

```bash
poetry install
poetry run trainings-sync \
  --config config/config.strava-to-garmin.json \
  --creds-json config/creds.strava-source.json
```

| Option                 | Description                                                                                                                                                               |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--config PATH`        | Path to the JSON config file. Required.                                                                                                                                   |
| `--creds-json PATH`    | JSON credentials file. Required for Garmin and Strava connectors.                                                                                                         |
| `--creds-keepass PATH` | KeePass database (.kdbx) instead of a JSON file. Master password is read from `KEEPASS_PASSWORD` env var, or prompted from stdin. Not supported with Strava sources or destinations. |
| `--start DATE`         | Start date (YYYY-MM-DD). Overrides the value in config. Defaults to `2000-01-01` if not set anywhere.                                                                     |
| `--end DATE`           | End date (YYYY-MM-DD). Overrides the value in config. Defaults to today.                                                                                                  |
| `--force`              | Re-download activities even if already cached.                                                                                                                            |

## Contributing

Before requesting a review, make sure the CI pipeline passes on your pull request. Once the pipeline is green, request a review from [@dchernykh1984](https://github.com/dchernykh1984).
