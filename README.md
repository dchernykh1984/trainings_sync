# trainings-sync

> [!WARNING]
> If your Strava sync appears frozen or stalled, you have most likely hit Strava API rate limits.
> Check the sync log file for rate limit messages and review your current usage on the [Strava API settings page](https://www.strava.com/settings/api).
> See the [Strava rate limits documentation](https://developers.strava.com/docs/rate-limits/) for details.

CLI tool for syncing training activities between Garmin Connect and a local folder (FIT/GPX/TCX). Strava upload support is also available.

## Setup

### 1. Download the project

Install Git if you don't have it:

- **macOS:** `brew install git`
- **Linux (Ubuntu / Debian):** `sudo apt install git`
- **Windows:** download from [git-scm.com](https://git-scm.com/downloads) and run the installer

Then clone the repository:

```bash
git clone https://github.com/dchernykh1984/trainings_sync.git
cd trainings_sync
```

All subsequent commands should be run from the `trainings_sync` folder.

### 2. Install Python 3.14

This project requires **Python 3.14**; `uv` installs a matching interpreter automatically, but you can also install it yourself as shown below.

**macOS**

```bash
brew install python@3.14
```

If you don't have Homebrew yet, install it first from [brew.sh](https://brew.sh).

**Linux (Ubuntu / Debian)**

The system `python3` package is usually not 3.14. Install it via the deadsnakes PPA:

```bash
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.14 python3.14-venv
```

**Windows**

Download the **Python 3.14** installer from [python.org/downloads](https://www.python.org/downloads/) and run it. On the first screen, check **"Add Python to PATH"** before clicking Install.

Verify the installation in a terminal:

- **macOS / Linux:** `python3.14 --version`
- **Windows:** `py -3.14 --version`

The output should start with `Python 3.14`.

### 3. Install uv

**macOS / Linux**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows**

Open **PowerShell** and run:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Restart your terminal afterwards so `uv` is on your `PATH`.

### 4. Create virtual environment and install dependencies

```bash
uv sync
```

### 5. Set up pre-commit hooks

```bash
uv run pre-commit install
uv run pre-commit install --hook-type commit-msg
```

After that pre commit hooks will run automatically on every commit.

To run all checks manually across all files:

```bash
uv run pre-commit run --all-files
```

## Usage

Example config and credentials files are in [config_templates/](config_templates/).
Two templates are provided:

- **`config.garmin-to-local.json`** - sync from Garmin Connect to a local folder (simple, no Strava).
- **`config.strava-and-garmin.json`** - full setup with two sync groups running in a single pass:
  1. Strava -> Garmin Connect (upload Strava activities to Garmin)
  2. Garmin + Strava -> local folder (save to disk, Garmin has priority)

First copy the templates to the ignored `config/` directory:

```bash
mkdir -p config
cp config_templates/*.json config/
```

Then edit files in `config/` and replace placeholder values such as Garmin
email, local folders, and Strava `client_id`. Run the CLI
with the local `config/` files, not the templates.

### Garmin -> local folder (simple)

Edit `config/config.garmin-to-local.json`: set `credential_login` to your Garmin email and `folder` to your local trainings directory.

Run with a JSON credentials file:

```bash
uv run trainings-sync \
  --config config/config.garmin-to-local.json \
  --creds-json config/creds.json
```

Run with KeePass credentials (replace the path with your `.kdbx` file):

```bash
uv run trainings-sync \
  --config config/config.garmin-to-local.json \
  --creds-keepass /path/to/keepass.kdbx
```

### Strava + Garmin (full setup)

#### Getting Strava credentials

1. Register your app at [strava.com/settings/api](https://www.strava.com/settings/api) (free, no approval needed). Note your **Client ID** and **Client Secret**.

2. Open the following URL in a browser (replace `YOUR_CLIENT_ID`):
   ```
   https://www.strava.com/oauth/authorize?client_id=YOUR_CLIENT_ID&redirect_uri=http://localhost&response_type=code&scope=activity:read_all
   ```
   Click **Authorize**. The browser redirects to `http://localhost/?...&code=XXXX` - copy the `code` value from the URL.

3. Exchange the code for a refresh token (replace placeholders):
   ```bash
   curl -X POST https://www.strava.com/oauth/token \
     -d client_id=YOUR_CLIENT_ID \
     -d client_secret=YOUR_CLIENT_SECRET \
     -d code=YOUR_CODE \
     -d grant_type=authorization_code
   ```
   Copy `refresh_token` from the response.

4. In `config/config.strava-and-garmin.json` set `client_id` to your Strava Client ID,
   `credential_login` to your Garmin email, and `folder` to your local trainings directory.
   In `config/creds.strava-source.json` fill in:
   - Garmin `password` - your Garmin password
   - Strava `login` - your Client Secret
   - Strava `password` - the refresh token from step 3

```bash
uv run trainings-sync \
  --config config/config.strava-and-garmin.json \
  --creds-json config/creds.strava-source.json
```

| Option                 | Description                                                                                                                                                               |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--config PATH`        | Path to the JSON config file. Required.                                                                                                                                   |
| `--creds-json PATH`    | JSON credentials file. Required for Garmin and Strava connectors.                                                                                                         |
| `--creds-keepass PATH` | KeePass database (.kdbx) instead of a JSON file. Master password is read from `KEEPASS_PASSWORD` env var, or prompted from stdin. Not supported with Strava sources or destinations. |
| `--start DATE`         | Start date (YYYY-MM-DD). Overrides the value in config. Defaults to `2000-01-01` if not set anywhere.                                                                     |
| `--end DATE`           | End date (YYYY-MM-DD). Overrides the value in config. Defaults to today.                                                                                                  |
| `--force`              | Re-download activities even if already cached. Also re-downloads all wellness data.                                                                                       |
| `--skip-wellness`      | Skip wellness data sync (only sync training activities).                                                                                                                  |

If a sync run is interrupted, simply run the same command again. Already downloaded activities are stored in the cache (`cache_dir` in config) and will not be re-downloaded.

**Note on Strava rate limits:** Strava enforces API rate limits per registered app. For read-heavy
syncs the binding constraint is typically the read limit (100 requests per 15 minutes, 1000 per day),
but the exact values can change -- always check the sync log, which prints current usage after each
request:

```
[strava] rate limits: 15min=190/200, daily=1850/2000, read_15min=90/100, read_daily=900/1000
```

When a limit is reached, the sync automatically pauses and resumes after the window resets. Your app's
current limits are shown at [strava.com/settings/api](https://www.strava.com/settings/api).

## Desktop GUI

If you would rather not edit JSON config files by hand, a desktop GUI is
available. It exposes the same functionality as the CLI - credentials, sync
configuration, and running a sync with live progress - through a windowed
interface.

### Launching

```bash
uv run trainings-sync-gui
```

On Linux the GUI needs a few Qt system libraries. On a headless or minimal
install run:

```bash
sudo apt-get install -y libgl1 libegl1 libglib2.0-0 libdbus-1-3 \
  libfontconfig1 libxkbcommon0
```

Unlike the CLI, the GUI does not take `--config` / `--creds-json` arguments.
It stores everything in a fixed location under your home directory:

```
~/.config/trainings-sync/
  config.json        # connectors, sync groups, options
  credentials.json   # service credentials (plain JSON, same format as the CLI)
  cache/             # activity + wellness cache and sync.log
```

The window has three tabs, ordered by how often you use them: **Sync**
(the default landing tab), **Configuration**, and **Credentials**. If you
already have CLI config/creds files (for example under `config/`), you can
import them instead of re-entering everything - see the *Load from file...*
buttons below. The walkthrough that follows is in first-time setup order.

### Credentials tab

Manage the service logins used by the connectors. Use **Add** / **Edit** /
**Delete** to maintain the list. Each entry has a *Service*, *URL*, *Login*,
and *Password / Token*. Passwords are hidden while typing and masked in the
table.

- **Garmin:** *Login* is your Garmin email, *Password* is your Garmin password.
- **Strava:** *Login* is your Client Secret, *Password / Token* is the refresh
  token (see [Getting Strava credentials](#getting-strava-credentials) above).

**Load from file...** imports an existing CLI-style credentials JSON file (the
same format as `--creds-json`), replacing the current list.

### Configuration tab

Build the sync configuration visually:

- **Connectors** - add each data source/destination. Pick a type (`garmin`,
  `strava`, or `local_folder`); the dialog shows only the fields that type
  needs. For Garmin/Strava the *Service* and *URL* must match a credentials
  entry; for a local folder just provide the path. Deleting a connector also
  removes it from any sync group that referenced it.
- **Sync Groups** - define what syncs where. Add sources (each with a priority;
  higher priority wins when the same activity exists in several sources) and
  destinations, both chosen from the connectors you defined.
- **Options** - optional custom start/end dates, *Force re-download* (ignore the
  cache), and *Skip wellness sync*. Click **Save configuration** to persist.

**Load from file...** imports an existing config JSON file - both the GUI's own
`config.json` and a CLI config file work (the CLI-only `cache_dir` field is
ignored). It replaces the whole configuration.

### Sync tab

Click **Run Sync** to start. A progress bar appears for every task, mirroring
the console output. If the run fails, the error is shown in a dialog; use
**Show full log** to open the full `sync.log` for the traceback and details.
The sync runs in the background, so the window stays responsive.

## Wellness data sync

In addition to training activities, the tool automatically syncs wellness data from each configured service to local folder destinations. This happens on every run by default (use `--skip-wellness` to disable).

### What is synced

**From Garmin Connect** (stored under `{folder}/wellness/`):

| Category | Data types |
|---|---|
| Sleep & recovery | `sleep`, `hrv`, `body_battery`, `body_battery_events`, `training_readiness`, `morning_training_readiness` |
| Heart & blood | `heart_rates`, `resting_hr`, `spo2`, `blood_pressure` |
| Stress | `stress_daily`, `all_day_stress`, `weekly_stress` |
| Body composition | `body_composition`, `weigh_ins`, `daily_weigh_ins` |
| Activity | `steps_daily`, `daily_steps_range`, `weekly_steps`, `floors`, `intensity_minutes`, `weekly_intensity_minutes`, `hydration`, `lifestyle_logging` |
| Performance | `vo2max`, `lactate_threshold`, `training_status`, `endurance_score`, `running_tolerance`, `race_predictions`, `fitness_age`, `hill_score` |
| Miscellaneous | `personal_records`, `user_summary`, `stats` |

**From Strava** (snapshot data): `athlete_stats`, `athlete_zones`

### Storage layout

Each data type is stored as JSON files under:
```
{local_folder}/wellness/{data_type}/{YYYY-MM-DD}.json   # daily data
{local_folder}/wellness/{data_type}/{start}_{end}.json  # range data
{local_folder}/wellness/{data_type}/snapshot.json       # snapshot data
```

Wellness data is also cached in `{cache_dir}/wellness/` using the same layout. Re-running the tool only fetches dates not yet in the cache. Use `--force` to invalidate the cache and re-download everything.

### Writable data types

For Garmin, the following types support both reading and writing: `body_composition`, `weigh_ins`, `daily_weigh_ins`, `blood_pressure`, `hydration`. Writing back to Garmin from a local folder is not yet automated but the connectors implement the full interface.

## Contributing

Before requesting a review, make sure the CI pipeline passes on your pull request. Once the pipeline is green, request a review from [@dchernykh1984](https://github.com/dchernykh1984).
