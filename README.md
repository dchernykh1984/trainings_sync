# trainings_sync

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
```

After that pre commit hooks will run automatically on every commit.

To run all checks manually across all files:

```bash
poetry run pre-commit run --all-files
```

## Contributing

Before requesting a review, make sure the CI pipeline passes on your pull request. Once the pipeline is green, request a review from [@dchernykh1984](https://github.com/dchernykh1984).
