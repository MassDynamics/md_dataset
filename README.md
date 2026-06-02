| | |
| :-- | :--  |
| code repository              | [![github repo badge](https://img.shields.io/badge/github-repo-000.svg?logo=github&labelColor=gray&color=blue)](https://github.com/MassDynamics/md_dataset) |
| license                      | [![github license badge](https://img.shields.io/github/license/MassDynamics/md_dataset)](https://github.com/MassDynamics/md_dataset) |

Common package for integrating with Mass Dynamics workflows

## Installation

This project uses [uv](https://docs.astral.sh/uv/) to manage its environment and
dependencies. Install it first if you don't have it:

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

To install md_dataset from GitHub repository, do:

```sh
git clone git@github.com:MassDynamics/md_dataset.git
cd md_dataset
uv sync
```

To develop the package:

```sh
uv sync --extra dev
uv run ruff check
uv run pytest
```

The CI helpers wrap these commands:

```sh
./scripts/test   # uv run pytest -n auto
./scripts/lint   # uv run ruff check
./scripts/audit  # uv run pip-audit
```

## Documentation

[R implementation](https://github.com/MassDynamics/MDCustomR)

To run a dataset locally, you can using localstack:

```sh
USE_LOCALSTACK=true
```

To use an AWS source buckets:

```sh
RESULTS_BUCKET=bucket-name
BOTO3_PROFILE=your-profile
```

These will both use a local prefect installation.

## Credits

This initial package was created with [Copier](https://github.com/copier-org/copier) and the [NLeSC/python-template](https://github.com/NLeSC/python-template).
