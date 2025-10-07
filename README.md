| | |
| :-- | :--  |
| code repository              | [![github repo badge](https://img.shields.io/badge/github-repo-000.svg?logo=github&labelColor=gray&color=blue)](https://github.com/MassDynamics/md_dataset) |
| license                      | [![github license badge](https://img.shields.io/github/license/MassDynamics/md_dataset)](https://github.com/MassDynamics/md_dataset) |

Common package for integrating with Mass Dynamics workflows

## Installation

To install md_dataset from GitHub repository, do:

```sh
git clone git@github.com:MassDynamics/md_dataset.git
cd md_dataset
python -m pip install .
```

To develop the package:

```sh
python -m pip install -e '.[dev]'
ruff check
pytest
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
