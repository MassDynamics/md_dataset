ARG BASE_TAG=latest
ARG BASE_IMAGE=massdynamics/md_dataset_package_base:${BASE_TAG}
FROM ${BASE_IMAGE} AS build

RUN pip install --no-cache-dir --upgrade pip setuptools wheel build

COPY pyproject.toml .
COPY MANIFEST.in .
COPY src/ src/

RUN python -m build

ARG BASE_TAG=latest
ARG BASE_IMAGE=massdynamics/md_dataset_package_base:${BASE_TAG}
FROM ${BASE_IMAGE}

# setuptools >= 78.1.1 fixes CVE-2022-40897, CVE-2024-6345, CVE-2025-47273.
# The base image already pins it, but re-assert here.
RUN pip install --no-cache-dir --upgrade pip "setuptools>=78.1.1"
COPY --from=build /usr/src/app/dist/md_dataset-*-py3-none-any.whl /tmp/
RUN pip install --no-cache-dir /tmp/md_dataset-*-py3-none-any.whl
