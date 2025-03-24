ARG BASE_TAG=latest
ARG BASE_IMAGE=243488295326.dkr.ecr.ap-southeast-2.amazonaws.com/md_dataset_package_base:${BASE_TAG}
FROM ${BASE_IMAGE} AS build

RUN pip install --no-cache-dir --upgrade pip setuptools wheel build

COPY pyproject.toml .
COPY MANIFEST.in .
COPY src/ src/

RUN python -m build

ARG BASE_TAG=latest
ARG BASE_IMAGE=243488295326.dkr.ecr.ap-southeast-2.amazonaws.com/md_dataset_package_base:${BASE_TAG}
FROM ${BASE_IMAGE}

RUN pip install --no-cache-dir --upgrade pip
COPY --from=build /usr/src/app/dist/md_dataset-*-py3-none-any.whl /tmp/
RUN pip install --no-cache-dir /tmp/md_dataset-*-py3-none-any.whl
