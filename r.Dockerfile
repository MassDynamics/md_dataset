ARG BASE_TAG
FROM 243488295326.dkr.ecr.ap-southeast-2.amazonaws.com/md_dataset_package_base:$BASE_TAG AS build

RUN pip install --no-cache-dir --upgrade pip setuptools wheel build
COPY . .
RUN python -m build

FROM 243488295326.dkr.ecr.ap-southeast-2.amazonaws.com/md_dataset_package_base:$BASE_TAG

RUN pip install --no-cache-dir --upgrade pip
COPY --from=build /usr/src/app/dist/md_dataset-*-py3-none-any.whl /tmp/
RUN pip install --no-cache-dir /tmp/md_dataset-*-py3-none-any.whl[r]
