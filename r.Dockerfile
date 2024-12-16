ARG BASE_TAG
FROM 243488295326.dkr.ecr.ap-southeast-2.amazonaws.com/md_dataset_package_base:$BASE_TAG

COPY . .
RUN python -m pip install -e '.[r]'
