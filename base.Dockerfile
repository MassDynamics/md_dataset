FROM amazonlinux:2023.11.20260526.0

RUN yum -y update

# INSTALL PYTHON

RUN yum install -y wget tar gzip gcc
RUN dnf install -y dnf-plugins-core
RUN dnf builddep -y python3

ARG PYTHON_VERSION=3.11.10
RUN cd /opt/ && wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
RUN cd /opt/ && tar xzf Python-${PYTHON_VERSION}.tgz
RUN cd /opt/Python-${PYTHON_VERSION} && \
      ./configure --enable-optimizations && \
      make -j $(nproc) && \
      make altinstall

RUN ln -s /opt/Python-${PYTHON_VERSION}/python /usr/bin/python

RUN cd /opt/ && wget https://bootstrap.pypa.io/get-pip.py && \
      python get-pip.py

ENV WORK_DIR="/usr/src/app"
WORKDIR $WORK_DIR

# setuptools >= 78.1.1 fixes CVE-2022-40897, CVE-2024-6345, CVE-2025-47273.
# get-pip bootstraps an older one into the runtime site-packages, so pin it here
# in the base image; every downstream image inherits the patched version.
RUN pip install "cython<3.0.0" wheel "setuptools>=78.1.1"
RUN pip install "pyyaml==5.4.1" --no-build-isolation

ENV PYTHON_EXECUTABLE="/opt/Python-${PYTHON_VERSION}/python"
