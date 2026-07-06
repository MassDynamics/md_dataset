FROM amazonlinux:2023.12.20260611.0

RUN yum -y update

# INSTALL PYTHON

RUN yum install -y wget tar gzip gcc
RUN dnf install -y dnf-plugins-core
RUN dnf builddep -y python3

ARG PYTHON_VERSION=3.14.6
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
# Cython >= 3.1 is required to compile extensions on Python 3.14; the old
# "cython<3.0.0" pin cannot build against the 3.14 C-API.
RUN pip install "cython>=3.1" wheel "setuptools>=78.1.1"
# PyYAML 5.4.1 cannot build on Python 3.12+ (its pinned Cython chokes on the new
# C-API); 6.0.2 ships cp314 wheels.
RUN pip install "pyyaml>=6.0.2"

# Re-apply OS security updates from a newer AL2023 repo snapshot.
RUN yum -y --releasever=2023.12.20260629 update && yum clean all

ENV PYTHON_EXECUTABLE="/opt/Python-${PYTHON_VERSION}/python"
