FROM amazonlinux:2023.12.20260817.0

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

# Re-apply OS security updates from a newer AL2023 repo snapshot than the pinned
# base tag ships. 2023.12.20260914 has no Docker Hub tag yet but carries the
# HIGH fixes flagged by the trivy image scan: openssl 3.5.8-1.amzn2023.0.1
# (CVE-2026-63072 et al.), expat 2.8.3-1.amzn2023.0.1 (CVE-2026-56404 et al.),
# bluez-libs 5.62-2.amzn2023.0.6 (CVE-2026-80186) and emacs-filesystem
# 28.2-3.amzn2023.0.11 (CVE-2026-77219, CVE-2026-79992).
RUN yum -y --releasever=2023.12.20260914 update && yum clean all

ENV PYTHON_EXECUTABLE="/opt/Python-${PYTHON_VERSION}/python"
