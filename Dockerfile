FROM amazonlinux:2023.6.20241010.0

RUN yum -y update

RUN yum install -y wget tar gzip gcc
RUN dnf install -y dnf-plugins-core
RUN dnf builddep -y python3

ARG PYTHON_VERSION=3.12.7
RUN cd /opt/ && wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
RUN cd /opt/ && tar xzf Python-${PYTHON_VERSION}.tgz
RUN cd /opt/Python-${PYTHON_VERSION} && \
      ./configure --enable-optimizations --with-pydebug && \
      make -j $(nproc) && \
      make altinstall

RUN ln -s /opt/Python-${PYTHON_VERSION}/python /usr/bin/python

RUN cd /opt/ && wget https://bootstrap.pypa.io/get-pip.py && \
      python get-pip.py

# ENV
ENV WORK_DIR="/usr/src/app"
WORKDIR $WORK_DIR

ARG GIT_HASH
RUN pip install git+https://github.com/MassDynamics/md_dataset.git@${GIT_HASH}

ENV PYTHON_EXECUTABLE="/opt/Python-${PYTHON_VERSION}/python"
