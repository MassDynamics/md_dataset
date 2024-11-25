FROM amazonlinux:2023.6.20241010.0

RUN yum -y update

# INSTALL PYTHON

RUN yum install -y wget tar gzip gcc
RUN dnf install -y dnf-plugins-core
RUN dnf builddep -y python3

ARG PYTHON_VERSION=3.11.10
RUN cd /opt/ && wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
RUN cd /opt/ && tar xzf Python-${PYTHON_VERSION}.tgz
RUN cd /opt/Python-${PYTHON_VERSION} && \
      ./configure --enable-optimizations --with-pydebug && \
      make -j $(nproc) && \
      make altinstall

RUN ln -s /opt/Python-${PYTHON_VERSION}/python /usr/bin/python

RUN cd /opt/ && wget https://bootstrap.pypa.io/get-pip.py && \
      python get-pip.py

# INSTALL R


ENV R_VERSION=4.3.0

RUN yum -y update && \
        yum -y groupinstall "Development Tools" && \
        yum -y install \
        gcc \
        gcc-c++ \
        gfortran \
        make \
        zlib-devel \
        bzip2 \
        bzip2-devel \
        xz-devel \
        libcurl-devel \
        libpng-devel \
        libjpeg-devel \
        cairo-devel \
        pango-devel \
        libX11-devel \
        libXt-devel \
        readline-devel \
        openssl-devel \
        texinfo \
        texlive \
        wget \
        tar \
        xz && \
    yum clean all

RUN cd /usr/local/src && \
    wget https://cran.r-project.org/src/base/R-4/R-${R_VERSION}.tar.gz && \
    tar -xzf R-${R_VERSION}.tar.gz && \
    cd R-${R_VERSION} && \
    ./configure --enable-R-shlib --with-blas --with-lapack && \
    make && \
    make install && \
    cd .. && \
    rm -rf R-${R_VERSION} R-${R_VERSION}.tar.gz

RUN R --version

# INSTALL PACKAGE
ENV WORK_DIR="/usr/src/app"
WORKDIR $WORK_DIR

RUN pip install "cython<3.0.0" wheel
RUN pip install "pyyaml==5.4.1" --no-build-isolation

# ARG GIT_HASH
# RUN pip install git+https://github.com/MassDynamics/md_dataset.git@${GIT_HASH}

RUN yum install -y \
    blas blas-devel \
    lapack lapack-devel \
    gcc-gfortran \
    libquadmath libquadmath-devel \
    libicu libicu-devel \
    lzma \
    zlib zlib-devel \
    bzip2 bzip2-devel \
    readline readline-devel \
    ncurses ncurses-devel \
    libxcb libXau libXrender \
    && yum clean all

ENV LD_LIBRARY_PATH=/usr/local/lib64/R/lib:/usr/lib64:/usr/local/lib64:$LD_LIBRARY_PATH

COPY . .
python -m pip install -e '.[r]'

ENV PYTHON_EXECUTABLE="/opt/Python-${PYTHON_VERSION}/python"
