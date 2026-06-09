ARG BASE_TAG=latest
ARG BASE_IMAGE=massdynamics/md_dataset_package_base:${BASE_TAG}
FROM ${BASE_IMAGE} AS build

RUN yum -y update

# INSTALL R

ENV R_VERSION=4.5.0

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
        xz \
        which && \
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

# Re-apply OS security updates from a newer AL2023 repo snapshot than the pinned
# base tag ships. AL2023 uses deterministic, version-locked repos, so we pin an
# explicit, recent releasever for reproducible, reviewable builds — bump it
# deliberately (keep it in step with base.Dockerfile) to pick up newer distro
# fixes. The expensive R build above stays cached; changing the releasever re-runs
# this layer.
RUN yum -y --releasever=2023.12.20260608 update && yum clean all

ENV LD_LIBRARY_PATH=/usr/local/lib64
ENV LD_LIBRARY_PATH=/usr/local/lib64/R/lib:/usr/lib64:/usr/local/lib64:$LD_LIBRARY_PATH
