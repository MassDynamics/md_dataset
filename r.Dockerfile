FROM 243488295326.dkr.ecr.ap-southeast-2.amazonaws.com/md_dataset_package:0.2.0-28

RUN yum -y update

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
