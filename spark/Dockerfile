FROM apache/spark:3.5.6

USER root

RUN apt-get update && apt-get install -y \
        wget build-essential libssl-dev zlib1g-dev \
        libncurses5-dev libncursesw5-dev libreadline-dev \
        libsqlite3-dev libffi-dev libbz2-dev liblzma-dev \
        xz-utils tk-dev \
    && wget https://www.python.org/ftp/python/3.11.8/Python-3.11.8.tgz \
    && tar -xvf Python-3.11.8.tgz \
    && cd Python-3.11.8 \
    && ./configure --enable-optimizations \
    && make -j$(nproc) \
    && make altinstall \
    && cd .. \
    && rm -rf Python-3.11.8 Python-3.11.8.tgz \
    && update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.11 1

# Set Spark to use the new Python
ENV PYSPARK_PYTHON=/usr/local/bin/python3.11
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11

USER spark


# RUN apt-get update \
#  && apt-get install -y curl unzip \
#     python3 python3-setuptools \
#  && ln -s /usr/bin/python3 /usr/bin/python \
#  && easy_install3 pip py4j \
#  && apt-get clean \
#  && rm -rf /var/lib/apt/lists/*
