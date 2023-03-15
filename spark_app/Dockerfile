#ARG SPARK_VERSION=3.1.1
#FROM svishal9/spark-py:$SPARK_VERSION.latest
ARG PYTHON_VERSION=3.9.4
#USER root
#RUN apt-get update && \
#    apt-get install -y build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev
#RUN wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz
#RUN tar -xf Python-$PYTHON_VERSION.tgz
#RUN cd Python-$PYTHON_VERSION && \
#    ./configure --enable-optimizations && \
#    make -j 12 && \
#    make altinstall
#RUN apt-get install -y python3-pip
FROM python:$PYTHON_VERSION
USER root
WORKDIR /opt
RUN wget https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.11%2B9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.11_9.tar.gz && \
    wget https://downloads.lightbend.com/scala/2.13.5/scala-2.13.5.tgz && \
    wget https://apache.mirror.digitalpacific.com.au/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
RUN tar xzf OpenJDK11U-jdk_x64_linux_hotspot_11.0.11_9.tar.gz && \
    tar xvf scala-2.13.5.tgz && \
    tar xvf spark-3.1.1-bin-hadoop3.2.tgz
ENV PATH="/opt/jdk-11.0.11+9/bin:/opt/scala-2.13.5/bin:/opt/spark-3.1.1-bin-hadoop3.2/bin:$PATH"

RUN pip3 install pipenv
#TODO : Change the user to non root user
#USER 185
WORKDIR /app
COPY ../Pipfile Pipfile.lock setup.cfg /app/
COPY ../go.sh /app/go.sh
COPY ../scripts /app/scripts
RUN ./go.sh setup
COPY ../spark_app/com /app/com
ARG ARG_RUN_ACTION
ENV RUN_ACTION=$ARG_RUN_ACTION
#ENV RUN_ACTION=demo
ENTRYPOINT exec ./go.sh $RUN_ACTION
