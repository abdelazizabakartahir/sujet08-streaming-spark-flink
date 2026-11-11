FROM flink:1.18-scala_2.12
USER root
RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir apache-flink==1.18.0
USER flink
WORKDIR /opt/flink/usrlib
