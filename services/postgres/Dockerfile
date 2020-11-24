FROM postgres:11.4
LABEL maintainer="sahilmalhotra848@gmail.com"

COPY setup-primary.sh /docker-entrypoint-initdb.d/setup-primary.sh

WORKDIR /workspace

COPY wal2json-master.zip wal2json-master.zip

RUN apt-get update && \
  apt-get install -y wget unzip make gcc postgresql-server-dev-11 && \
  unzip wal2json-master.zip && \
  cd wal2json-master && \
  make && \
  make install && \
  apt-get remove -y unzip wget make gcc postgresql-server-dev-11 && \
  cd .. && \
  rm -rf ./*

ENTRYPOINT /docker-entrypoint.sh postgres