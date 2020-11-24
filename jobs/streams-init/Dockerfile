FROM confluentinc/cp-kafka:5.5.0

WORKDIR /usr/bin
COPY schemas /usr/bin/schemas
COPY connectors /usr/bin/connectors

# Add docker-compose-wait tool
ENV WAIT_VERSION 2.7.2
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/$WAIT_VERSION/wait /wait
RUN chmod +x /wait
RUN apt-get update && apt-get --yes --force-yes install jq

COPY streams-init.sh streams-init.sh
ENTRYPOINT ["./streams-init.sh"]
