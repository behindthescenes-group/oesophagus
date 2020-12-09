#!/bin/bash

# Setup ENV variables in connectors json files
sed -i "s/POSTGRES_USER/${POSTGRES_USER}/g" connectors/postgres.json
sed -i "s/POSTGRES_PASSWORD/${POSTGRES_PASSWORD}/g" connectors/postgres.json
sed -i "s/POSTGRES_DB/${POSTGRES_DB}/g" connectors/postgres.json
sed -i "s/ELASTIC_PASSWORD/${ELASTIC_PASSWORD}/g" connectors/elasticsearch.json

# Simply wait until original kafka container and zookeeper are started.
export WAIT_HOSTS=zookeeper:2181,broker:9092,schema-registry:8081,ksqldb-server:8088,elasticsearch:9200,connect:8083
export WAIT_HOSTS_TIMEOUT=300
/wait

# Parse string of kafka topics into an array
# https://stackoverflow.com/a/10586169/4587961
kafkatopicsArrayString="$KAFKA_TOPICS"
IFS=', ' read -r -a kafkaTopicsArray <<< "$kafkatopicsArrayString"

# A separate variable for zookeeper hosts.
zookeeperHostsValue=$ZOOKEEPER_HOSTS

# Create kafka topic for each topic item from split array of topics.
for newTopic in "${kafkaTopicsArray[@]}"; do
    # https://kafka.apache.org/quickstart
    curl http://elasticsearch:9200/enriched_$newTopic/_search --user elastic:${ELASTIC_PASSWORD}
    curl -X DELETE http://schema-registry:8081/subjects/store.public.$newTopic-value
    kafka-topics --create --topic "store.public.$newTopic" --partitions 1 --replication-factor 1 --if-not-exists --zookeeper "$zookeeperHostsValue"
    curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data @schemas/$newTopic.json http://schema-registry:8081/subjects/store.public.$newTopic-value/versions


done

# Terminate all queries
curl -s -X "POST" "http://ksqldb-server:8088/ksql" \
         -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
         -d '{"ksql": "SHOW QUERIES;"}' | \
  jq '.[].queries[].id' | \
  xargs -Ifoo curl -X "POST" "http://ksqldb-server:8088/ksql" \
           -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
           -d '{"ksql": "TERMINATE 'foo';"}'



# Drop All Tables
curl -s -X "POST" "http://ksqldb-server:8088/ksql" \
             -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
             -d '{"ksql": "SHOW TABLES;"}' | \
      jq '.[].tables[].name' | \
      xargs -Ifoo curl -X "POST" "http://ksqldb-server:8088/ksql" \
               -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
               -d '{"ksql": "DROP TABLE \"foo\";"}'


# Drop All Streams
curl -s -X "POST" "http://ksqldb-server:8088/ksql" \
           -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
           -d '{"ksql": "SHOW STREAMS;"}' | \
    jq '.[].streams[].name' | \
    xargs -Ifoo curl -X "POST" "http://ksqldb-server:8088/ksql" \
             -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
             -d '{"ksql": "DROP STREAM \"foo\";"}'

curl -X "POST" "http://ksqldb-server:8088/ksql" -H "Accept: application/vnd.ksql.v1+json" -d $'{ "ksql": "CREATE STREAM \\"brands\\" WITH (kafka_topic = \'store.public.brands\', value_format = \'avro\');", "streamsProperties": {} }'
curl -X "POST" "http://ksqldb-server:8088/ksql" -H "Accept: application/vnd.ksql.v1+json" -d $'{ "ksql": "CREATE STREAM \\"enriched_brands\\" WITH ( kafka_topic = \'enriched_brands\' ) AS SELECT CAST(brand.id AS VARCHAR) as \\"id\\", brand.tenant_id as \\"tenant_id\\", brand.name as \\"name\\" from \\"brands\\" brand partition by CAST(brand.id AS VARCHAR) EMIT CHANGES;", "streamsProperties": {} }'

curl -X "POST" "http://ksqldb-server:8088/ksql" -H "Accept: application/vnd.ksql.v1+json" -d $'{ "ksql": "CREATE STREAM \\"brand_products\\" WITH ( kafka_topic = \'store.public.brand_products\', value_format = \'avro\' );", "streamsProperties": {} }'
curl -X "POST" "http://ksqldb-server:8088/ksql" -H "Accept: application/vnd.ksql.v1+json" -d $'{ "ksql": "CREATE TABLE \\"brands_table\\" AS SELECT id as \\"id\\", latest_by_offset(tenant_id) as \\"tenant_id\\" FROM \\"brands\\" group by id EMIT CHANGES;", "streamsProperties": {} }'
curl -X "POST" "http://ksqldb-server:8088/ksql" -H "Accept: application/vnd.ksql.v1+json" -d $'{ "ksql": "CREATE STREAM \\"enriched_brand_products\\" WITH ( kafka_topic = \'enriched_brand_products\' ) AS SELECT \\"brand\\".\\"id\\" as \\"brand_id\\", \\"brand\\".\\"tenant_id\\" as \\"tenant_id\\", CAST(brand_product.id AS VARCHAR) as \\"id\\", brand_product.name AS \\"name\\" FROM \\"brand_products\\" AS brand_product INNER JOIN \\"brands_table\\" \\"brand\\" ON brand_product.brand_id = \\"brand\\".\\"id\\" partition by CAST(brand_product.id AS VARCHAR) EMIT CHANGES;", "streamsProperties": {} }'

curl -X DELETE http://connect:8083/connectors/enriched_writer
curl -X "POST" -H "Content-Type: application/json" --data @connectors/elasticsearch.json http://connect:8083/connectors

curl -X DELETE http://connect:8083/connectors/event_reader
curl -X "POST" -H "Content-Type: application/json" --data @connectors/postgres.json http://connect:8083/connectors
sleep 15.0s
curl -X DELETE http://connect:8083/connectors/event_reader
curl -X "POST" -H "Content-Type: application/json" --data @connectors/postgres.json http://connect:8083/connectors
