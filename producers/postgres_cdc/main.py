import os
import psycopg2
import logging
import time
from kafka import KafkaProducer
import json


class PostgresCDC:

    def __init__(self, host, port, dbname, user, password, replication_slot):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.replication_slot = replication_slot
        self.cur = None
        self.conn = None

    def start_replication_server(self):
        """Setup/Connect to the replication slots at Postgres
        """
        connect_string = 'host={0} port={1} dbname={2} user={3} password={4}'.format(
            self.host, self.port, self.dbname, self.user, self.password)
        self.conn = psycopg2.connect(
            connect_string, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self.cur = self.conn.cursor()
        try:
            self.cur.start_replication(
                slot_name=self.replication_slot, decode=True)
        except psycopg2.ProgrammingError:
            self.cur.create_replication_slot(
                self.replication_slot, output_plugin='wal2json')
            self.cur.start_replication(
                slot_name=self.replication_slot, decode=True)

    def start_streaming(self, stream_receiver):
        """Listen and consume streams
        Args:
            cur (object): Cursor object
            conn (object): Connection object
            stream_receiver (function): Function to execute received streams
        """
        while True:
            logging.info("Starting streaming, press Control-C to end...")
            try:
                self.cur.consume_stream(stream_receiver)

            except KeyboardInterrupt:
                self.cur.close()
                self.conn.close()
                logging.warning("The slot '{0}' still exists. Drop it with "
                                "SELECT pg_drop_replication_slot('{0}'); if no longer needed.".format(self.replication_slot))
                logging.info("Transaction logs will accumulate in pg_wal "
                             "until the slot is dropped.")
                return
            except:
                time.sleep(5)
                try:
                    self.start_replication_server()
                except Exception as e:
                    logging.error(e)


def send_to_kafka(record):
    producer.send(topic=record['table'], value=record)


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))
    cdc = PostgresCDC(
        os.environ['HOST'], os.environ['PORT'], os.environ['DBNAME'], os.environ['USER'], os.environ['PASSWORD'], os.environ['REPLICATION_SLOT'])
    cdc.start_replication_server()
    cdc.start_streaming(send_to_kafka)
    producer.close()
