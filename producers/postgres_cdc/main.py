import psycopg2
import logging
import time


class PostgresCDC:

    def __init__(self, host, port, dbname, user, password, replication_slot):
        self.host = host
        self.port = port
        self.db_name = dbname
        self.user = user
        self.password = password
        self.replication_slot = replication_slot

    def start_replication_server(self):
        """Setup/Connect to the replication slots at Postgres
        Returns:
            tuple : Cursor, and Connection object for Postgres Replication conection
        """
        connect_string = 'host={0} port={1} dbname={2} user={3} password={4}'.format(
            self.host, self.port, self.db_name, self.user, self.password)
        conn = psycopg2.connect(
            connect_string, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        cur = conn.cursor()
        try:
            cur.start_replication(slot_name=self.replication_slot, decode=True)
        except psycopg2.ProgrammingError:
            cur.create_replication_slot(
                self.replication_slot, output_plugin='wal2json')
            cur.start_replication(slot_name=self.replication_slot, decode=True)

        return cur, conn

    def start_streaming(self, cur, conn, stream_receiver):
        """Listen and consume streams
        Args:
            cur (object): Cursor object
            conn (object): Connection object
            stream_receiver (function): Function to execute received streams
        """
        for _ in range(100):
            logging.info("Starting streaming, press Control-C to end...")
            try:
                cur.consume_stream(stream_receiver)

            except KeyboardInterrupt:
                cur.close()
                conn.close()
                logging.warning("The slot '{0}' still exists. Drop it with "
                                "SELECT pg_drop_replication_slot('{0}'); if no longer needed.".format(self.replication_slot))
                logging.info("Transaction logs will accumulate in pg_wal "
                             "until the slot is dropped.")
            except:
                time.sleep(5)
                try:
                    cur, conn = self.start_replication_server()
                except Exception as e:
                    logging.error(e)


if __name__ == "__main__":
    pass
