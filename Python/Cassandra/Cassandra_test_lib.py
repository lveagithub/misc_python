import os
import logging
from cassandralvea import helper

def main():
    #Constants
    CASSANDRA_HOST = '127.0.0.1'
    CASSANDRA_PORT = 9042
    LOG_LEVEL = 'DEBUG'
    LOG_FORMATTER = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    CURRENT_PATH = os.path.dirname(os.path.abspath(__file__)) + os.path.sep
    LOG_FILE_NAME = CURRENT_PATH + "log/Cassandra.log"
    KEYSPACE = "testks"
    CSV_FILE_NAME = CURRENT_PATH + "employees.csv"
    
    logHelper = helper.CassandraLoggin(version = "1.0")
    print(logHelper)

    cassandraHelper = helper.CassandraHelper(version = "1.0")
    print(cassandraHelper)

    logHelper.init_log_to_file(LOG_FILE_NAME, LOG_FORMATTER, LOG_LEVEL)
    logging.info("Connecting to Cassandra Host:" + CASSANDRA_HOST + " Port:" + str(CASSANDRA_PORT))
    session_ = cassandraHelper.cassandra_conn(CASSANDRA_HOST, CASSANDRA_PORT)
    #print(session_.execute("SELECT cluster_name FROM system.local").one())
    cassandraHelper.cassandra_create_keyspace(KEYSPACE, session_)
    cassandraHelper.cassandra_create_table(KEYSPACE, session_, table_ = KEYSPACE + ".employee")
    df = cassandraHelper.read_csv_df(CSV_FILE_NAME, None, header_list = ["id", "name", "gender", "department", "salary"])
    logging.info("Reading from csv file:" + CSV_FILE_NAME)
    logging.info("The content is:")
    logging.info(df)
    cassandraHelper.cassandra_insert_data(keyspace_name = KEYSPACE, session_ = session_, table_ = KEYSPACE + ".employee", df = df)
    df_sel = cassandraHelper.cassandra_table_to_df(keyspace_name = KEYSPACE, session_ = session_, table_ = KEYSPACE + ".employee", df = df)
    logging.info("Reading from the table employee")
    logging.info("The content of the table is:")
    logging.info(df_sel)
if __name__ == "__main__":
    main()