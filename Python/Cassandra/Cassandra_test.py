import sys
import logging
import logging.handlers
import os
import pandas as pd
from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster

class LogHelper():
    """Logging Helper"""
    def __init__(self, version):
        self.version = version
    
    def __str__(self):
        return f"Logging helper version {self.version}"
    
    def init_logs(self, log_level, log_formatter):
        log = logging.getLogger('cassandra')
        log.setLevel(log_level)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(log_formatter))
        log.addHandler(handler)
        return log
    
    def init_log_to_file(self, log_file_name, log_formatter, log_level):
        #handler = logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", log_file_name))
        handler = logging.handlers.RotatingFileHandler(os.environ.get("LOGFILE", log_file_name), maxBytes=1000000, backupCount=2)
        formatter = logging.Formatter(logging.BASIC_FORMAT)
        handler.setFormatter(formatter)
        root = logging.getLogger()
        root.setLevel(os.environ.get("LOGLEVEL", log_level))
        root.addHandler(handler)

class CassandraHelper():
    """Cassandra Helper"""
    def __init__(self, version):
        self.version = version
    
    def __str__(self):
        return f"Cassandra helper version {self.version}"

    def cassandra_conn(self, cassandra_host, cassandra_port):
        cluster_ = Cluster(contact_points=[cassandra_host],port=cassandra_port)
        session_ = cluster_.connect()
        return session_

    def cassandra_create_keyspace(self, keyspace_name, session_):
        rows = session_.execute("""SELECT keyspace_name FROM system_schema.keyspaces""" )
        if keyspace_name in [row[0] for row in rows]:
            logging.info("Dropping keyspace:" + keyspace_name)
            session_.execute("DROP KEYSPACE " + keyspace_name)
        
        logging.info("Creating keyspace:" + keyspace_name)
        session_.execute("""
            CREATE KEYSPACE %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
            """ % keyspace_name)

    def cassandra_create_table(self, keyspace_name, session_, table_):
        logging.info("creating table:" + keyspace_name)
        session_.execute("""
            CREATE TABLE %s (
                id int PRIMARY KEY,
                name text,
                gender text,
                department text,
                salary varint
            )
            """ % table_)

    def read_csv_df(self, file_name, header_, header_list):
        df = pd.read_csv (file_name, header=header_, names=header_list)
        return df

    def cassandra_insert_data(self, keyspace_name, session_, table_, df):
        logging.info("Inserting data into table:" + table_)
        query = "INSERT INTO %s(id,name,gender,department,salary) VALUES (?,?,?,?,?)" %table_
        #print(query)
        prepared = session_.prepare(query)
        #for item in df:
        #    session_.execute(prepared, (item.id,item.name,item.gender,item.department,item.salary))
        for index, row in df.iterrows():
            #print(row['id'], row['name'], row['gender'], row['department'], row['salary'])
            session_.execute(prepared, (row['id'], row['name'], row['gender'], row['department'], row['salary']))

    def cassandra_table_to_df(self, keyspace_name, session_, table_, df):
        query = "SELECT id, department, gender, name, salary FROM testks.employee"
        df_sel = pd.DataFrame(list(session_.execute(query)))
        return df_sel

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

    #log_ = init_logs(LOG_LEVEL, LOG_FORMATTER)
    #log_.info("Starting process")
    
    logHelper = LogHelper(version = "1.0")
    #print(logHelper)

    cassandraHelper = CassandraHelper(version = "1.0")
    #print(cassandraHelper)

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