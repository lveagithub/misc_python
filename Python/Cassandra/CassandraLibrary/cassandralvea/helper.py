import sys
import logging
import logging.handlers
import os
import pandas as pd
from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster

class CassandraLoggin():
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