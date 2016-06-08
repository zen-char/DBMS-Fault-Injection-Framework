"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

Create a DBSession object which can be used to easily communicate with the
database and obtain the results.

This file is made to create a simple Apache-Cassandra wrapper.

Data sets in the form of a CSV can be loaded via 'insert_csv'  function,
a folder structure with files (images or anything else), can be loaded via the 'insert_files'
command.

FILE db_functions.py

USAGE: python db_functions.py # This will run the can_connect function.
       - Create a DBSession to insert files or anything else.

"""


import os
import io
import sys
import time
import datetime as dt
import cassandra
from cassandra.cluster import Cluster, NoHostAvailable, NoConnectionsAvailable
from cassandra.protocol import ConfigurationException
import hashlib


# Code referenced from:
# http://stackoverflow.com/questions/3431825/
# Do not set the hasher in the header of the function.
def gen_checksum_from_file(file_path, hasher=None, blocksize=65536, use_file=False):
    if hasher is None:
        hasher = hashlib.md5()

    opened_file = file_path if use_file else io.open(file_path, 'rb')
    buf = opened_file.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = opened_file.read(blocksize)

    if not use_file:
        opened_file.close()
    return hasher.digest().encode('hex')


def gen_checksum_from_bytes(byte_string, hasher=None):
    if hasher is None:
        hasher = hashlib.md5()
    hasher.update(byte_string)
    return hasher.digest().encode('hex')


# Check if the cassandra cluster is already available.
def can_connect():
    retries = 0
    start = time.time()
    while True:
        try:
            cluster = Cluster().connect()
            return retries
        except (NoHostAvailable, NoConnectionsAvailable):
            retries += 1
            time.sleep(2.0)
        if time.time() - start > 300:
            print "ERROR: could not connect."
            sys.exit()


class DBSession:
    def __init__(self, keyspace, host='127.0.0.1', port=7000, keyspace_init=None, reuse_keyspace=True):
        self.keyspace = keyspace
        self.data_id = 0
        self.host = host
        self.port = port
        self.session = self.open_db(keyspace_init, reuse_keyspace=reuse_keyspace)
        self.session.default_timeout = 60

    # Create a new key space with the simply strategy params.
    def create_keyspace(self, replication_parameters):
        if replication_parameters is None:
            replication_parameters = {'class': 'SimpleStrategy', 'replication_factor': 1}

        self.session.execute("CREATE KEYSPACE {} ".format(self.keyspace) +
                             "WITH REPLICATION = {};".format(str(replication_parameters)), timeout=60)

    # Open the database with a lot of error handling as it is not easy to check if key spaces exist.
    def open_db(self, keyspace_init=None, reuse_keyspace=True):
        cluster = Cluster()  # Cluster([self.host], port=self.port)
        cluster.connect_timeout = 20

        try:
            self.session = cluster.connect()
        except cassandra.cluster.NoHostAvailable:
            print "Ensure an instantiation of Cassandra is running."
            sys.exit()

        if reuse_keyspace:
            try:
                self.session.execute("USE {}".format(self.keyspace))
            except (cassandra.InvalidRequest, ConfigurationException):
                print "Keyspace: '{}' does not exists, creating the keyspace.".format(self.keyspace)
                self.create_keyspace(keyspace_init)
                time.sleep(0.5)  # Sleep half a second.
                self.session.execute("USE {}".format(self.keyspace))
        else:
            try:
                self.session.execute("DROP KEYSPACE {};".format(self.keyspace), timeout=60)
            except (cassandra.InvalidRequest, ConfigurationException):
                print "Keyspace: '{}' does not exists, creating the keyspace.".format(self.keyspace)

            self.create_keyspace(keyspace_init)
            time.sleep(0.5)  # Sleep half a second.
            self.session.execute("USE {}".format(self.keyspace))
        return self.session

    # Execute a query statement with time out 5 sec.
    def query_db(self, query, params=None, time_out=60.0, hash_files=False):
        results = []
        start = time.time()
        query_res = []
        return_results = {}

        try:
            if params is None:
                query_res = self.session.execute(query, timeout=time_out)
            else:
                query_res = self.execute_stmt(query, params, time_out=time_out)
            return_results['timestamp'] = query_res.response_future.message.timestamp * 10.0 ** -6
        except cassandra.ReadFailure:   # Result could not be read.
            return_results['read_failure'] = 1
        except cassandra.WriteFailure:  # Result could not be written.
            return_results['write_failure'] = 1
        except cassandra.CoordinationFailure:  # Result coordination went wrong.
            return_results['coordinator_failure'] = 1
        except cassandra.Timeout:  # Query timeout; no results could be obtained.
            return_results['time_out'] = 1
        except cassandra.InvalidRequest:  # Query is invalid.
            return_results['invalid_request'] = 1
        except NoHostAvailable:
            return_results['no_host_available'] = 1

        # Add or convert timestamp in seconds. Based on:
        # http://stackoverflow.com/questions/7852855/
        if 'timestamp' not in return_results:
            return_results['timestamp'] = dt.datetime.utcnow()
        else:
            return_results['timestamp'] = dt.datetime.utcfromtimestamp(return_results['timestamp'])
        return_results['timestamp'] = str(return_results['timestamp'].time())

        for row in query_res:
            row = list(row)
            if hash_files:
                row[1] = int(gen_checksum_from_bytes(row[1]), 16)
            results.append(list(row))
        return_results["result"] = results
        return_results["time"] = time.time() - start
        return return_results

    def create_table(self, table_name, params):
        try:
            self.session.execute("CREATE TABLE {} (".format(table_name) +
                                 "id INT PRIMARY KEY," + params + ");")
        except cassandra.AlreadyExists:
            print "Using already existing table.."

    # Execute a prepared database statement.
    def execute_stmt(self, prepare_stmt, params, time_out=60.0):
        return self.session.execute(prepare_stmt, params, timeout=time_out)

    # Prepare a statement so database networking is minimized.
    # The statement is of the format:
    # INSERT INTO db_name (id, field1, field2, ...) VALUES (?, ?, ?, ...);
    def prepare_stmt(self, table_name, params, num_params):
        stmt = "INSERT INTO {} (id, ".format(table_name)
        stmt += params + ") "
        stmt += "VALUES ("
        stmt += ", ".join(['?'] * (num_params + 1))
        stmt += ");"
        return self.session.prepare(stmt)

    # Insert files in the database given a directory. Use the folder names as tables
    # and use data blobs to insert the information.
    def insert_files(self, dir_name, use_hash=False):
        file_num = 0
        file_hash = None
        for root_dir, _, files in os.walk(dir_name):
            paths = [os.path.join(root_dir, name) for name in files]

            if len(files) == 0:
                continue

            # Create a table from the current folder name with spaces converted to '_' chars.
            table_name = root_dir.split(os.sep)[-1]
            table_name = '_'.join(table_name.split(' '))
            print "Current table: {}".format(table_name)

            # Create table...
            self.create_table(table_name, "file_name text, data blob")
            # Prepare insert stmt.
            insert_stmt = self.prepare_stmt(table_name, "file_name, data", 2)

            # Load binary data and insert.
            for path in paths:
                file_name = path.split(os.sep)[-1]
                if use_hash:
                    file_hash = int(gen_checksum_from_file(path), 16)
                with io.open(path, 'rb') as f:
                    if use_hash:
                        self.execute_stmt(insert_stmt, [file_hash, file_name, f.read()], time_out=60)
                    else:
                        self.execute_stmt(insert_stmt, [file_num, file_name, f.read()], time_out=60)
                file_num += 1

    # Insert a csv file into the database, where the first row is used as table column names.
    # The column types can be given in a separate array, such as: ["int", "text", "blob"]
    def insert_csv(self, file_name, column_types=None):
        with io.open(file_name, 'r') as f:
            # Get file name from current file path by splitting on the path separator
            # then get the name without the extension.
            table_name = file_name.split(os.sep)[-1]
            table_name = table_name.split('.')[0]
            csv_header = f.readline()

            # Create the table and insertion statement from the header.
            self._create_table_from_csv_header(csv_header, table_name, column_types)
            insert_stmt = self._prepare_insert_stmt_from_csv_header(csv_header, table_name)

            for row in f:
                # Remove newline and convert to list.
                row = row[:-1].split(',')

                # Prevent unicode decode errors.
                if column_types is not None:
                    row = [int(row[i]) if column_types[i] == 'int' else row[i] for i in range(len(row))]

                # Execute the query
                self.execute_stmt(insert_stmt, [self.gen_next_data_id()] + row)

    # Create the table from the csv_header.
    def _create_table_from_csv_header(self, header, table_name, column_types):
        header = header[:-1].split(',')

        if column_types is None:
            params = ", ".join(data + " text" for data in header)
        else:
            params = ", ".join(data + " " + str(column_type) for (data, column_type) in zip(header,
                                                                                            column_types))
        self.create_table(table_name, params)

    # Prepare an insert statement given the csv_header.
    def _prepare_insert_stmt_from_csv_header(self, header, table_name):
        header = header[:-1].split(',')

        num_columns = len(header)
        column_names = ", ".join(header)
        return self.prepare_stmt(table_name, column_names, num_columns)

    # Centralized function to generate a data id.
    def gen_next_data_id(self):
        self.data_id += 1
        return self.data_id

    def shutdown(self):
        self.session.cluster.shutdown()
        self.session.shutdown()


if __name__ == '__main__':
    # Try to connect with the cassandra instance.
    print "Connected in {} tries:".format(can_connect())
