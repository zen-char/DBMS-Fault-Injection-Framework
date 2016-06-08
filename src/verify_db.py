"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

File to communicate with a local SQLite database and create a verifications database.
Has to be extended to work with multiple file formats.

FILE: verify_db.py

USAGE:
    from verify_db import SQLiteDB.
    db = SQLiteDB(db_name='verifications', table_name='results')
    db.insert(query_id, query_res_num, file_name, file_hash)
    db.check(query_id, query_res_num=query_res_num)

"""
import sqlite3


class SQLiteDB:
    def __init__(self, db_name='verifications', table_name='results'):
        self.connection = sqlite3.connect(db_name)
        self.db_name = db_name
        self.table_name = table_name
        self.setup()

    # Return a mysql database connection.
    def close_connection(self):
        self.connection.close()

    # Query the db.
    def _query_db(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()

    # Use one of the databases.
    def use_db(self, db_name):
        self._query_db("USE {}".format(db_name))
        self.db_name = db_name

    # Insert query id, file name and hash into the main table.
    def insert(self, query_id, query_res_num, file_name, file_hash):
        cursor = self.connection.cursor()
        insert_stmt = "INSERT INTO {} (query_id, query_res_num, ".format(self.table_name) +\
                      "file_name, hash) VALUES (?, ?, ?, ?)"
        cursor.execute(insert_stmt, (query_id, query_res_num, file_name, file_hash))
        self.connection.commit()

    # Get all hashes from a query id.
    def check(self, query_id, query_res_num=None):
        cursor = self.connection.cursor()
        if query_res_num is not None:
            search_stmt = "SELECT hash FROM {} WHERE query_id=? ".format(self.table_name) +\
                          "AND query_res_num=?"
            cursor.execute(search_stmt, (query_id, query_res_num,))
            result = cursor.fetchone()
            if result is None:
                return result
            else:
                return result[0]
        else:
            search_stmt = "SELECT hash FROM {} WHERE query_id=?".format(self.table_name)
            cursor.execute(search_stmt, (query_id,))
            results = cursor.fetchall()
            if results is None:
                return []
            else:
                return [res[0] for res in results]

    # Cleanup the mysql database.
    def drop_table(self):
        self._query_db("DROP TABLE IF EXISTS {}".format(self.table_name))

    # Setup the database.
    def setup(self):
        create_table = "CREATE TABLE IF NOT EXISTS {} (".format(self.table_name) +\
                         "id             INT  UNSIGNED AUTO_INCREMENT PRIMARY KEY," +\
                         "query_id       INT  UNSIGNED NOT NULL," +\
                         "query_res_num  INT  UNSIGNED NOT NULL," +\
                         "file_name      TEXT          NOT NULL," +\
                         "hash           TEXT          NOT NULL);"
        cursor = self.connection.cursor()
        cursor.execute(create_table)
        self.connection.commit()
