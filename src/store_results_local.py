"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

This file implements a small wrapper to easily insert the query results
locally from the MongoDB client.

FILE: store_results_local.py

USAGE:

    db = LocalDB('test')
    db.print_db_info()
    db.insert_fi_result(n_nodes, db_type, db_meta, test_dataset,
                        test_scenario, effects, run_id, res_id)

"""

from pymongo import MongoClient
from datetime import datetime


class LocalDB:
    def __init__(self, db_name, collection="faults"):
        self.client = MongoClient()
        self.db = self.create_db(db_name)
        self.collection = collection

    def create_db(self, db_name):
        return self.client[db_name]

    def switch_db(self, db_name):
        self.db = self.client[db_name]

    def switch_col(self, col_name):
        self.collection = col_name

    def print_db_info(self):
        print "Current DB: {}\nCurrent collection: {}".format(self.db, self.collection)

    def insert_fi_result(self, n_nodes, db_type, db_version, db_meta, test_dataset,
                         test_scenario, effects, run_id, res_id):
        self.db[self.collection].insert_one(
            {
                "server_params":
                    {
                        "n_nodes": n_nodes
                    },
                "db_type": db_type,
                "db_version": db_version,
                "db_meta": db_meta,
                "tested_dataset": test_dataset,
                "test_scenario": test_scenario,
                "effects": effects,
                "run_id": run_id,
                "res_id": res_id,
                "time": datetime.now().isoformat()
             }
            )

    def query_db(self, query=None):
        return self.db[self.collection].find(query)
