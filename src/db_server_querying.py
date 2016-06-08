"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

This file contains code used on the server side to run most of the
test commands using a database.

The file is used to initialize the database and run the queries,
and afterwards validate the query results. The verification database
is also filled with the query results. The restoration of the backup
files also is performed using the restore command in this file.

FILE: db_server_querying.py

USAGE: python db_server_querying.py scenario.json index '{"type": "query/verify/test/restore"}'
       Also see the main method of this file for more information.

"""

import sys
import json
import os
import io
import re
import subprocess
import tarfile
from utils import print_json, load_json_file, ascii_encode_dict, gen_checksum_from_file, color_str
from verify_db import SQLiteDB


# Insert data in the database.
def insert_data(db_session, data_to_insert):
    if 'files' in data_to_insert:  # Insert blob data into the database.
        files_dir = 'fi-framework/' + data_to_insert['files']
        db_session.insert_files(files_dir)

    elif 'csv' in data_to_insert:  # Insert text/numbers/blob data in the database.
        csv_file = data_to_insert['csv']
        # If no column types are specified, everything is inserted as text.
        csv_column_types = data_to_insert['columns'] if 'columns' in data_to_insert else None
        db_session.insert_csv(csv_file, csv_column_types)


# Create data in the database.
def create_dbsession_from_type(db_type, db_meta, host='127.0.0.1', force_reuse_keyspace=False):
    db_session = None

    if db_type == 'cassandra':
        # === Type cassandra ===
        # - Required: Keyspace name given as keyspace in the db_meta data.
        # - Optional: If the keyspace has to be reused. Default: True.
        # - Optional: The keyspace init params, default replication is 1 using SimpleStrategy.
        db_key = db_meta['keyspace'] if 'keyspace' in db_meta else None
        if db_key is None:
            print "No keyspace given.. exiting"
            sys.exit()
        db_reuse_key = db_meta['reuse_keyspace'] if 'reuse_keyspace' in db_meta else True
        if force_reuse_keyspace:  # Do only delete the keyspace when inserting data.
            db_reuse_key = True
        db_init_params = db_meta['init'] if 'init' in db_meta else None

        # Load cassandra db_functions.
        from databases.cassandra.db_functions import DBSession
        # Ascii is only accepted...
        db_init_params = ascii_encode_dict(db_init_params)
        db_session = DBSession(db_key, host=host, keyspace_init=db_init_params, reuse_keyspace=db_reuse_key)

    if db_session is None:
        print "Session could not be started with db_type: {}.".format(db_type)
        sys.exit()

    return db_session


def verify_and_test_db(main_file, host_id, run_params):
    # Parse main query file.
    parse_data = load_json_file(main_file)
    query_data = load_json_file('fi-framework/' + parse_data['query_file'])
    queries = query_data['queries']

    db_type = parse_data['db_type']
    db_init = parse_data['db_meta']

    localhost = '127.0.0.1'
    run_type = run_params['type']
    db_session = None
    if db_type == 'cassandra' and run_type in ['verify', 'test', 'query']:
        # Do not always delete the keyspace when the reuse_keyspace param is set to
        # False. Example given, when querying you probably do not want to remove the
        # data each time.
        force_reuse_keyspace = False
        if ('insert_data' in parse_data and parse_data['insert_data'] == True) and \
           (run_type != 'verify'):
            force_reuse_keyspace = True
        db_session = create_dbsession_from_type(db_type, db_init, host=localhost,
                                                force_reuse_keyspace=force_reuse_keyspace)

    if run_type == 'verify':  # Initialize and fill the verification database.
        _insert_and_verify_cmd(parse_data, db_session, queries)
        db_session.shutdown()
    elif run_type == 'test':  # Run the test queries and verification queries.
        _test_cmd(db_session, queries, run_params, db_type)
        db_session.shutdown()
    elif run_type == 'retrieve_targets':  # Retrieve DBMS target files.
        password = ''
        if 'password' in parse_data['server_meta']:
            password = parse_data['server_meta']['password']
            if not (isinstance(password, unicode) or isinstance(password, str)):
                password = password[host_id]
        _retrieve_cmd(run_params, parse_data, password, db_type)
    elif run_type == 'clear_verification_db':  # Clear the verification DBMS.
        print "Deleting verification db."
        verification_db = SQLiteDB()
        verification_db.drop_table()
    elif run_type == 'restore':  # Restore the current db_data directory with the backup tar.
        _restore_cmd(run_params)
    elif run_type == 'query':  # Query the DBSession.
        _query_cmd(db_session, run_params)
        db_session.shutdown()
    else:
        print "Unknown command given: {}".format(run_type)


def _insert_and_verify_cmd(parse_data, db_session, queries):
    if 'insert_data' in parse_data and parse_data['insert_data'] == True:
        insert_data(db_session, parse_data['data_to_insert'])

    query_results = []

    verification_db = SQLiteDB()
    verification_db.drop_table()
    verification_db.setup()

    query_id = 0
    hash_data = False
    if 'files' in parse_data['data_to_insert']:
        hash_data = True

    for query in queries:
        print "Querying: {}".format(query)
        query_res = db_session.query_db(query, hash_files=hash_data, time_out=300)
        query_res_count = 0
        # Row result is expected to contain:
        # [[ID, file hashed by query_db function, file name], ...]
        for row in query_res['result']:
            verification_db.insert(query_id, query_res_count, row[-1], str(row[1]))
            query_res_count += 1
        if query_res_count == 0:
            warning = color_str("WARNING: ", color='y')
            print warning, "Invalid query occurred, no results retrieved (may be intentional)."
            print warning, "QUERY {}".format(query)
            print warning, "RESULT {}".format(query_res)
        query_results.append(query_res)
        query_id += 1

    print print_json({"results": query_results})


# Test on several errors per db_type. It is expected that those exceptions
# are caught.
def _test_cmd(db_session, queries, run_params, db_type):
    hash_data = False
    if 'data_type' in run_params and run_params['data_type'] == 'files':
        hash_data = True

    verification_db = SQLiteDB()
    number_of_duplicates, query_mismatches = 0, 0
    query_faults = {}

    error_list = []
    if db_type == "cassandra":
        error_list = ["read_failure", "time_out", "coordinator_failure",
                      "write_failure", "invalid_request", "no_host_available"]

    query_id = 0
    for query in queries:
        # Query the database and check the query results.
        # The expected result is formatted as:
        # {result: [[ID, File contents hash, File name], ...], timeout: 1, ..}
        query_res = db_session.query_db(query, hash_files=hash_data, time_out=300)
        query_faults[query_id] = {}
        if len(query_res['result']) == 0:
            for error in error_list:
                if error in query_res:
                    query_faults[query_id][error] = 1
        query_dups = _verify_query(query_res['result'])

        # Secondly calculate verification_db mismatches, which will include out of order
        # result mismatches.
        query_res_count = 0
        query_verify_errors = 0
        for row in query_res['result']:
            verification_hash = verification_db.check(query_id, query_res_count)
            if str(verification_hash) != str(row[1]):
                query_verify_errors += 1
            query_res_count += 1

        # Initialize all found errors.
        number_of_results_check = verification_db.check(query_id)
        query_faults[query_id]["verification_errors"] = query_verify_errors
        query_faults[query_id]["results_missing"] = len(query_res['result']) - len(number_of_results_check)
        query_faults[query_id]["duplicates"] = query_dups
        query_faults[query_id]["timestamp"] = query_res['timestamp']

        number_of_duplicates += query_dups
        query_id += 1

    print "'{}'".format(json.dumps(query_faults))


# Verify the results of a query on duplicates, by checking the ids
# and the file name if they did not already occur somewhere.
# The query res rows are expected to be in the format [ID, ..., file name].
def _verify_query(query_res):

    query_files = []
    query_ids = []
    # Check if the file is already found in the query file list, else append.
    # If the item is already found, a duplicate occurred.
    duplicates = 0
    for row in query_res:
        if row[-1] in query_files and row[0] in query_ids:
            duplicates += 1
        else:
            query_files.append(row[-1])
            query_ids.append(row[0])

    return duplicates


def _retrieve_cmd(run_params, parse_data, password, db_type):
    scenario_id = run_params['scenario_id']
    test_scenario = parse_data['test_scenarios']['scenarios'][scenario_id]

    # Get the requested features of the target list.
    excluded_files = test_scenario['excluded_files'] if 'excluded_files' in test_scenario else None
    excluded_ext = test_scenario['excluded_extensions'] if 'excluded_extensions' in test_scenario else None
    target_sys_files = test_scenario['target_sys_files'] if 'target_sys_files' in test_scenario else False
    excluded_substrs = test_scenario['exclude_containing'] if 'exclude_containing' in test_scenario else None

    # Get the file list via a sudo command, as the file pointer of the files used in this script
    # can already be in use.
    container_id = run_params['container_id']
    strace_cmd = os.path.dirname(os.path.abspath(__file__)) + os.sep + 'attach_strace.py'
    p = subprocess.Popen(['sudo', '-S', 'python', strace_cmd, db_type, 'list'], stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    (db_files, err) = p.communicate(password + '\n')

    db_files = db_files.split('\n')
    db_files = list(set(db_files))

    # Filter on specific file extensions.
    if excluded_ext is not None:
        excluded_files = [db_file for db_file in db_files for ext in excluded_ext if db_file.endswith(ext)]
        db_files = list(set(db_files) - set(excluded_files))

    if excluded_substrs is not None:
        db_files = remove_substrs_from_db_files(db_files, excluded_substrs)

    base_cmd = ['sudo', '-S', 'docker', 'exec', container_id, 'file']

    # Now retrieve a valid target file list.
    for db_file in db_files:
        # Skip non path files such as inode, excluded files and some system files.
        if (db_file == '') or (db_file[0] != '/') or \
           (excluded_files is not None and db_file in excluded_files) or \
           (not target_sys_files and db_file in ['/proc/stat/', '/proc/stat', '/dev/urandom', '/dev/random']):
            continue

        # Check if the file is not a directory and exists.
        p = subprocess.Popen(base_cmd + [db_file], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, universal_newlines=True)
        (out, err) = p.communicate(password + '\n')
        if not (': directory' in out or '(No such file or directory)' in out):
            print db_file


def remove_substrs_from_db_files(db_files, excluded_substrs):
    to_remove = []
    for db_file in db_files:
        for substr in excluded_substrs:
            pattern = re.compile(substr)
            if pattern.search(db_file):
                to_remove.append(db_file)
    return list(set(db_files) - set(to_remove))


def _restore_cmd(run_params, backup_file_list='fi-framework/backup_file_list.json'):
    backup_path = run_params['backup']
    data_path = run_params['data']

    backup_file = tarfile.open(backup_path)
    tar_file_list = _get_tar_file_list(backup_file_list, backup_file)

    # First remove newly created files:
    data_files = []
    for root_dir, _, files in os.walk(data_path):
        paths = [os.path.join(root_dir, name) for name in files]
        data_files += paths

    modified_files = []
    for data_file in data_files:
        # It is a newly added file. Which has to be removed.
        if data_file not in tar_file_list:
            os.remove(data_file)
        else:  # Now check if it is a changed files
            with io.open(data_file, 'rb') as f:
                if tar_file_list[data_file] != gen_checksum_from_file(f, use_file=True):
                    modified_files.append(data_file)

    # No files are changed here, and all files specific files are still there.
    if len(modified_files) == 0 and len(data_files) == len(tar_file_list):
        return

    # Check if all backup files exists..
    for backup_file_path in tar_file_list:
        if not os.path.exists(backup_file_path) and backup_file_path not in modified_files:
            modified_files.append(backup_file_path)

    # Restore from the backup files.
    if len(modified_files) > 0:
        for member in backup_file:
            if member.name in modified_files:
                backup_file.extract(member)
                modified_files.remove(member.name)
                if len(modified_files) == 0:
                    break
    backup_file.close()


def _get_tar_file_list(backup_file_list, backup_file):
    tar_file_list = {}
    # Create a {file_name : checksum} dictionary of all backup files.
    # This has only to be created once. Else all data can just be read again.
    if os.path.exists(backup_file_list):
        with io.open(backup_file_list, 'r') as f:
            tar_file_list = json.loads(f.read())
    else:
        for member in backup_file:
            if member.isfile():
                f = backup_file.extractfile(member)
                tar_file_list[unicode(member.name)] = gen_checksum_from_file(f, use_file=True)
                f.close()
        # Write the entire dictionary.
        with io.open(backup_file_list, 'w+') as f:
            f.write(unicode(print_json(tar_file_list)))
    return tar_file_list


def _query_cmd(db_session, run_params):
    timeout = 60
    hash_files = False

    if 'query_type' in run_params and run_params['query_type'] == 'files':
        hash_files = True
    if 'time_out' in run_params:
        timeout = run_params['time_out']
    print db_session.query_db(run_params['query'], time_out=timeout, hash_files=hash_files)


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print "Usage: python db_sever_querying.py <main_json.json> <host_id> <json_params>"
        print "Where <json params> could be:"
        print "      {type: query, query:.., query_type:.., timeout:..}"
        print "      {type: verify}"
        print "      {type: test, test_id, ..}"
        print "      {type: restore, backup: backup_path, data: data_path}"
        sys.exit()

    verify_and_test_db(sys.argv[1], int(sys.argv[2]), json.loads(sys.argv[3].strip("'")))
