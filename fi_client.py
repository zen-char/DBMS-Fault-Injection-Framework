"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

This file contains the main fault injection framework code which make use
of all the files listed below. The code below demonstrates usage of the framework.

The framework creates a database cluster on which fault injection can be
performed via the json test scenarios provided as input file. The example_q and
example.json files should give an indication how the fault injections are performed.
The tests make use of

NOTE:
    The framework does not guarantee it will always work and is tested and written
    in a small time period. So expect bugs and sometimes unexpected results.

TODO:
    - Use a more object oriented approach in some framework files
     (via means of inheritance using different db types).

Project structure:

=== ./ directory ===
- example_q.json & example.json
    Contain files to give an example of the supported framework features.
- fi_framework.py (used CLIENT side)
    The program that parses the json file and starts and installs the
    server dependencies.
- install_server_debs.py (used CLIENT side)
    Program that installs the server dependencies.

=== src/ directory ===
- db_server_querying.py (used SERVER side)
    Program containing the server functions which perform:
    - The database intialization
    - Querying of the database
    - Retrieving target files
    - Testing and querying the database following the test scenario
- attach_strace.py (used SERVER side)
    Program that finds a cassandra instance and attaches strace. Will kill itself
    automatically when the cassandra process closes.
- server_comm.py (used CLIENT side)
    Program that creates a ssh wrapper to transfer files and executes commands.
    It executes all server communication.
- utils.py (used CLIENT/SERVER side)
    Contains some json helper functions, time string to datetime converters.
- store_results_local.py (used CLIENT side)
    Program that creates a mongodb wrapper to store results more easily.
- verify_db.py (used SERVER side)
    Program that contains the SQLite verification database used on the server.

=== src/faults/ directory ===
- bit_flip.py (used SERVER side)
    A program that insert a bit flip given a program name and the number of flips
    to be inserted.
- stuck_bit.py (used SERVER side)
    NOTE: can not be used yet, parsing code has to be implemented yet.

=== src/databases/ ===
- cassandra/db_functions.py (used SERVER side)
    Program that created a cassandra DBSession, including key space. Also contains
    functions to load data sets.

FILE: fi_framework.py

USAGE: python fi_framework.py example.json

EXAMPLE USAGE OF FIFramework object:
>>> import fi_client as fi
>>>
>>> fi_client = fi.FIClient('./json_file_to_be_loaded.json')
>>>
>>> # Initialize the server, and transfer all framework files over. This can
>>> # take a lot of time.
>>> fi_client.setup_framework(install_server_deps=True)
>>> fi_client.start_docker_instances(execute_startup=True)
>>>
>>> # On which of the hosts has the data to be loaded? Choose with the index.
>>> # The fi_client.ensure_all_running() function could be used to wait till all
>>> # connections are available.
>>> fi_client.insert_data_and_verify(insert_data=True, host_index=0)
>>>
>>> # Now execute your defined fault scenario's.
>>> fi_client.run_test_scenarios(host_index=0, commit_image=True)

"""

import re
import sys
import json
import random
import time
import datetime
import uuid
import install_server_deps
from src import server_conn
from src.utils import print_json, load_json_file, get_time_from_str
from src.store_results_local import LocalDB
from threading import Thread


# Create the docker run commands given a db_type.
def get_docker_run_command(node_id, ip, main_ip, db_port, db_type, docker_image_id, connection,
                           create_backup=False, load_image=False, use_volume=True):
    node_name = '--name {}-node'.format(db_type)
    if create_backup:
        node_name = '--name {}-backup'.format(db_type)
    if load_image:
        node_name = ""

    run_cmd = None
    if db_type == "cassandra":
        # Create main command.
        base_cmd = "docker run {} -d -P -p {}:{} --net=host ".format(node_name, db_port, db_port)

        run_cmd = base_cmd + "-e CASSANDRA_BROADCAST_ADDRESS={} ".format(ip)
        if node_id != 0:  # Non main cassandra nodes have to listen to the main 'gossip' host.
            run_cmd += "-e CASSANDRA_SEEDS={} ".format(main_ip)
        if use_volume:
            main_dir = connection.get_user_dir()
            run_cmd += "-v {}fi-framework/db_data:/var/lib/cassandra ".format(main_dir)
        run_cmd += " {}".format(docker_image_id)

    return run_cmd


# Get the docker image id given an image name.
def get_docker_image_id(connection, image_name, db_version='latest'):
    (images, _) = connection.execute_cmd('docker images', sudo=True)
    docker_image_id = None
    for image in images.split('\n')[1:]:
        image = image.split()
        # Compare image name and db_tag to get the right image id.
        if len(image) >= 2 and\
           image[0] == image_name and image[1] == db_version:
            docker_image_id = image[2]
            break
    return docker_image_id


# Get the running docker container id given a image id and image name.
def get_docker_container_id(connection, image_id, image_name, db_version='latest'):
    db_tag = '{}:{}'.format(image_name, db_version)
    (processes, _) = connection.execute_cmd('docker ps', sudo=True)
    docker_container_id = None
    for process in processes.split('\n')[1:]:
        process = process.split()
        # On the first position most of the time the image_id is found, but sometimes
        # a string of the form: 'database:version' is found
        if len(process) >= 2 and (process[1] == image_id or process[1] == db_tag):
            docker_container_id = process[0]
            break
    return docker_container_id


# Thread function which injects the faults given an test_scenario configuration.
# Input dictionary containing the following information:
# - flips_per_file: the number of bit flips per file
# - num_files: the number of files targeted with bit flips.
# - start_delay_ms: a start delay in ms (optional)
# - delay_ms: delay between each bit flip fault injection (optional)
def start_fi_thread(fi_con, container_id, target_list, test_scenario, return_files, return_times):
    flips_per_file = test_scenario['flips_per_file']

    if len(target_list) == 0:
        print "No fault injection targets found."
        return

    if 'start_delay_ms' in test_scenario:
        time.sleep(test_scenario['start_delay_ms'] * 0.001)

    num_files_to_target = test_scenario['num_files']
    while num_files_to_target != 0:
        file_name = random.choice(target_list)
        try:
            (out, r) = fi_con.execute_cmd("docker exec {} python ".format(container_id) +
                                          "bit_flip.py {} {}".format(file_name, flips_per_file),
                                          sudo=True, print_output=False)
        except EOFError:
            continue
        if len(out) > 0 and "ValueError" in out[-1]:  # Target was empty, choose another file.
            continue
        return_times.append(datetime.datetime.utcnow().strftime('%H:%M:%S'))
        if 'delay_ms' in test_scenario:
            time.sleep(test_scenario['delay_ms'] * 0.001)
        return_files.append(file_name)
        num_files_to_target -= 1


class FIClient:
    # To be modified to the number of different database types implemented.
    implemented_db_types = ['cassandra']

    # Local result database name.
    local_database_name = 'fault_results_logtest'

    def __init__(self, parse_file):
        self.fi_file = parse_file
        self.fi_file_json = load_json_file(self.fi_file)
        self.n_nodes = 0
        self.ssh_connections = []
        self.hosts, self.users, self.passwords = [], [], []
        self.server_port = None

        self.image_ids, self.container_ids = [], []
        self.backup_image_ids, self.backup_container_ids = [], []

        # Parse the file to get the file with all example data.
        self.db_port = None
        self.db_version = self.fi_file_json['db_version']
        self.db_type = self.fi_file_json['db_type']
        if self.db_type not in self.implemented_db_types:
            print "Unknown DB type: {}".format(self.db_type)
            print "Implemented types: {}".format(self.implemented_db_types)
            sys.exit()

        if self.db_type == "cassandra":
            self.db_port = 7000

        # Initialize the connections from the server meta data.
        self._create_server_connections(self.fi_file_json['server_meta'])

    def __str__(self):
        info = '{}\n'.format(self)
        info += "== Read file info ==\n"
        info += "Name:", self.fi_file, "Contents:\n"
        info += print_json(self.fi_file_json)
        info += '\n'
        info += "== Server setup info ==\n"
        info += "N_nodes:", self.n_nodes, "Server port:", self.server_port, '\n'
        info += "Hosts:", self.hosts, "Users", self.users, '\n'
        info += "== Database info ==", '\n'
        info += "DB_type:", self.db_type, "DB_version:", self.db_version, "Db_port:", self.db_port, '\n'
        info += "== Docker info ==", '\n'
        info += "Image ids:", self.image_ids, "Container ids:", self.container_ids, '\n'
        info += "Backup image ids:", self.backup_image_ids, "Backup container ids:", self.backup_container_ids, '\n'
        return info

    # Get some server side info of one of the hosts.
    def get_host_info(self, host_index):
        if host_index >= self.n_nodes:
            print "Get host info: Invalid index given."
            return

        connection = self.ssh_connections[host_index]
        connect_host = self.hosts[host_index]
        connect_dir = connection.get_user_dir() + 'fi-framework/'
        return connection, connect_host, connect_dir

    # Create the server connections and initialize several other values retrieved
    # from the server meta data.
    def _create_server_connections(self, server_meta):
        self.n_nodes = int(server_meta['n_nodes'])
        self.server_port = server_meta['port'] if 'port' in server_meta else 22

        def list_from_server_meta(key):
            data = server_meta[key]
            return [data] * self.n_nodes if isinstance(data, unicode) else data

        self.hosts = list_from_server_meta('host')
        self.users = list_from_server_meta('user')

        if "password" in server_meta:
            self.passwords = list_from_server_meta('password')
        else:
            self.passwords = [None] * self.n_nodes

        for i in range(self.n_nodes):
            conn = server_conn.SSHConnection(self.hosts[i], port=self.server_port,
                                             user=self.users[i], password=self.passwords[i])
            self.ssh_connections.append(conn)

    # Setup the framework on the servers, and transfer all framework files over.
    def setup_framework(self, install_server_dependencies=False):
        for i in range(self.n_nodes):
            connection, connect_host, connect_dir = self.get_host_info(i)
            print "Setting up dependencies at {}@{}".format(self.users[i], connect_host)
            connection = self.ssh_connections[i]
            if install_server_dependencies:
                install_server_deps.install_dependencies(connection, self.db_type)

            # Transfer the framework files.
            connection.transfer_file("src/", transfer_location=connect_dir)

            # Transfer the query and main parameter files.
            connection.transfer_file(self.fi_file, transfer_location=connect_dir)
            connection.transfer_file(self.fi_file_json['query_file'], transfer_location=connect_dir)

    # Start the docker instances and retrieve their docker image and container ids.
    def start_docker_instances(self, execute_startup=False):
        docker_startup_cmds = self._setup_db_docker_cluster()

        self.image_ids, self.container_ids = [], []

        # When starting up each docker container, its id is returned.
        for startup_cmd, connection in zip(docker_startup_cmds, self.ssh_connections):
            if execute_startup:
                connection.execute_cmd(startup_cmd, sudo=True)

            # Retrieve image id from created docker instance.
            image_id = get_docker_image_id(connection, '{}-node'.format(self.db_type),
                                           self.db_version)
            if image_id is None:
                image_id = get_docker_image_id(connection, self.db_type, self.db_version)

            container_id = None
            # Check if a id is found.
            if image_id is None:
                print "No docker container or image created; something went wrong."
                print "Look at the above output of the executed startup command."
                print "Trying to continue."
            else:
                container_id = get_docker_container_id(connection, image_id, self.db_type,
                                                       self.db_version)

            self.image_ids.append(image_id)
            self.container_ids.append(container_id)

        return self.image_ids, self.container_ids

    # Create all the docker commands to create a cassandra cluster.
    def _setup_db_docker_cluster(self):
        cmds = [None] * self.n_nodes
        if self.db_type == "cassandra":
            db_ips = self.fi_file_json['db_meta']['connection_ip']
            main_ip = db_ips[0]

            cmds = []
            db_name = "cassandra:{}".format(self.db_version)
            for i in range(self.n_nodes):
                cmds.append(get_docker_run_command(i, db_ips[i], main_ip, self.db_port, self.db_type,
                                                   db_name, self.ssh_connections[i]))

        return cmds

    # Transfer and load a data set to one of the hosts when required following the
    # fi_file_json parameters.
    def insert_data_and_verify(self, insert_data, host_index=0):
        self.start_db_file_tracer(host_index)
        # Send the data set over to one of the servers.
        connection, _, connect_dir = self.get_host_info(host_index)

        # Transfer the json file and set the 'insert_data' field according
        # the input here.
        self._change_json_field_and_send('insert_data', insert_data, connection, connect_dir)

        # Optional insert a data set into the database.
        if insert_data:
            self.start_db_file_tracer(host_index)
            connection.transfer_file(self.fi_file_json['query_file'], connect_dir)
            file_to_send = self._get_test_dataset(print_info=True)
            self._transfer_dataset(connection, connect_dir, file_to_send)

        self._load_dataset_and_verify(host_index)

    def _change_json_field_and_send(self, field, value, connection, connect_dir):
        temp_file = 'temp.json'
        with open(temp_file, 'w+') as f:
            self.fi_file_json[field] = value
            json.dump(self.fi_file_json, f)
        connection.transfer_file(temp_file, connect_dir)
        connection.execute_cmd('mv fi-framework/{} fi-framework/{}'.format(temp_file, self.fi_file))

    def _get_test_dataset(self, print_info=False):
        data_to_insert = self.fi_file_json['data_to_insert']
        if print_info:
            print "data_to_insert:", data_to_insert

        if 'files' in data_to_insert:
            file_to_send = data_to_insert['files']
        elif 'csv' in data_to_insert:
            file_to_send = data_to_insert['csv']
        else:
            print 'No valid test dataset given!'
            print 'The options are binary files or a csv file.'
            return
        return file_to_send

    @staticmethod
    def _transfer_dataset(connection, connect_dir, file_to_send):
        (res, _) = connection.execute_cmd('file {}'.format(connect_dir + file_to_send))
        if '(No such file or directory)' in res:  # File or directory does not exists yet.
            connection.transfer_file(file_to_send, connect_dir)
        else:
            replace_dataset = raw_input("File/Dir does already exists, replace? [y/n]: ")
            if replace_dataset == 'y':
                connection.execute_cmd('rm -rf {}'.format(file_to_send))
                connection.transfer_file(file_to_send, connect_dir)

    # Execute loading a data set on the test host, and fill the verification database.
    def _load_dataset_and_verify(self, host_index=0):
        query_cmd = self._get_db_querying_cmd() + " {} '{}'".format(host_index, json.dumps({'type': 'verify'}))
        (query_results, _) = self.ssh_connections[host_index].execute_cmd(query_cmd, sudo=True)
        return query_results

    # Return the query server cmd which is used to execute the test and verifications
    # calls defined in the db_server_querying.py file.
    def _get_db_querying_cmd(self):
        return 'python fi-framework/src/db_server_querying.py fi-framework/{} '.format(self.fi_file)

    # Run the test scenario's as defined in the fi-file.
    def run_test_scenarios(self, host_index=0, commit_image=True):
        start = time.time()
        connection, _, connect_dir = self.get_host_info(host_index)
        self.backup_image_ids, self.backup_container_ids = [], []

        if commit_image:
            # Commit and backup all containers, the host_index variable only is used
            # to indicate the main fault injection host.
            print "Preparing FI host: {}".format(self.hosts[host_index])
            self._prepare_fi_host(self.container_ids[host_index], host_index)
            for i in range(self.n_nodes):
                self.commit_image(i)

        else:  # Obtain the backup ids from the running backups.
            image_name = '{}-backup'.format(self.db_type)
            for i in range(self.n_nodes):
                temp_connection = self.ssh_connections[i]
                backup_image_id = get_docker_image_id(temp_connection, image_name, db_version='1.0')
                backup_container_id = get_docker_container_id(temp_connection, backup_image_id, image_name, '1.0')
                self.backup_image_ids.append(backup_image_id)
                self.backup_container_ids.append(backup_container_id)
                self._prepare_fi_host(backup_container_id, host_index=i)

        if len(self.backup_container_ids) == 0 or None in self.backup_container_ids:
            print "=== Something went wrong with retrieving backup docker ids of DBMS cluster. ==="
            return

        # Ensure all cassandra instances can be queried.
        if not self.ensure_all_running():
            print "=== Timeout on waiting on database connections. ==="
            return

        # Create query_cmd.
        query_cmd = self._get_db_querying_cmd()
        test_cmd = {'type': 'test'}
        test_scenarios = self.fi_file_json['test_scenarios']
        test_repetitions = test_scenarios['repetitions']
        test_cmd['data_type'] = test_scenarios['data_type']
        query_cmd += " {} '{}'".format(host_index, json.dumps(test_cmd))

        self.setup_framework(False)
        self.start_db_file_tracer(host_index)

        # Run all test scenarios a number of repetitions times. When the scenario is running,
        # a fault injector thread is started as the database is queried again. The server will
        # verify the results from the initialized mysql database.
        #
        # Afterwards all experiment results are saved in the local MongoDB database. Next the
        # database docker image and the database volume is restored again. Up till all
        # repetitions are finished.
        print "=== Starting {} test scenarios ===".format(len(test_scenarios['scenarios']))
        for scenario_id in range(len(test_scenarios['scenarios'])):
            result_uuid = uuid.uuid4()
            print "=== Scenario run id: {} ===".format(result_uuid)
            cur_container_id = self.backup_container_ids[host_index]
            target_list = self._get_possible_targets(scenario_id, connection, cur_container_id, host_index)
            test_scenario = test_scenarios['scenarios'][scenario_id]
            for run_id in range(test_repetitions):
                print "=== Starting run: {}/{} ===".format(run_id + 1, test_repetitions)
                print "=== Ensuring all {}:{} instances are running ===".format(self.db_type, self.db_version)
                if not self.ensure_all_running():
                    print "=== Timeout on waiting on database reconnection. ==="
                    return

                cur_container_id = self.backup_container_ids[host_index]
                targeted_files = []
                injection_times = []
                fi_thread = Thread(target=start_fi_thread,
                                   args=(connection, cur_container_id, target_list, test_scenario,
                                         targeted_files, injection_times,))

                print "=== Starting the fault injector and db queries ==="
                # Return streams, so that the io is asynchronous.
                (stdin, stdout, stderr) = connection.execute_cmd(query_cmd, return_streams=True)
                fi_thread.start()
                out = stdout.readlines()
                fi_thread.join()
                server_results = []
                try:
                    server_results = json.loads(out[-1].strip("\r\n").strip("'"))
                except ValueError:
                    print out
                stdin.close(), stderr.close(), stdout.close()
                time.sleep(5.0)
                (logs, _) = connection.execute_cmd("docker logs {}".format(cur_container_id),
                                                   sudo=True, print_output=False)
                result_assemble_thread = Thread(target=self._assemble_results_thread,
                                                args=(test_scenario, logs, server_results, targeted_files,
                                                      injection_times, result_uuid, run_id,))
                result_assemble_thread.start()
                print "=== Finished run, restoring everything ==="

                db_ips = self.fi_file_json['db_meta']['connection_ip']
                main_ip = db_ips[host_index]
                for i in range(self.n_nodes):
                    sec_connection = self.ssh_connections[i]
                    backup_id = self.backup_container_ids[i]
                    sec_connection.execute_cmd('docker stop {}'.format(backup_id), sudo=True)
                    sec_connection.execute_cmd('docker rm {}'.format(backup_id), sudo=True)
                    self._restore_tar_backup(host_index=i)

                    # Restore docker by stopping and running command again with all data.
                    run_cmd = get_docker_run_command(i, db_ips[i], main_ip, self.db_port,
                                                     self.db_type, self.backup_image_ids[i], sec_connection,
                                                     load_image=True)

                    (container_id, _) = sec_connection.execute_cmd(run_cmd, sudo=True)
                    self.backup_container_ids[i] = container_id[:12]
                result_assemble_thread.join()
                self.start_db_file_tracer(host_index)
        print "=== Finished scenarios ==="
        print "Took: {} seconds".format(time.time() - start)

    def _assemble_results_thread(self, test_scenario, logs, server_results,
                                 targeted_files, injection_times, result_uuid, run_id):
        local_result_db = LocalDB(self.local_database_name)

        # Insert the results from the scenario run.
        def insert_scenario_result(run_res_id, res_id, test_scenario_data, server_result):
            db_meta = self.fi_file_json['db_meta']
            dataset = self._get_test_dataset()
            local_result_db.insert_fi_result(self.n_nodes, self.db_type, self.db_version,
                                             db_meta, dataset, test_scenario_data, server_result,
                                             run_res_id, res_id)

        db_logs = self._get_log_results(logs, server_results)
        test_scenario['target_files'] = targeted_files
        test_scenario['injection_times'] = injection_times
        test_scenario['db_error_logs'] = db_logs
        insert_scenario_result(run_id, result_uuid, test_scenario, server_results)

    # Retrieve the logging results of the databases.
    def _get_log_results(self, logs, query_effects, interval=3):
        errors = []
        if self.db_type == "cassandra":

            delta_time_pairs = []
            # A list is received when it was not possible to retrieve any
            # results from the DBMS.
            if isinstance(query_effects, dict):
                for query_res in query_effects.values():
                    # Check if there is an error with this query, if not the interval is
                    # not really interesting to search for warnings.
                    # if sum([val for val in query_res.values() if isinstance(val, int)]) == 0:
                    #     continue
                    timestamp = get_time_from_str(query_res['timestamp'], return_list=True)
                    if len(timestamp) > 3:
                        timestamp = datetime.datetime(1, 1, 1, timestamp[0], timestamp[1], timestamp[2], timestamp[3])
                    else:
                        timestamp = datetime.datetime(1, 1, 1, timestamp[0], timestamp[1], timestamp[2])
                    delta_time_pairs.append([(timestamp - datetime.timedelta(seconds=interval)).time(),
                                             (timestamp + datetime.timedelta(seconds=interval)).time()])

            # Expression to analyse the Cassandra database logs:
            # WARN  10:19:29  the log message.
            # It extracts the timestamp and warning.
            log_regex = re.compile('(WARN|ERROR|FATAL)\s*([0-9]{2}:[0-9]{2}:[0-9]{2})')
            log_starts = re.compile('WARN|ERROR|FATAL|INFO')

            for i in range(len(logs)):
                result = log_regex.match(logs[i])

                if result is not None:
                    message, log_time = result.groups()
                    # All fatal or error logs are retrieved.
                    if message in ['ERROR', 'FATAL']:
                        errors.append(logs[i].strip())
                    else:
                        # Extract only relevant warning logs.
                        log_time = get_time_from_str(log_time)
                        for time_min, time_max in delta_time_pairs:
                            if time_min <= log_time <= time_max:
                                errors.append(logs[i].strip())
                # Check if a new log is starting or maybe the current log is continuing.
                # This can happen with stack traces which are split on multi lines.
                elif log_starts.match(logs[i]) is None and len(errors) > 0:
                    # Just append to the last string all other additional information.
                    errors[-1] += logs[i]

        return errors

    def _restore_tar_backup(self, host_index=0):
        restore_cmd = {
            "type": "restore",
            "backup": "fi-framework/backup.tar.gz",
            "data": "fi-framework/db_data"}
        restore_cmd = self._get_db_querying_cmd() + " {} '{}'".format(host_index, json.dumps(restore_cmd))
        self.ssh_connections[host_index].execute_cmd(restore_cmd, sudo=True, print_output=False)

    def _remove_tar_backup(self, host_index):
        self.ssh_connections[host_index].execute_cmd('rm -rf fi-framework/db_data', sudo=True)

    # Get the target lists via the db_server_querying server function.
    def _get_possible_targets(self, scenario_id, connection, container_id, host_index=0):
        scenario = self.fi_file_json['test_scenarios']['scenarios'][scenario_id]
        if 'target_file_list' in scenario:
            return scenario['target_file_list']
        run_params = {'type': 'retrieve_targets', 'container_id': container_id, 'scenario_id': scenario_id}
        query_cmd = self._get_db_querying_cmd() + " {} '{}'".format(host_index, json.dumps(run_params))
        (targets, _) = connection.execute_cmd(query_cmd, print_output=False)
        return [target.strip('\r\n') for target in targets]

    # Automatically used by the run test scenario function. Prepare the host where
    # fault injections will occur by transferring the json files and copying the
    # fault injector file into the docker container.
    def _prepare_fi_host(self, container_id, host_index=0):
        connection, _, connect_dir = self.get_host_info(host_index)

        connection.execute_cmd('docker cp {}src/faults/bit_flip.py'.format(connect_dir) +
                               ' {}:/bit_flip.py'.format(container_id), sudo=True)

    # Commit the images used in the docker tests. This will be automatically executed before
    # a fault injection test is done.
    def commit_image(self, host_index=0):
        backup_name = '{}-backup'.format(self.db_type)

        db_ips = self.fi_file_json['db_meta']['connection_ip']
        main_ip = db_ips[host_index]

        for node_id in range(self.n_nodes):
            connection = self.ssh_connections[node_id]

            # Check if a backup already exists, if it does do nothing.
            backup_image_id = get_docker_image_id(connection, backup_name, db_version='1.0')
            if not backup_image_id:
                # Create a backup image and archive of the mounted volume.
                # When doing so, get the backup image id and stop the current running db image.
                # Afterwards the backup image is started.
                backup_image_id = self._create_backup_image(node_id, connection, backup_name)
                run_cmd = get_docker_run_command(node_id, db_ips[node_id], main_ip, self.db_port,
                                                 self.db_type, backup_image_id, connection,
                                                 create_backup=True)
                (container_id, _) = connection.execute_cmd(run_cmd, sudo=True)
                backup_container_id = container_id[:12]
            else:
                backup_container_id = get_docker_container_id(connection, backup_image_id,
                                                              self.db_type, self.db_version)

            self.backup_image_ids.append(backup_image_id)
            self.backup_container_ids.append(backup_container_id)

    # Commit an image to create a backup and create an archive of all data files.
    def _create_backup_image(self, node_id, connection, backup_name):
        if self.db_type == 'cassandra':
            # Flush all data from memory to disk with the cassandra nodetool.
            connection.execute_cmd('docker exec {} nodetool flush'.format(self.container_ids[node_id]),
                                   sudo=True)

        (backup_image_id, _) = connection.execute_cmd('docker commit {} '.format(self.container_ids[node_id]) +
                                                      '{}:1.0'.format(backup_name), sudo=True)

        # Stop current image so tar of the db_data directory can be made.
        connection.execute_cmd('docker stop {}'.format(self.container_ids[node_id]), sudo=True)
        connection.execute_cmd('docker rm {}'.format(self.container_ids[node_id]), sudo=True)
        connection.execute_cmd('tar -czf fi-framework/backup.tar.gz fi-framework/db_data')

        return backup_image_id.split(':')[1][:12]

    # Wait till each host docker instance is up and running.
    def ensure_all_running(self):
        for connection, host in zip(self.ssh_connections, self.hosts):
            (out, _) = connection.execute_cmd('python fi-framework/src/databases/' +
                                              '{}/db_functions.py '.format(self.db_type))
            for line in out:
                if "ERROR" in line:
                    return False
        return True

    # Start the strace and lsof combination process to track all opened files.
    def start_db_file_tracer(self, host_index=0):
        connection, _, connection_dir = self.get_host_info(host_index)
        connection.execute_cmd(self._get_strace_cmd(connection_dir) + ' {} start'.format(self.db_type),
                               return_streams=True, sudo=True)

    # List all files opened files of the database in the docker container.
    def get_db_file_tracer_results(self, host_index=0):
        connection, _, connection_dir = self.get_host_info(host_index)
        (out, _) = connection.execute_cmd(self._get_strace_cmd(connection_dir) + ' {} list'.format(self.db_type),
                                          print_output=False, sudo=True)
        return [row.strip() for row in out]

    # Empty the list of files opened by the db while querying the database.
    def empty_db_file_tracer_results(self, host_index=0):
        connection, _, connection_dir = self.get_host_info(host_index)
        connection.execute_cmd(self._get_strace_cmd(connection_dir) + ' {} empty'.format(self.db_type),
                               return_streams=True, sudo=True)

    # Kill the strace process running on one of the hosts.
    def kill_db_file_tracer(self, host_index=0):
        connection, _, connection_dir = self.get_host_info(host_index)
        connection.execute_cmd(self._get_strace_cmd(connection_dir) + ' {} kill'.format(self.db_type),
                               return_streams=True, sudo=True)

    @staticmethod
    def _get_strace_cmd(connection_dir):
        return 'python {}src/attach_strace.py'.format(connection_dir)

    # Query one of the servers.
    def query_db_host(self, query, host_index, db_data_type=None, timeout=None):
        connection, _, connection_dir = self.get_host_info(host_index)
        cmd = {'type': 'query', 'query': query}
        if db_data_type is not None:
            cmd['db_type'] = db_data_type
        if timeout is not None:
            cmd['timeout'] = None

        query_cmd = self._get_db_querying_cmd() + " {} '{}'".format(host_index, json.dumps(cmd))
        (out, _) = connection.execute_cmd(query_cmd)
        return out

    # Inject faults given a file and number of bit flips. This could be done at the server side
    # actually; in the db_server_querying file called by the run_test_scenarios() method of
    # this class. This is more of a simple test function.
    def inject_fault(self, image_name, version, file_name, n_flips, host_index=0):
        connection, _, connection_dir = self.get_host_info(host_index)
        image_id = get_docker_image_id(connection, image_name, db_version=version)
        container_id = get_docker_container_id(connection, image_id, image_name, version)
        connection.execute_cmd('docker exec {} python bit_flip.py {} {}'.format(container_id, file_name, n_flips),
                               sudo=True)


if __name__ == '__main__':
    if len(sys.argv[1:]) != 1:
        print 'Give a test scenario\'s file in the form provided in the examples map.'
    else:
        framework_obj = FIClient(sys.argv[1])
        framework_obj.setup_framework(install_server_dependencies=False)
        framework_obj.start_docker_instances(execute_startup=False)

        test_host_index = 0
        framework_obj.insert_data_and_verify(True, host_index=test_host_index)
        framework_obj.run_test_scenarios(host_index=test_host_index, commit_image=False)
