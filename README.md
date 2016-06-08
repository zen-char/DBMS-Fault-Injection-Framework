# Fault injection framework for testing DBMS

This project implements a fault injection framework, modular by being able to
be used for other Database Management Systems (DBMS). The intial implementation
details the distributed Apache Cassandra DBMS. The framework details a wrapper of
the paramiko SSH connection module, used to test the DBMS on a server environment. 

The test environment is isolated by using Docker. This has the consequence the
DBMS under test has to be provided within a Docker image, with basic functions
to obtain the DBMS logs.

The framework is now mainly designed to only support file datasets to be inserted
and verified on errors. The main focus of the implementation project was the 
to analyse the data integrity of a DBMS under test.

The framework was build as part of my Bachelor Thesis at the University of Amsterdam.
It is sometimes a bit limited in function because of a tight deadline, and is
maybe extended when a bit of time is found.

## What does it do exactly?
-------------------
The framework will transfer the framework files using the SSH connections (defined
in src/ssh_comm.py). Docker instances are started and a data set is inserted into
the DBMS. Meanwhile the data is inserted into the DBMS, a SQLite verification database
is created at the server side, containing the query id, file name and MD5 hash of 
the file. 

When a test scenario is started, a backup is created first. On the Docker image
under test the fault injection script is transfered to a running Docker container
and committed as a backup image. The mounted volume created with the Docker run
command is also archived. A potentional file target list is retrieved, created
when the DBMS was queried using the `src/attach_strace.py` script. The script
creates a list of files at the server side used by the DBMS, which are the only
interesting target files. Now, the test scenario can start.

A thread is started to start the fault injections and via the SSH connection
the database server is queried server side. The query method also performs
the verifications using the DBMS. When the thread is finished along with the
server verifications, the DBMS logs are retrieved and a thread is started to
assemble the results. Meanwhile, the docker container is stopped, removed and
a backup of the volume is restored via comparing file hashes. Then, the Docker
backup is started again repeated for each server connection. When the result
assembling is finished, it is stored in a MongoDB client running locally, as
unstructured data has to be stored. The next repetition or scenarios can then
be performed.

### Design overview

An overview of the design tried to be implemented is shown below.

<img src="https://gitlab-fnwi.uva.nl/10552073/scriptie-framework/raw/master/doc/FrameworkDesign.png" width="600">

The arrows are directing the communication flows between the components. 

## Requirements
---------------
The framework now simply assumes the servers to connect with runs a version of
Ubuntu. Thus installs all dependencies via the apt-get command. The client can
be of any operating system, as long the paramiko library is installed.

The MongoDB database is used to store all fault injection results locally, so it
has to be installed along with the pymongo module. 

## A test definition
--------------------
The framework makes use of JSON to parse fault injection test configurations.
The example below features the paramters and a short description of them.

```python
{
  "server_meta" :
  {
    "n_nodes" : 1, # Number of nodes to configure
    "user" : "username",
    "password" : "password", # Optional - can also be a list."
    "host" : # List of hosts to create SSH connections with.
    [
      "127.0.0.1", "127.0.0.3", "..."
    ],
    "port" : 22 # Server listening port - can also be a list."
  },
  "db_type" : "cassandra",
  "db_version" : "3.5",
  "db_meta" : # - DBMS configuration data.
  {
    "connection_ip": ["Private_IP1", "Private_IP2", "..."],
    "init" : # Paramaters used when the DBMS is still empty and data is inserted.
    {
      "class" : "SimpleStrategy",
      "replication_factor" : 3
    },
    "reuse_keyspace" : true,
    "keyspace" : "files"
  },
  "test_scenarios" : {
    "repetitions": 100, # Repetition of a single scenario.
    "data_type": "files", # Files are only supported for now."
    "scenarios":
    [
      {
        "FI_type": "bitflip",
        "num_files": 1,
        "target_list": "# Optional - These files will only be used, by each scenario.",
        "excluded_extensions":
        [
          "# Ignore the following files when creating a target file list.",
          ".log",
          ".jar",
          ".java",
          ".log.0.current",
          ".so"
        ],
        "exclude_containing": 
        [
          "# Ignore substrings containing",
          "/jvm/"
        ],
        "flips_per_file": 1,
        "start_delay_ms": 100, # Delay in ms before starting fault injections,
        "delay_ms": 100 # Delay interval in ms between fault injections
      }
    ]
  },
  "data_to_insert" :
  { 
     # Key argument: type of data to insert, value: path to data
    "files": "datasets/complete_ms_data/"
  },
  "query_file": "queries.json"
}
```

The query file:

```json
{
  "queries":
    [
      "SELECT * FROM a_table WHERE file_id == 10 LIMIT 1;",
      "Another query.."
    ]
}
```

Note, JSON does not support comments or multi-line strings. Keep that in mind
when designing your tests!

## Quick start of a experiment
------------------------------
To run an experiment the following steps have to be performed.
1. First parse the configuration file and create a new framework object.
2. Setup the framework files.
3. Start the docker images on the server, configured by the JSON file.
4. Insert a data set, the Multi-spectra image data set was used in the experiment
   while this framework was created.
5. Run the test framework. 

In code this would be:

```python
import fi_client as fi

fi_client = fi.FIClient('./json_file_to_be_loaded.json')

# Initialize the server, and transfer all framework files over. This can
# take a lot of time.
fi_client.setup_framework(install_server_deps=True)
fi_client.start_docker_instances(execute_startup=True)

# On which of the hosts has the data to be loaded? Choose with the index.
# The fi_client.ensure_all_running() function could be used to wait till all
# DBMS sessions are available.
fi_client.insert_data_and_verify(insert_data=True, host_index=0)

# Now execute your defined fault scenario's.
fi_client.run_test_scenarios(host_index=0, commit_image=True)
```

The distributed test environment used while testing the framework was set up
using a Google Cloud instances and should work with other environments. The
framework can also be used when, e.g. a virtualbox environment is used. 

It is also recommended to run the test in an interactive python environment
such as iPython. New configuration files can be loaded by creating a new 
FIClient object. 

## Example output
-----------------
An (shortened) output of the framework would be as stored in MongoDB: 
```json
{
  "db_meta":
   {
    "reuse_keyspace": true,
    "connection_ip": ["PrivateIP"],
    "init": {"class": "SimpleStrategy", "replication_factor": 1}, 
    "keyspace": "files"
  },
  "server_params": {"n_nodes": 1},
  "db_type": "cassandra",
  "db_version": "3.5",
  "test_scenario":
   {
    "start_delay_ms": 0,
    "delay_ms": 0,
    "excluded_extensions":
      [".log", ".jar", ".java", ".log.0.current", ".so", ".yaml"],
    "FI_type": "bitflip",
    "injection_times": ["17:57:36"],
    "num_files": 1,
    "flips_per_file": 1,
    "exclude_containing": ["/jvm/"],
    "target_files": ["/var/lib/cassandra/data/files/.../ma-1-big-Data.db"],
    "db_error_logs": ["WARN  17:57:39 Exception..."]
  },
  "run_id": 1,
  "tested_dataset": "datasets/complete_ms_data/",
  "effects":
  {
    "0": {"duplicates": 0, "timestamp": "17:57:42.842471", "verification_errors": 0, "results_missing": 0},
    "1": {"duplicates": 0, "timestamp": "17:57:42.646548", "verification_errors": 0, "results_missing": 0},
    "2": {"duplicates": 0, "timestamp": "17:57:42.842062", "verification_errors": 0, "read_failure": 1, "results_missing": -33
  },
  "res_id": "d105e0db-a071-4f68-a0b3-424ebd09b5cc"
}
```
The read failure is a server error. So the exception is caught in this situation
and no invalid data is returned to the 'client'. 

The result can be used to create a small summary using the `analyze_results.py`
python script.

```text
time_diff_fault_time_and_injections : ['00:00:02.842062']
error_sum : 32 
injection_times : [u'17:57:36']
fault_time : [u'17:57:42.842062']
flips_per_file : 1
results_missing : -31
verification_errors : 0
read_failure : 1
db_error_logs - num logs: 1 - values : [u'Exception ..']
targets : [u'/var/lib/cassandra/data/files/.../ma-1-big-Data.db']
time_diff_last_injection_log_error : 00:00:01
```

The analyze results script is not really flexible, but should provide a small
summary how well everything worked per fault. A total amount of results is found
as the last entry printed by the script.

## Todo
-------
As this is my first framework ever designed and implemented, a number of todos 
are still to be solved. 

- Try to make the addition of a DBMS cleaner via means of inheritance.
- Extend modularity when the Docker Image does not provide all commands (such as the logs).
- Extend the number of fault injections techniques and test configurations.
- Extend that the injection threads could run on multiple DBMS systems.
- Make the server dependencies installation script Linux distribution independent.
- Change print statements such that a logging module is used.
- Look at python 3.0 compatibility (would probably solved by previous todo).
- Split 'out of order results' and 'checksum mismatches'.
