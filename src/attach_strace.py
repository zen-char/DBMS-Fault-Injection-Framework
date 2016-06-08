"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

This file attaches a strace process on the cassandra process to track
which files it is reading or writing to while querying. Will for now write output
to a file.

NOTE: this file has to be run with sudo rights. And ensure no other strace cmd
      is running.

FILE: attach_strace.py

USAGE: python attach_strace.py <db_type> <start/kill/empty/list>
"""

import io
import re
import os
import sys
import time
import subprocess


# Using a grep command and ps -aux output, get all output from the grep command.
def search_process_pid(grep_param):
    ps = subprocess.Popen('ps -aux'.split(' '), stdout=subprocess.PIPE)
    grep_cmd = ["grep", "{}".format(grep_param)]
    grep = subprocess.Popen(grep_cmd, stdin=ps.stdout, stdout=subprocess.PIPE)
    ps.stdout.close()
    output = grep.communicate()[0]
    ps.wait()
    return output


# Start the strace process
def start_strace(strace_cmd):
    return subprocess.Popen(strace_cmd.split(), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)


# Needed since the terminate call does not always terminate all processes correctly.
# Moreover, only a single strace process may be ran on a machine.
def kill_strace(strace_cmd):
    output = search_process_pid("{}".format(strace_cmd))
    strace_proc = [out for out in output.split('\n') if out.endswith(strace_cmd)]

    print strace_proc
    if len(strace_proc) > 1:
        print "Killing strace.."
        pid = strace_proc[0].split()[1]
        kill_cmd = ["kill", str(pid)]
        kill = subprocess.Popen(kill_cmd)
        kill.wait()


# Attach, kill, list results of a strace process.
def attach_strace(db_type, command_option='start', db_files='opened_files.txt'):
    if command_option == 'list':
        with io.open(db_files, 'a+') as f:
            f.seek(0)
            for line in f:
                print line,
        return
    elif command_option == 'empty':
        os.remove(db_files)
        return

    pid = None
    if db_type == 'cassandra':
        pid = get_cassandra_pid()
    if pid is None:
        print "No {} process found, or not implemented.".format(db_type)
        return

    strace_cmd = 'strace -p {} -f -e trace=write,read,open'.format(pid)
    if command_option == 'kill':
        kill_strace(strace_cmd)
        return

    strace = start_strace(strace_cmd)
    opened_files = []
    with io.open(db_files, 'a+') as file_list:
        file_list.seek(0)
        opened_files.append(file_list.readline().strip())
    analyse_output(strace.stderr, pid, opened_files, db_files)

    strace.stdout.close()
    strace.stderr.close()


def get_cassandra_pid():
    # Search the cassandra pid.
    output = search_process_pid('cassandra')
    cassandra_process = [out for out in output.split('\n') if out.endswith('cassandra.service.CassandraDaemon')]

    cassandra_pid = None
    if len(cassandra_process) == 1:
        cassandra_pid = cassandra_process[0].split()[1]
        print "Found cassandra process with pid:", cassandra_pid
    else:
        print "No cassandra process found"
    return cassandra_pid


# Use a subprocess to get all opened files by a process id.
# psutil could be used, but does not work with super user rights; and does
# not give all opened files when the process does not give permission.
def search_file_descriptor_file(process_pid, fd):
    lsof = subprocess.Popen("lsof -p {}".format(process_pid).split(),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # First line can be skipped, which contain column names.
    lsof.stdout.readline()
    for process in lsof.stdout:
        process = process.split()
        if len(process) > 0 and process[3][:-1] == fd:
            lsof.stdout.close()
            lsof.stderr.close()
            # On last location of a line the file name is found.
            return process[-1].strip()

    lsof.stdout.close()
    lsof.stderr.close()


# Parse the open, read and write commands retrieved from the strace pipe output.
def analyse_output(input_file, process_pid, opened_files, db_files):
    open_pattern = re.compile('open\(.*\) = ([0-9]*)')
    read_pattern = re.compile('read\(([0-9]*)')
    write_pattern = re.compile('write\(([0-9]*)')

    file_descriptors = []
    no_input_counter = 0

    def find_descriptor(result):
        # Retrieve file descriptor anSd check if is a new one.
        descriptor = result.groups()[0]
        if descriptor not in file_descriptors:
            # Now find corresponding file and append to file list used by a process.
            file_descriptors.append(descriptor)
            fd = search_file_descriptor_file(process_pid, descriptor)
            if fd and fd not in opened_files:
                opened_files.append(fd.strip())
                with io.open(db_files, 'a+') as f:
                    f.write(fd.strip() + unicode('\n'))
                print fd, file_descriptors

    while True:
        line = input_file.readline()
        if line == '':
            time.sleep(0.1)
            no_input_counter += 1
        else:
            no_input_counter = 0
            res = read_pattern.search(line)
            if res:
                find_descriptor(res)
                continue

            res = open_pattern.search(line)
            if res:
                find_descriptor(res)
                continue

            # Mostly certain a write call is found.
            res = write_pattern.search(line)
            if res:
                find_descriptor(res)
        # When no input is retrieved for 10 secs, the while loop can be stopped.
        # As it is unlikely to retrieve any information.
        if no_input_counter == 100:
            break

if __name__ == '__main__':
    # To be called as: python attach_strace.py db_type kill/start/list/empty
    attach_strace(db_type=sys.argv[1], command_option=sys.argv[2])
