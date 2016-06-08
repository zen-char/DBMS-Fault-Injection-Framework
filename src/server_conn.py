"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

This file implements a small wrapper to easily transfer files using the paramiko
library. The files are first zipped with bzip2 compression before send over.
The files are immediately unzipped after the transfer.

FILE: server_conn.py

USAGE:

    # Example usage how to use this wrapper:
    from server_conn import SSHConnection
    conn = SSHConnection(host, port=22, user=name, password=pass)

    (stdout, stderr) = conn.exec_command('command', sudo=False, return_streams=False,
                                         print_output=True)

    conn.transfer_file('dir/file', 'dir location relative from home on server')

"""

from paramiko import SSHClient
from paramiko.client import AutoAddPolicy
import os
import tarfile
from utils import color_str


def create_archive(directory, name='./temp.tar.bz2'):
    tar = tarfile.open(name, mode='w:bz2')
    if not os.path.isfile(directory):
        tar.type = tarfile.DIRTYPE
    tar.add(directory)
    tar.close()
    return name


class SSHConnection:
    def __init__(self, host, port=22, user=None, password=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.client = self.connect_client()

    # Connect to a server.
    def connect_client(self):
        client = SSHClient()
        client.set_missing_host_key_policy(AutoAddPolicy())
        client.connect(self.host, port=self.port, username=self.user, password=self.password)
        return client

    # Execute a command on the server. Sudo command referenced from:
    # https://stackoverflow.com/questions/22587855/
    def execute_cmd(self, command, sudo=False, print_output=True, return_streams=False, debug=True):
        if sudo:
            command = "sudo -S -p '' {}".format(command)

        if debug:
            print "{}: {}".format(color_str('[Executing]', color='y'), command)
        stdin, stdout, stderr = self.client.exec_command(command, get_pty=True)

        if sudo and self.password is not None:
            stdin.write(self.password + '\n')
            stdin.flush()

            # The password and a new line are echo'ed when a Sudo command is executed.
            stdout.readline()
            stdout.readline()

        if return_streams:
            return stdin, stdout, stderr
        elif not print_output:
            return stdout.readlines(), stderr.readlines()

        out = ""
        for line in stdout:
            if out == "":
                print "Output:"
            out += line
            print line,

        error = ""
        for line in stderr:
            if out == "":
                print "Errors:"
            error += line
            print line,
        return out, error

    # Get user home directory from current ssh system.
    def get_user_dir(self):
        return '/home/' + self.user + '/'

    # Unarchive the compressed file on the server.
    # Ensure the files or extracted in the target directory, as all commands
    # are executed relative from the home directory. Which is why the location
    # has also to be given as parameter.
    def unarchive_on_server(self, file_name, location):
        self.execute_cmd('tar -xf {} -C {}'.format(file_name, location))
        self.execute_cmd('rm {}'.format(file_name))

    # Transfer a file or directory to the server, will transfer to
    # the home directory when no location is specified. The files
    # or the directory is compressed and decompressed when send.
    def transfer_file(self, transfer_file, transfer_location=None):

        transfer_file = transfer_file.strip()  # Remove redundant spaces
        if os.path.isdir(transfer_file):
            print "{}: '{}'".format(color_str('[Sending directory]', color='y'), transfer_file)
        else:
            print "{}: '{}' of size: {} bytes".format(color_str('[Sending file]', color='y'),
                                                      transfer_file, os.path.getsize(transfer_file))

        # When transfer location is None, use home directory.
        if transfer_location is None:
            transfer_location = self.get_user_dir()
        else:
            # Ensure transfer directory exists
            self.execute_cmd("mkdir -p {}".format(transfer_location))

        # Create archive to be send.
        temp_name = create_archive(transfer_file)

        sftp = self.client.open_sftp()
        sftp.put(temp_name, self.get_user_dir() + os.sep + temp_name)
        os.remove(temp_name)

        self.unarchive_on_server(temp_name, transfer_location)
        sftp.close()

    # Close client connection properly.
    def close_connection(self):
        self.client.close()

if __name__ == '__main__':
    u_host = '127.0.0.1'
    u_port = 22
    u_user = 'user'
    u_password = 'password'
    obj = SSHConnection(u_host, u_port, u_user, password=u_password)
    print 'testing ls:', obj.execute_cmd('ls', sudo=True)
