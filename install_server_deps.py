"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

Install the server dependencies to get e.g. docker running.

FILE: install_server_deps.py

USAGE:
python install_server_deps.py <host> -port port -user user -password password
where port, user and password are optional.

"""

import argparse
from src import server_conn


def install_docker(ssh, db_type):
    print "=== START Installing prerequisites packages for docker ==="

    print "Installing transport certificates"
    ssh.execute_cmd("apt-get -y --force-yes install apt-transport-https ca-certificates", sudo=True)

    print "Installing curl.."
    ssh.execute_cmd("apt-get -y --force-yes install curl", sudo=True)

    print "=== END installing prerequisites for docker ==="

    # Check if docker is already installed..
    res = ssh.execute_cmd("docker", sudo=True)
    if 'sudo: docker: command not found' in res[0]:  # Check stderr to see if the command exists.
        print "Installing docker.. (this may take a while)"
        ssh.execute_cmd("curl -fsSL https://get.docker.com/ | sh", sudo=True)

    if db_type == "cassandra":
        ssh.execute_cmd("docker pull cassandra", sudo=True)


def install_python_deps(ssh, db_type):
    print "=== START Installing python dependencies ==="
    ssh.execute_cmd("apt-get -y --force-yes install python-pip", sudo=True)

    if db_type == "cassandra":
        ssh.execute_cmd("apt-get -y --force-yes install build-essential python-dev", sudo=True)
        ssh.execute_cmd("apt-get -y --force-yes install libffi6 libffi-dev", sudo=True)

        # The cassandra package is slow with installing, and does not check if it is
        # already installed. So use a small file which checks a import error.
        ssh.transfer_file("check_cassandra.py")
        (res1, res2) = ssh.execute_cmd("python check_cassandra.py")
        if res1.strip() != 'OK':
            print "Installing cassandra-driver (This may take a while...)"
            ssh.execute_cmd("pip install cassandra-driver")
        ssh.execute_cmd("rm check_cassandra.py")

    print "=== END Installing python dependencies ==="


def install_dependencies(ssh, db_type=None):
    print "=== Installing dependencies ==="
    install_docker(ssh, db_type)
    install_python_deps(ssh, db_type)
    print "=== Finished installing dependencies ==="


if __name__ == '__main__':
    description = "Install the server dependencies to run the framework. " +\
                  "A ssh connection is setup given the command line arguments. " +\
                  "The installation procedure will follow automatically."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("host", type=str, help="given host address for ssh connection.")
    parser.add_argument("-port", type=int, help="given port to connect with")
    parser.add_argument("-user", type=str, help="user name to connect with ")
    parser.add_argument("-password", type=str, help="password to connect with")

    args = parser.parse_args()

    # Set values for optional args.
    port = 22 if 'port' not in args else args.port
    user = None if 'user' not in args else args.user
    password = None if 'password' not in args else args.password

    # Communication
    server_ssh = server_conn.SSHConnection(args.host, port=port, user=user, password=password)
    install_dependencies(server_ssh)
