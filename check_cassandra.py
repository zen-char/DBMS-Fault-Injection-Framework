"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

This file simply checks if the cassandra driver can be imported.

FILE: check_cassandra.py

TODO: Expand to other libraries and name more generic.

USAGE: python check_cassandra.py

"""

try:
    import cassandra
    # For some reason, the cluster module is not always present.
    from cassandra.cluster import Cluster

    print "OK"
except ImportError as _:
    print "ERROR: cassandra not installed"
