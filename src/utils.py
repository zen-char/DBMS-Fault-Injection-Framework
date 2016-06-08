"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

This file contains simple json helper functions.

FILE: json_functions.py

USAGE: python attach_strace.py
"""

import io
import json
import hashlib
import datetime as dt


"""
    Str to time & time utilities
"""


# Convert a string in format Hours:Mins:Secs e.g. 12:30:02 in a datetime.time() obj.
def get_time_from_str(time_str, return_list=False):
    time_str_numbers = time_str.split(':')
    time_numbers = [int(float(number)) for number in time_str_numbers[:2]]
    if len(time_str_numbers[2].split('.')) == 2:
        secs, microsecs = [int(number) for number in (time_str_numbers[2]).split('.')]
        if return_list:
            return time_numbers + [secs, microsecs]
        return dt.time(time_numbers[0], time_numbers[1], secs, microsecs)
    else:
        if return_list:
            return time_numbers + [int(time_str_numbers[2])]
        return dt.time(time_numbers[0], time_numbers[1], int(time_str_numbers[2]))


# Find the time between a and b return a time object
def get_time_difference(time_a, time_b):
    datetime_a = dt.datetime(1, 1, 1, time_a.hour, time_a.minute, time_a.second, time_a.microsecond)
    datetime_b = dt.datetime(1, 1, 1, time_b.hour, time_b.minute, time_b.second, time_b.microsecond)
    try:
        time_delta = datetime_b - datetime_a
        return (dt.datetime.min + time_delta).time()
    except OverflowError:
        time_delta = datetime_a - datetime_b
        return (dt.datetime.min + time_delta).time()

"""
    Print utilities
"""


# Add color codes to a string without adding another library.
# Based on: http://stackoverflow.com/questions/287871
def color_str(str_to_color, color='y'):
    color_start_symbs = '\x1B['
    c = ''
    if color == 'y':
        c = color_start_symbs + '0;33;80m'
    elif color == 'g':
        c = color_start_symbs + '0;92;80m'
    return c + str_to_color + color_start_symbs + '0m'

"""
    Dictionary to str
"""


# Simple remapping of a dictionary to a non unicode str.
# Based on: http://stackoverflow.com/questions/9590382
def ascii_encode_dict(data):
    def ascii_encode(x):
        return x.encode('ascii') if isinstance(x, unicode) else x
    return dict(map(ascii_encode, pair) for pair in data.items())


"""
    JSON utilities
"""


# Pretty print json data.
def print_json(json_data):
    return json.dumps(json_data, sort_keys=True, indent=2, separators=(',', ': '))


def load_json_file(file_name):
    try:
        with io.open(file_name, 'r') as json_file:
            data = json.load(json_file)
        return data
    except ValueError:
        print 'Error while parsing the file: {}.'.format(file_name)
    except IOError:
        print 'File: {} does not exists. Exiting.'.format(file_name)
    return None

"""
    Checksum utilities
"""


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
