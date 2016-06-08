"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

This file can be used to create faults such as single bitflips in files on
specific offsets. Or random ones when not specifying file offsets.

FILE: bit_flip.py

USAGE: Function usage:
       insert_bit_flips([filepath, ..], [file_offsets, ..])
       insert_bit_flips([filepath, ..], None)

       Command line usage:
       python bit_flip.py file_name n_bit_flips

NOTE: This script probably needs sudo privileges to run on a opened file.
      Also create a BACKUP before testing this file!

"""

import io
import random
import os
import sys
from operator import xor


# Insert n random bit flips in random locations of a file, or specified file_offsets.
def insert_bit_flips(file_paths, file_offsets=None, debug=False):
    # Replace a random character with the same character where one if its bits is
    # flipped.

    for i in range(len(file_paths)):
        path = file_paths[i]
        with io.open(path, 'rb+') as f:
            if file_offsets is None:
                f.seek(0, os.SEEK_END)
                offset = random.randint(0, f.tell() - 1)
            else:
                offset = file_offsets[i]

            f.seek(offset)
            f_data = f.read(1)
            # Bit flip one of the bits of a single byte (8 bits) via xor, thus xor
            # with chars with binary format of:
            # 0000 0001,
            # 0000 0010,
            # etc. 2 ** n.
            n_data = xor(ord(f_data), 2 ** random.randint(0, 8 - 1))
            if debug:
                print 'Chars:', f_data, chr(n_data), '\tOrd:', ord(f_data), n_data
                print bin(n_data), '\n', bin(ord(f_data)), '\n'

            # Reset file pointer.
            f.seek(offset)
            f.write(chr(n_data))

if __name__ == '__main__':
    file_name = sys.argv[1]
    n_insertions = int(sys.argv[2])

    for _ in range(n_insertions):
        insert_bit_flips([file_name], None)
