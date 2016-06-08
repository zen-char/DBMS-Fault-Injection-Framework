"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

This file can be used to create faults such as stuck bits.

FILE:  stuck_bit.py

USAGE: Only for import usage.
       insert_stuck_bit([filepath, ..], [file_offsets, ..], [bit_position, ...])


NOTE: This file probably needs sudo privileges to open a running file. Moreover this
      fault has to be repeated several times. A way has still be found to implement
      this effect properly.
"""

import io
from operator import xor
from itertools import izip


def insert_stuck_bit(file_paths, file_offsets, bit_positions, debug=False):
    for path, offset, bit_position in izip(file_paths, file_offsets, bit_positions):
        with io.open(path, 'rb+') as f:

            f.seek(offset)
            f_data = f.read(1)
            # Bit flip one of the bits of a single byte (8 bits) via xor, thus xor
            # with chars with binary format of:
            # 0000 0001,
            # 0000 0010,
            # etc. 2 ** n.
            n_data = xor(ord(f_data), 2 ** bit_position)
            if debug:
                print 'Chars:', f_data, chr(n_data), '\tOrd:', ord(f_data), n_data
                print bin(n_data), '\n', bin(ord(f_data)), '\n'

            # Reset file pointer.
            f.seek(offset)
            f.write(chr(n_data))
