#!/usr/bin/python
#
# Expand.py:
#   Expand a macro with respect to a data structure derived from pre-processing C source code.
#
#   Note:  The "AddLoc()" calls are generated automatically from the source via the C PreProcessor ("cpp".)
#

import sys

# Global variables:

# Dictionaries of memory allocation-related locations in the server source code:

# The forward map:  mallocation counter to program location.
table = dict()

# The reverse map:  program location ==> mallocation counter.
rev_map = dict()

# Counter used as the primary key for each memory allocation-related function call in the server source code.
counter = 1

# Add an entry to the table of memory allocation-related function calls in the server.
def AddLoc(func, file, line):
 global counter
 table[counter] = [func, file, line]
 rev_map[func + file + str(line)] = counter
 counter += 1

# Expand a macro based upon a generated data structure.
def Expand(func, file, line):
 return (lambda x: x if x is not None else 0)(rev_map.get(func + file + line))

#
# Here begins the sequence of calls to populate the table of memory allocation-related
# function call location in the server source code.
#
# (Generated via the C Pre-Processor.)
#

from CPPMallocations import AddLocs

AddLocs(AddLoc)

#
# Here ends the sequence of calls to populate the table of memory allocation-related
# function call location in the server source code.
#

#
# Expand the macro.
#

#print 'Expanding macro: \'' + str(sys.argv[1:4]) + '\'....'
#print Expand(*(sys.argv[1:4]))
sys.stdout.write(str(Expand(*(sys.argv[1:4]))))
#print 'Done.'
