#!/usr/bin/python
#
# GenMallocations.py:
#   Generate the server header file with details about every memory allocation-related
#   location in the server source code.
#
#   Note:  The "Add()" calls are generated automatically from the source via the C PreProcessor ("cpp".)
#

import time
import os
import sys

# Global variables:

# Process command-line arguments.
if (len(sys.argv) == 2):
  gen_dir = sys.argv[1]
  sys.path.append(gen_dir)
  # Set the name of the header file to be generated.
  header_file = gen_dir + '/mallocations.h'
else:
  print "Usage: " + sys.argv[0] + " <GenDirName>"
  print "  Generate the memory allocation locations header file in the given directory name for use with ASMalloc."
  exit(-1)

# Dictionary of memory allocation-related locations in the server source code.
table = dict()

# Counter used as the primary key for each memory allocation-related function call in the server source code.
counter = 1

# Constant dictionary mapping source code function name to constants representing the memory allocation-related operation type.
func2type = {'cf_malloc_count': 'MALLOCATION_TYPE_MALLOC',
             'cf_calloc_count': 'MALLOCATION_TYPE_CALLOC',
             'cf_realloc_count': 'MALLOCATION_TYPE_REALLOC',
             'cf_free_count': 'MALLOCATION_TYPE_FREE',
             'cf_strdup_count': 'MALLOCATION_TYPE_STRDUP',
             'cf_strndup_count': 'MALLOCATION_TYPE_STRNDUP',
             'cf_valloc_count': 'MALLOCATION_TYPE_VALLOC'}

# Add an entry to the table of memory allocation-related function calls in the server.
def AddLoc(func, file, line):
 global counter
 table[counter] = [func, file, line]
 counter += 1

# One way to enumerate the table.
def Dump():
 for key in table:
  print key, '==>', table[key]

# Another way to enumerate the table.
def Dump2():
 for key, value in table.iteritems():
   print key, '==>', value

# Normalize the pathname to have at most one directory component.
def NormalizePathname(pn):
 try:
  start = pn[0:pn.rindex('/')].rindex('/') + 1
 except:
  start = 0
 return pn[start:]

# Generate and write out the header file.
def GenHeader():
 of = open(header_file, 'w')
 of.write('/*\n')
 of.write(' * Aerospike Server\n')
 of.write(' *\n')
 of.write(' * mallocations.h - Automatically-generated header file for tracking memory allocation-releated operations.\n')
 of.write(' *\n')
 of.write(' * Copyright (C) 2013-2014 Aerospike, Inc.\n')
 of.write(' *\n')
 of.write(' * Portions may be licensed to Aerospike, Inc. under one or more contributor\n')
 of.write(' * license agreements.\n')
 of.write(' *\n')
 of.write(' * This program is free software: you can redistribute it and/or modify it under\n')
 of.write(' * the terms of the GNU Affero General Public License as published by the Free\n')
 of.write(' * Software Foundation, either version 3 of the License, or (at your option) any\n')
 of.write(' * later version.\n')
 of.write(' *\n')
 of.write(' * This program is distributed in the hope that it will be useful, but WITHOUT\n')
 of.write(' * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS\n')
 of.write(' * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more\n')
 of.write(' * details.\n')
 of.write(' *\n')
 of.write(' * You should have received a copy of the GNU Affero General Public License\n')
 of.write(' * along with this program.  If not, see http://www.gnu.org/licenses/\n')
 of.write(' */\n\n')
 of.write('/*\n')
 of.write(' * SYNOPSIS\n')
 of.write(' *   Define types and a statically-initialized table giving the info. about\n')
 of.write(' *   each memory allocation-related function call in the server source code.\n')
 of.write(' */\n')
 of.write('\n')
 of.write('/**************************************************************************/\n')
 of.write('/*** NOTE:  This file was auto-generated: ' + time.asctime() + '     ****/\n')
 of.write('/**************************************************************************/\n\n')
 of.write('#pragma once\n')
 of.write('\n')
 of.write('typedef enum mallocation_type_e {\n')
 of.write('\tMALLOCATION_TYPE_NONE,\n')
 for key, value in func2type.iteritems():
  of.write('\t' + func2type[key] + ',\n')
 of.write('} mallocation_type_t;\n')
 of.write('\n')
 of.write('char *mallocation_type_names[] = {\n')
 of.write('\t"MALLOCATION_TYPE_NONE",\n')
 for key, value in func2type.iteritems():
  of.write('\t"' + value + '",\n')
 of.write('};\n')
 of.write('\n')
 of.write('typedef struct mallocation_s {\n')
 of.write('\tmallocation_type_t type;\n')
 of.write('\tchar *file;\n')
 of.write('\tint line;\n')
 of.write('\tint id;\n')
 of.write('} mallocation_t;\n')
 of.write('\n')
 of.write('#define  NUM_MALLOCATIONS  (' + str(counter) + ')\n')
 of.write('\n')
 of.write('mallocation_t mallocations[NUM_MALLOCATIONS] = {\n')
 of.write('\t{ MALLOCATION_TYPE_NONE, "", 0, 0 }, /* Non-existent mallocation. */\n')
 for key, value in table.iteritems():
  of.write('\t{ ' + func2type[value[0]] + ', \"' + NormalizePathname(value[1]) + '\", ' + str(value[2]) + ', ' + str(key)+' },\n')
 of.write('};\n')

print 'Defining the memory allocation-related program locations....'

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

print 'Done.'

#
# Actually generate the header file.
#

print 'Generating the header file: \'' + header_file + '\'....'
GenHeader()
print 'Done.'
