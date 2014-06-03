/*
 * ldt_record.h
 *
 * Copyright (C) 2013-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

/*
 * as_record interface implementation for Large Stack Objects
 */

#pragma once

#include "base/feature.h" // turn new AS Features on/off (must be first in line)

#include <stddef.h>
#include <stdint.h>

#include <aerospike/as_aerospike.h>
#include <aerospike/as_rec.h>

#include "base/index.h"
#include "base/transaction.h"
#include "base/udf_record.h"
#include "storage/storage.h"


/*
 * Large Data Type (LDT)
 *
 * The General LDT value comprises a "Top Record" (a regular Aerospike record)
 * which contains a bin that is a Large Data Type.  In that LDT Bin there is
 * a map that contains pointers to some number of "Sub Records" (aka. "Chunks",
 * aka "child records", that are linked to the Top Rec).
 * Currently we limit the number of open sub (esr + subrec) to 20, but this
 * will change as the LDT structures get more sophisticated.  Over time, we'll
 * likely manage the open structures dynamically and thus allow large numbers
 * of open subs, should the situation require it.
 */

/* A Large Data Type (LDT) "Chunk" refers to a single record that is a
 * child to an Aerospike "Top Record".
 */
typedef struct ldt_chunk_s {
	as_rec              c_urec;
	udf_record          c_urecord;   // Currently open chunk
	as_transaction      tr;
	as_storage_rd       rd;
	as_index_ref        r_ref;
	int                 slot; 
} ldt_chunk;

/*
 * This structure represents an open record that contains an LDT Object.
 * "ldt_chunk" represents an opened sub (limit is currently 6 (20) ).
 * NOTE: Entire thing is deliberately stack allocated to make it efficient
 */
#define MAX_LDT_CHUNKS 20 /* TODO: Make this dynamic */
struct ldt_record_s {
	as_rec             * h_urec;
	ldt_chunk            chunk[MAX_LDT_CHUNKS]; // If used wisely won't need more than
	// this at a time. The structure is pretty
	// big redo it.
	as_aerospike       * as;       // To operate on ldt_record_chunk
	uint64_t             version;  // this is version key used to open/close/search
	// for the sub_record digest
};

extern const as_rec_hooks ldt_record_hooks;

//extern int ldt_record_init(ldt_record *lr, as_namespace *ns, cf_digest *keyd);
extern int   ldt_record_init   (ldt_record *lrecord);
extern int   ldt_record_pickle (ldt_record *lrecord, uint8_t **pickled_buf, size_t *pickled_sz, uint32_t *pickled_void_time);

// TODO this must change with the MAX_LDT_CHUNKS!!!

#define FOR_EACH_SUBRECORD(i, lrecord) \
	for (int i = 0; (i < MAX_LDT_CHUNKS); i++)  if ((lrecord)->chunk[i].slot != -1)
