/*
 * ldt.h
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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
 * Large (Linked) Data Type module
 *
 */

#pragma once

#include "base/feature.h" // turn new AS Features on/off

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/as_bytes.h>
#include <citrusleaf/cf_digest.h>

#include "clock.h"
#include "fault.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt_record.h"
#include "base/write_request.h"
#include "storage/storage.h"


// Use these flags to designate various LDT bin types -- but they are all
// HIDDEN BINS.
#define LDT_FLAG_LDT_BIN 1
#define LDT_FLAG_HIDDEN_BIN 2
#define LDT_FLAG_CONTROL_BIN 4

extern cf_clock cf_clock_getabsoluteus();

typedef struct ldt_sub_gc_info_s {
	as_namespace	*ns;
	uint32_t		num_gc;
	uint32_t		num_version_mismatch_gc;
} ldt_sub_gc_info;


extern int      as_ldt_flatten_component   (as_partition_reservation *rsv, as_storage_rd *rd, as_index_ref *r_ref, as_record_merge_component *c);

extern bool     as_ldt_set_flag            (uint16_t flag);
extern bool     as_ldt_flag_has_parent     (uint16_t flag);
extern bool     as_ldt_flag_has_sub        (uint16_t flag);
extern bool     as_ldt_flag_has_subrec     (uint16_t flag);
extern bool     as_ldt_flag_has_esr        (uint16_t flag);

extern void     as_ldt_sub_gc_fn           (as_index_ref *r_ref, void *udata);
extern int      as_ldt_shipop              (write_request *wr, cf_node dest_node);

extern int      as_ldt_parent_storage_set_version (as_storage_rd *rd, uint64_t, uint8_t **);
extern int      as_ldt_parent_storage_get_version (as_storage_rd *rd, uint64_t *);
extern int      as_ldt_subrec_storage_get_pdigest (as_storage_rd *rd, cf_digest *keyd);
extern int      as_ldt_subrec_storage_get_edigest (as_storage_rd *rd, cf_digest *keyd);
extern void     as_ldt_subrec_storage_validate    (as_storage_rd *rd, char *op);

extern void     as_ldt_digest_randomizer   (as_namespace *ns, cf_digest *dig);
extern bool     as_ldt_merge_component_is_candidate(as_partition_reservation *rsv, as_record_merge_component *c);

extern void     as_ldt_record_set_rectype_bits    (as_record *r, const as_rec_props *props);

// Version related functions
extern uint64_t as_ldt_generate_version();
extern void     as_ldt_subdigest_setversion   (cf_digest *dig, uint64_t version);
extern uint64_t as_ldt_subdigest_getversion   (cf_digest *dig);
extern void     as_ldt_subdigest_resetversion (cf_digest *dig);

/*
 * Returns true if passed in record an LDT Parent (top record).
 * NOTE: Record is expected to be properly initialized and locked and
 * partition reserved
 */
static inline bool
as_ldt_record_is_parent(as_record *r)
{
	return ((!r)
			? false
			: as_index_is_flag_set( r, AS_INDEX_FLAG_SPECIAL_BINS ));
}

static inline bool
as_ldt_record_is_sub(as_record *r)
{
	return ((!r)
			? false
			: (as_index_is_flag_set( r, AS_INDEX_FLAG_CHILD_ESR ) ||
			   as_index_is_flag_set( r, AS_INDEX_FLAG_CHILD_REC )));
}

// Return true if this record is ANY type of LDT record (parent, child, esr)
static inline bool
as_ldt_record_is_ldt(as_index *r)
{
	return ((!r)
			? false
			: (as_index_is_flag_set( r, AS_INDEX_FLAG_CHILD_ESR ) ||
			   as_index_is_flag_set( r, AS_INDEX_FLAG_CHILD_REC ) ||
			   as_index_is_flag_set( r, AS_INDEX_FLAG_SPECIAL_BINS )));
}

// Return true if this record is an LDT subrecord; Child or ESR subrec.
static inline bool
as_ldt_record_is_subrec(as_record *r)
{
	return ((!r)
			? false
			: (as_index_is_flag_set( r, AS_INDEX_FLAG_CHILD_REC )));
}

// Return true if this LDT subrecord is of type ESR (Existence Sub Record).
static inline bool
as_ldt_record_is_esr(as_record *r)
{
	return ((!r)
			? false
			: as_index_is_flag_set( r, AS_INDEX_FLAG_CHILD_ESR ));
}

/*
 * Create 16 bit property field for storing on disk
 * Notes:
 * (1) All this property is some way or the other also in index so that is
 * pattern used.
 * (2) Must not add anything which is not in index as record
 * property at this point!
 */
static inline uint16_t
as_ldt_record_get_rectype_bits(as_record *r)
{
	// TODO: may be optimized
	uint16_t flag = 0;
	if (as_index_is_flag_set(r, AS_INDEX_FLAG_CHILD_ESR))
		flag |= AS_INDEX_FLAG_CHILD_ESR;
	if (as_index_is_flag_set(r, AS_INDEX_FLAG_CHILD_REC))
		flag |= AS_INDEX_FLAG_CHILD_REC;
	if (as_index_is_flag_set(r, AS_INDEX_FLAG_SPECIAL_BINS))
		flag |= AS_INDEX_FLAG_SPECIAL_BINS;

	cf_detail(AS_LDT, "Property field has Parent=%d ESR=%d REC=%d",
			flag & AS_INDEX_FLAG_SPECIAL_BINS,
			flag & AS_INDEX_FLAG_CHILD_ESR,
			flag & AS_INDEX_FLAG_CHILD_REC);

	return flag;
}

static inline int
as_ldt_bytes_todigest(as_bytes *bytes, cf_digest *keyd)
{
	if (as_bytes_size(bytes) < CF_DIGEST_KEY_SZ) {
		cf_warning(AS_LDT, "ldt_string_todigest Invalid digest size %d", as_bytes_size(bytes));
		return -1;
	}

	int i = 0;
	for(;;) {
		uint8_t val;
		as_bytes_get_byte(bytes, i, &val);
		//cf_detail(AS_UDF, "dig %d", val);
		keyd->digest[i++] = val;
		if (i == CF_DIGEST_KEY_SZ) break;
	}
	return 0;
}

// Note: If the string form bdig ever changes this function will break.
// TODO: change the whole thing into as_bytes.
static inline int
as_ldt_string_todigest(const char *bdig, cf_digest *keyd)
{
	if (strlen(bdig) < ((CF_DIGEST_KEY_SZ * 3) - 1)) {
		cf_warning(AS_LDT, "ldt_string_todigest Invalid digest %s:%d", bdig, strlen(bdig));
		return -1;
	}
	// bdig looks like
	// "06 89 8C 53 4A 51 AC F7 70 29 8D 71 FE FF 00 00 00 00 00 00"
	//
	// start at 1 and at every byte shift three positions
	int j = 0;
	for (int i = 0; ; i++) {
		char val[2];
		val[0] = bdig[i++];
		val[1] = bdig[i++];
		val[2] = '\0';
		int intval = strtol(val, NULL, 16);
		cf_detail(AS_UDF, "dig %s->%d", val, intval);
		keyd->digest[j++] = intval;
		if (j == CF_DIGEST_KEY_SZ) break;
	}
	cf_detail(AS_LDT, "Convert %s to %"PRIx64"", bdig, keyd);
	return 0;
}
