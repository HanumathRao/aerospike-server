/*
 * udf_record.c
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

#include "base/feature.h" // Turn new AS Features on/off (must be first in line)

#include "base/udf_record.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_rec.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"

#include "clock.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/rec_props.h"
#include "base/transaction.h"
#include "base/udf_rw.h"
#include "storage/storage.h"

bool
udf_record_ldt_enabled(const as_rec * rec)
{
	udf_record *urecord = (udf_record *)as_rec_source(rec);
	if (!urecord)         return false;
	as_namespace *ns    = urecord->tr->rsv.ns;
	if (!ns)              return false;
	if (!ns->ldt_enabled) return false;
	else        		  return true;
}
/*
 * Function: Open storage record for passed in udf record
 *           also set up flag like exists / read et al.
 *
 * Parameters:
 * 		urec    : UDF record
 *
 * Return value : 0 always
 *
 * Callers:
 * 		udf_record_open
 *
 * Note: There are no checks, so the caller has to make sure that all
 *       protections are taken and all checks are done.
 *
 *  Side effect:
 *  	Counters will be reset
 *  	flag will be set
 *  	bins will be opened
 */
int
udf_storage_record_open(udf_record *urecord)
{
	cf_detail_digest(AS_UDF, &urecord->tr->keyd, "udf_storage_record_open: Opening record key:");
	as_storage_rd  *rd    = urecord->rd;
	as_index       *r	  = urecord->r_ref->r;
	as_transaction *tr    = urecord->tr;
	int rv = as_storage_record_open(tr->rsv.ns, r, rd, &r->key);
	if (0 != rv) {
		cf_warning(AS_UDF, "Could not open record !! %d", rv);
		return rv;
	}
	rd->n_bins = as_bin_get_n_bins(r, rd);
	// if multibin storage, we will use urecord->stack_bins, so set the size appropriately
	if ( ! tr->rsv.ns->storage_data_in_memory && ! tr->rsv.ns->single_bin ) {
		rd->n_bins = sizeof(urecord->stack_bins) / sizeof(as_bin);
	}

	// stack bins are used when data-on-storage will not work for
	// namespace going beyond stack size !! Currently set to 256
	rd->bins = as_bin_get_all(r, rd, urecord->stack_bins);
	if (tr->rsv.ns->storage_data_in_memory) {
		urecord->starting_memory_bytes = as_storage_record_get_n_bytes_memory(rd);
	}
	urecord->flag   |= UDF_RECORD_FLAG_STORAGE_OPEN;

	urecord->ldt_rectype_bits = as_ldt_record_get_rectype_bits(r);
	cf_detail(AS_RW, "TO URECORD FROM INDEX Digest=%"PRIx64" bits %d %p",
			  *(uint64_t *)&urecord->tr->keyd.digest[8], urecord->ldt_rectype_bits, urecord);

	cf_detail_digest(AS_UDF, &tr->keyd, "Storage Open: Rec(%p) flag(%x) Digest:", urecord, urecord->flag );
	if (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) {
		as_ldt_subrec_storage_validate(rd, "Reading");
	}
	return 0;
}

/*
 * Function: Close storage record if it open and also set flags
 *
 * Parameters:
 * 		urec    : UDF record
 *
 * Return value : 0 in case storage was open
 *                1 in case storage was not open
 *
 * Callers:
 * 		udf_record_close
 * 		udf_aerospike__storage_commit
 *
 *  Side effect:
 *  	flag will be reset
 *  	bins will be closed
 */
int
udf_storage_record_close(udf_record *urecord)
{
	if (urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN) {
		as_index_ref   *r_ref = urecord->r_ref;
		as_storage_rd  *rd    = urecord->rd;

		// TODO - would be nice to not do any rec_props preparation if we only
		// opened for read, but because of sizing for stack allocation we must.
		// However, it appears that ALLOW_UPDATES is always set.
		as_storage_record_get_key(rd);

		// In case allow update is not set .. the record has been opened for
		// the aggregation. Do not do any rec property update.
		// Pick info from index and put it in storage record.
		size_t  rec_props_data_size = as_storage_record_rec_props_size(rd);
		uint8_t rec_props_data[rec_props_data_size];
		if (urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES) {
			if (rec_props_data_size > 0) {
				cf_detail(AS_LDT, "LDT_INDEXBITS Setting Property at close time parent=%d, esr=%d, sub=%d",
						  as_ldt_record_is_parent(rd->r),
						  as_ldt_record_is_esr(rd->r),
						  as_ldt_record_is_subrec(rd->r));
				as_storage_record_set_rec_props(rd, rec_props_data);
			}
		}

		if (!(urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD)) {
			if (as_ldt_record_is_parent(rd->r)) {
				cf_detail_digest(AS_LDT, &rd->keyd, "LDT_INDEXBIT Parent @ write: Digest:");
			}
		} else {
			as_ldt_subrec_storage_validate(rd, "Writing");
		}

		if (r_ref) {
			as_storage_record_close(r_ref->r, rd);
			bzero(urecord->rd, sizeof(as_storage_rd));
		} else {
			// Should never happen.
			cf_warning(AS_UDF, "Unexpected Internal Error (null r_ref)");
		}
		urecord->flag &= ~UDF_RECORD_FLAG_STORAGE_OPEN;
		cf_detail_digest(AS_UDF, &urecord->tr->keyd, "Storage Close:: Rec(%p) Flag(%x) Digest:",
				urecord, urecord->flag );
		return 0;
	} else {
		return 1;
	}
}

/*
 * Function: Destroy storage record if it open and also set flags
 *
 * Parameters:
 * 		urec    : UDF record
 *
 * Return value : 0 in case of success
 * 				: 1 if storage record is not open
 *              : -1 if inuse bin is not 0
 *
 * Callers:
 * 		udf_aerospike__storage_commit
 *
 *  Side effect:
 *  	flag will be reset
 *  	bins will be closed
 *  	Reference to record on the storage is lost
 */
int
udf_storage_record_destroy(udf_record *urecord)
{
	if (urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN) {
		as_index_ref   *r_ref = urecord->r_ref;
		as_storage_rd  *rd    = urecord->rd;
		if (r_ref) {
			if (as_bin_inuse_has(rd)) {
				return -1;
			} else {
				as_storage_record_destroy(rd->ns, r_ref->r);
				return 0;
			}
		} else {
			return -2;
			// What to do . should never happen.
		}
	} else {
		return 1;
	}
}


/*
 * Function: Open storage record for passed in udf record
 *           also set up flag like exists / read et al.
 *           Does as_record_get as well if it is not done yet.
 *
 * Parameters:
 * 		urec    : UDF record
 *
 * Return value :
 *  	 0 in case record is successfully read
 * 		-1 in case record is not found
 * 		-2 in case record is found but has expired
 *
 * Callers:
 * 		query_agg_istream_read
 * 		ldt_crec_open
 */
int
udf_record_open(udf_record * urecord)
{
	cf_detail_digest(AS_UDF, &urecord->tr->keyd, "udf_record_open: Opening record key:");
	if (urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN) {
		cf_detail(AS_UDF, "Record already open");
		return 0;
	}
	as_transaction *tr    = urecord->tr;
	as_index_ref   *r_ref = urecord->r_ref;
	as_index_tree  *tree  = tr->rsv.tree;

	if (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) {
		tree = tr->rsv.sub_tree;
	}

	int rec_rv = 0;
	if (!(urecord->flag & UDF_RECORD_FLAG_OPEN)) {
		cf_detail(AS_UDF, "Opening %sRecord ",
				  (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) ? "Sub" : "");
		rec_rv = as_record_get(tree, &tr->keyd, r_ref, tr->rsv.ns);
	}

	if (!rec_rv) {
		as_index *r = r_ref->r;
		// check to see this isn't an expired record waiting to die
		if (r->void_time &&
				r->void_time < as_record_void_time_get()) {
			as_record_done(r_ref, tr->rsv.ns);
			cf_detail(AS_UDF, "udf_record_open: Record has expired cannot read");
			rec_rv = -2;
		} else {
			urecord->flag   |= UDF_RECORD_FLAG_OPEN;
			cf_detail_digest(AS_UDF, &tr->keyd, "Open %p %x Digest:", urecord, urecord->flag);
			udf_storage_record_open(urecord);
		}
	} else {
		cf_detail(AS_UDF, "udf_record_open: rec_get returned with %d", rec_rv);
	}
	return rec_rv;
}

/*
 * Function: Close storage record for udf record. Release
 *           all locks and partition reservation / namespace
 *           reservation etc. if requested.
 *           Also cleans up entire cache (updated from udf)
 *
 * Parameters:
 * 		urec       : UDF record being operated on
 * 		release_rsv: If tree partition reservation is released
 *
 * Return value : Nothing
 *
 * Callers:
 * 		query_agg_istream_read
 * 		ldt_aerospike_crec_close
 * 		as_query__agg
 * 		udf_record_destroy
 */
void
udf_record_close(udf_record *urecord, bool release_rsv)
{
	as_transaction *tr    = urecord->tr;

	if (urecord->flag & UDF_RECORD_FLAG_OPEN) {
		as_index_ref   *r_ref = urecord->r_ref;
		cf_detail(AS_UDF, "Closing %sRecord",
				  (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) ? "Sub" : "");
		udf_storage_record_close(urecord);
		as_record_done(r_ref, tr->rsv.ns);
		bzero(urecord->r_ref, sizeof(as_index_ref));
		urecord->flag &= ~UDF_RECORD_FLAG_OPEN;
		cf_detail_digest(AS_UDF, &urecord->tr->keyd,
			"Storage Close:: Rec(%p) Flag(%x) Digest:", urecord, urecord->flag );
	}

	// No references are release in scope of UDF record
	// only in case of aggregation for stream interface
	if (tr && release_rsv) {
		as_partition_release(&tr->rsv);
		cf_atomic_int_decr(&g_config.dup_tree_count);
	}
	udf_record_cache_free(urecord);
}

/*
 * Function: This function called to reinitialize the udf_record. It sets up
 *           the basic value back to default. Can be called after the UDF
 *           record has been used. Reset the fact that record pre_exits or
 *           was actually read etc.
 *
 * Parameters:
 * 		urec	: UDF record being initialized
 *
 * Return value : Nothing
 *
 * Callers:
 * 		ldt_chunk_init (for chunk)
 * 		udf_rw_local   (parent record before calling UDF)
 */
void
udf_record_init(udf_record *urecord)
{
	urecord->tr                 = NULL;
	urecord->r_ref              = NULL;
	urecord->rd                 = NULL;
	urecord->nupdates           = 0;
	urecord->particle_data      = NULL;
	urecord->cur_particle_data  = NULL;
	urecord->end_particle_data  = NULL;
	urecord->starting_memory_bytes = 0;
	urecord->lrecord            = NULL;

	// Init flag
	urecord->flag               = 0;
	urecord->flag              |= UDF_RECORD_FLAG_ALLOW_UPDATES;

	urecord->pickled_buf        = NULL;
	urecord->pickled_sz         = 0;
	urecord->pickled_void_time  = 0;

	as_rec_props_clear(&urecord->pickled_rec_props);

	urecord->ldt_rectype_bits   = 0;
	urecord->keyd               = cf_digest_zero;
}

/*
 * Function: Cleans up the pickled if it is hanging from the udf_record.
 *           frees it as well if pickled_buf needs to be freed up.
 *
 * Parameters:
 * 		urec	: UDF record
 *
 * Return value : Nothing
 *
 * Callers:
 * 		udf_rw_finish
 */
void
udf_record_cleanup(udf_record *urecord, bool dofree)
{
	if (urecord->pickled_buf) {
		if (dofree) {
			cf_free(urecord->pickled_buf);
		}

		urecord->pickled_buf       = NULL;
		urecord->pickled_sz        = 0;
		urecord->pickled_void_time = 0;
	}

	if (urecord->pickled_rec_props.p_data) {
		if (dofree) {
			cf_free(urecord->pickled_rec_props.p_data);
		}

		as_rec_props_clear(&urecord->pickled_rec_props);
	}
}

/*
static int print_buffer(as_buffer * buff) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    sbuf.data = buff->data;
    sbuf.size = buff->size;
    sbuf.alloc = buff->capacity;

    msgpack_zone mempool;
    msgpack_zone_init(&mempool, 2048);

    msgpack_object deserialized;
    msgpack_unpack(sbuf.data, sbuf.size, NULL, &mempool, &deserialized);

    printf("msg_buf:\n");
    msgpack_object_print(stdout, deserialized);
    puts("");

    msgpack_zone_destroy(&mempool);
    return 0;
}
*/

/*
 * Function: Get bin value from cached copy. All the update in a
 *           commit window is not applied to the record directly
 *           but maintained in-memory cache. This function used
 *           to retrieve cached value
 *
 *           Similar function for get and free of cache
 *
 * Parameters:
 * 		urec    : Parent ldt record
 *
 * Return value :
 * 		value  (as_val) in case of success [for get]
 * 		NULL  in case of failure
 * 		set and free return Nothing
 *
 * Callers:
 * 		GET and SET
 * 		udf_record_get
 * 		udf_record_set
 * 		udf_record_remove
 *
 * 		FREE
 * 		udf_aerospike__execute_updates (when crossing commit window)
 * 		ldt_aerospike_crec_close       (when closing chunk sub record)
 * 		udf_record_close               (finally closing record/generally subrecord)
 * 		udf_rw_commit                  (commit the udf record or parent ldt record)
 *
 *		ldt_aerospike_crec_create
 */
static as_val *
udf_record_cache_get(udf_record * urecord, const char * name)
{
	cf_debug(AS_UDF, "[ENTER] BinName(%s) ", name );
	if ( urecord->nupdates > 0 ) {
		cf_detail(AS_UDF, "udf_record_get: %s find", name);
		for ( uint32_t i = 0; i < urecord->nupdates; i++ ) {
			udf_record_bin * bin = &(urecord->updates[i]);
			if ( strncmp(name, bin->name, BIN_NAME_MAX_SZ) == 0 ) {
				cf_detail(AS_UDF, "Bin %s found, type(%d)", name, bin->value->type );
				if ( bin->value->type == AS_NIL ) {
					cf_detail(AS_UDF, "udf_record_get: %s return NULL", name);
					return NULL;
				}
				else {
					cf_detail(AS_UDF, "udf_record_get: %s return", name);
					return bin->value;
				}
			}
		}
	}
	return NULL;
}

void
udf_record_cache_free(udf_record * urecord)
{
	cf_debug(AS_UDF, "[ENTER] NumUpdates(%d) ", urecord->nupdates );

	for (int i = 0; i < urecord->nupdates; i ++ ) {
		udf_record_bin * bin = &urecord->updates[i];
		if ( bin->name[0] != '\0' && bin->value != NULL ) {
			bin->name[0] = '\0';
			as_val_destroy(bin->value);
			bin->value = NULL;
		}
		if ( bin->name[0] != '\0' && bin->oldvalue != NULL ) {
			bin->name[0] = '\0';
			as_val_destroy(bin->oldvalue);
			bin->oldvalue = NULL;
		}
	}
	urecord->nupdates = 0;
}

/**
 * Set the cache value for a bin, including flags.
 */
static void
udf_record_cache_set(udf_record * urecord, const char * name, as_val * value,
					 bool dirty)
{
	cf_debug(AS_UDF, "[ENTER] urecord(%p) name(%p)[%s] dirty(%d)",
			  urecord, name, name, dirty);

	bool modified = false;

	for ( uint32_t i = 0; i < urecord->nupdates; i++ ) {
		udf_record_bin * bin = &(urecord->updates[i]);

		// bin exists, then we will release old value and set new value.
		if ( strncmp(name, bin->name, BIN_NAME_MAX_SZ) == 0 ) {
			cf_detail(AS_UDF, "udf_record_set: %s found", name);

			// release previously set value
			as_val_destroy(bin->value);

			// set new value, with dirty flag
			if( value != NULL ) {
				bin->value = (as_val *) value;
			}
			bin->dirty = dirty;
			cf_detail(AS_UDF, "udf_record_set: %s set for %p:%p", name,
					urecord, bin->value);

			modified = true;
			break;
		}
	}

	// If not modified, then we will add the bin to the cache
	if ( !modified && urecord->nupdates < UDF_RECORD_BIN_ULIMIT - 1 ) {
		udf_record_bin * bin = &(urecord->updates[urecord->nupdates]);
		strncpy(bin->name, name, BIN_NAME_MAX_SZ);
		bin->value = (as_val *) value;
		bin->dirty = dirty;
		urecord->nupdates++;
		cf_detail(AS_UDF, "udf_record_set: %s not modified, add for %p:%p",
				name, urecord, bin->value);
	}
}

/**
 * Set the cache value for a bin, including flags.
 */
static void
udf_record_cache_sethidden(udf_record * urecord, const char * name)
{
	int modified = false;
	for ( uint32_t i = 0; i < urecord->nupdates; i++ ) {
		udf_record_bin * bin = &(urecord->updates[i]);

		// bin exists, then we will release old value and set new value.
		if ( strncmp(name, bin->name, BIN_NAME_MAX_SZ) == 0 ) {
			cf_detail(AS_UDF, "udf_record_cache_sethidden: %s found", name);
			// TODO make sure it is initialized to false
			bin->ishidden = true;
			modified      = true;
			break;
		}
	}

	// If not modified, then we will add the bin to the cache
	if ( !modified && urecord->nupdates < UDF_RECORD_BIN_ULIMIT - 1 ) {
		udf_record_bin * bin = &(urecord->updates[urecord->nupdates]);
		strncpy(bin->name, name, BIN_NAME_MAX_SZ);
		bin->ishidden = true;
		bin->dirty    = true;
		urecord->nupdates++;
		cf_detail(AS_UDF, "udf_record_cache_sethidden: %s not modified, add for %p:%p",
				name, urecord, bin->value);
	}
}

/*
 * Internal Function: Read and figure out if the bin is hidden
 *
 * Parameters:
 * 		r    : udf record
 * 		bname: Bin name of the bin which need to be read.
 *
 * Return value :
 * 	 	true:  if hidden
 * 	 	false: o/w or in case bin is not found
 *
 * Description:
 * 		Expectation is the record is already open. No checks are
 * 		performed in this function. Caller needs to make sure the
 * 		record is good to read e.g binname etc.
 *
 * Callers:
 * 		udf_aerospike__apply_update_atomic
 */
bool
udf_record_bin_ishidden(const udf_record *urecord, const char *name)
{
	if (!name) return NULL;
	const char *    bname = (char *) name;
	size_t          blen  = strlen(name);
	as_bin *        bb    = as_bin_get(urecord->rd, (uint8_t *) bname, blen);

	if ( !bb ) {
		cf_detail(AS_UDF, "udf_record_get: bin not found (%s)", name);
		return 0;
	}
	return as_bin_is_hidden(bb);
}

/*
 * Internal Function: Read the bin from storage and convert it
 *                    into as_val and return
 *
 * Parameters:
 * 		r    : udf record
 * 		bname: Bin name of the bin which need to be read.
 *
 * Return value :
 * 	 	value (as_val *) in case of success
 * 		NULL  in case of failure
 *
 * Description:
 * 		Expectation is the record is already open. No checks are
 * 		performed in this function. Caller needs to make sure the
 * 		record is good to read e.g binname etc.
 *
 * 		NB: as_val which is returned is allocated one. It is callers
 * 		    responsibility to free else in case it is passed on to
 * 		    lua ... lua has responsibility of garbage collecting it.
 * 		    Hence this function call incurs and malloc cost.
 *
 * Callers:
 * 		udf_record_get
 */
as_val *
udf_record_storage_get(const udf_record *urecord, const char *name)
{
	if (!name) {
		cf_detail(AS_UDF, "Passed Null bin name to storage get");
		return NULL;
	}
	const char *    bname = (char *) name;
	size_t          blen  = strlen(name);
	as_bin *        bb    = as_bin_get(urecord->rd, (uint8_t *) bname, blen);

	if ( !bb ) {
		cf_detail(AS_UDF, "udf_record_get: bin not found (%s)", name);
		return NULL;
	}

	return as_val_frombin(bb);
}

/*********************************************************************
 * INTERFACE FUNCTIONS                                               *
 *																	 *
 * See the as_aerospike for the API definition						 *
 ********************************************************************/
static as_val *
udf_record_get(const as_rec * rec, const char * name)
{
	if (!rec || !name) {
		cf_warning(AS_UDF, "Invalid Paramters: record=%p binname=%p", rec, name);
		return NULL;
	}
	udf_record  *   urecord = (udf_record *) as_rec_source(rec);
	if (!urecord) {
		return NULL;
	}
	as_val *        value   = NULL;

	cf_debug(AS_UDF, "[ENTER] rec(%p) name(%s)", rec, name );

	// Get from cache
	value = udf_record_cache_get(urecord, name);

	// If value not NULL, then return it.
	if ( value != NULL ) {
		return value;
	}

	// Check in the cache before trying to look up in record
	// Note: Record may not have been created yet ... Do not
	// change the order unless you fully understand what you
	// are doing
	if ( !(urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN) ) {
		if (udf_record_open(urecord)) { // lazy read the record from storage
			return NULL;
		}
	}

	// Check if storage is available
	if ( !urecord->rd->ns ) {
		cf_detail(AS_UDF, "udf_record_get: storage unavailable");
		return NULL;
	}

	value = udf_record_storage_get(urecord, name);

	// We have a value, so we will cache it.
	if ( urecord && value ) {
		udf_record_cache_set(urecord, name, value, false);
	}

	cf_detail(AS_UDF, "udf_record_get: end (%s) [%p,%p]", name, urecord, value);
	return value;
}

static int
udf_record_set(const as_rec * rec, const char * name, const as_val * value)
{
	if (!rec || !name) {
		cf_warning(AS_UDF, "Invalid Paramters: record=%p binname=%p", rec, name);
		return 2;
	}
	cf_detail(AS_UDF, "[ENTER] rec(%p) name(%s)", rec, name );

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (!urecord || !(urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES))
		return -1;

	cf_detail(AS_UDF, "udf_record_set: begin (%s)", name);
	if ( urecord && name ) {
		udf_record_cache_set(urecord, name, (as_val *) value, true);
	}
	cf_detail(AS_UDF, "udf_record_set: end (%s)", name);

	return 0;
}

/**
 * Set the flags for a specific bin.  This is how LDTs mark Hidden Bins. Other
 * uses may also apply.
 */
static int
udf_record_set_flags(const as_rec * rec, const char * name, uint8_t flags)
{
	if (!rec || !name) {
		cf_warning(AS_UDF, "Invalid Paramters: record=%p binname=%p", rec, name);
		return 2;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (!urecord || !(urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES))
		return -1;

	if ( urecord && name ) {
		if (flags & LDT_FLAG_HIDDEN_BIN || flags & LDT_FLAG_LDT_BIN || flags & LDT_FLAG_CONTROL_BIN ) {
			cf_debug(AS_UDF, "LDT flag(%d) Designates Hidden Bin", flags);
			udf_record_cache_sethidden(urecord, name);
		} else {
			cf_warning(AS_UDF, "Unidentified flag setting up %d", flags);
		}
	}
	return 0;
}

static int
udf_record_set_type(const as_rec * rec,  uint8_t  ldt_rectype_bits)
{
	if (!rec || !ldt_rectype_bits) {
		cf_warning(AS_UDF, "Invalid Paramters: record=%p rec_type_bits=%d", rec, ldt_rectype_bits);
		return 2;
	}

	if (!udf_record_ldt_enabled(rec)
			&& (as_ldt_flag_has_parent(ldt_rectype_bits)
				|| as_ldt_flag_has_sub(ldt_rectype_bits))) {
		cf_warning(AS_LDT, "Cannot Set Large Object Bits .. Not Enabled !!");
		return -2;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (!urecord || !(urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES)) {
		return -1;
	}
	urecord->ldt_rectype_bits = ldt_rectype_bits;
	cf_detail(AS_RW, "TO URECORD FROM LUA   Digest=%"PRIx64" bits %d",
			  *(uint64_t *)&urecord->rd->keyd.digest[8], urecord->ldt_rectype_bits);
	urecord->rd->write_to_device = true;
	return 0;
}
/* Keep this for reference.
 * typedef enum {
	// The first two values -- do NOT used in single bin mode
	AS_INDEX_FLAG_SPECIAL_BINS		= 0x01, // First user of this is @LDT (to denote subrecs)
	AS_INDEX_FLAG_CHILD_REC 		= 0x02, // Child Record of a regular record (LDT)
	AS_INDEX_FLAG_CHILD_ESR         = 0x04, // Special Child Existence Sub Record (ESR)
	AS_INDEX_FLAG_UNUSED_0x08		= 0x08,

	// Combinations:
	AS_INDEX_ALL_SINGLE_BIN_FLAGS	= 0x0C,
	AS_INDEX_ALL_MULTI_BIN_FLAGS	= 0x0F
} as_index_flag;

static inline
bool as_index_is_flag_set(as_index* index, as_index_flag flag) {
	return (((as_index_flag_bits*)&index->flex_bits_2)->flag_bits & flag) != 0;
}

static inline
void as_index_set_flags(as_index* index, as_index_flag flags) {
	((as_index_flag_bits*)&index->flex_bits_2)->flag_bits |= flags;
}
 */

static int
udf_record_remove(const as_rec * rec, const char * name)
{
	if (!rec || !name) {
		cf_warning(AS_UDF, "Invalid Paramters: record=%p binname=%p", rec, name);
		return 2;
	}
	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (!urecord || !(urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES))
		return -1;

	cf_detail(AS_UDF, "udf_record_remove: begin (%s)", name);
	if ( urecord && name ) {
		udf_record_cache_set(urecord, name, (as_val *) &as_nil, true);
	}
	cf_detail(AS_UDF, "udf_record_remove: end (%s)", name);

	return 0;
}

static uint32_t
udf_record_ttl(const as_rec * rec)
{
	if (!rec) {
		cf_warning(AS_UDF, "Invalid Paramters: record=%p", rec);
		return 0;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (!urecord) {
		return 0;
	}

	if (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) {
		cf_debug(AS_UDF, "Return 0 TTL for subrecord ");
		return 0;
	}

	if ((urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN)) {
		if (urecord->r_ref->r->void_time > 0) {
			return (urecord->r_ref->r->void_time - as_record_void_time_get());
		} else {
			return 0;
		}
	}
	else {
		cf_info(AS_UDF, "Error in getting ttl: no record found");
	}
	return 0;
}

static uint16_t
udf_record_gen(const as_rec * rec)
{
	if (!rec) {
		cf_warning(AS_UDF, "Invalid Parameters: record=%p", rec);
		return 0;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);
	if (urecord && urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN) {
		return urecord->r_ref->r->generation;
	}
	else {
		cf_warning(AS_UDF, "Error in getting generation: no record found");
		return 0;
	}
}

static bool
udf_record_destroy(as_rec *rec)
{
	if (!rec) {
		cf_warning(AS_UDF, "Invalid Paramters: record=%p", rec);
		return false;
	}

	udf_record *urecord = (udf_record *) as_rec_source(rec);
	if (!urecord || !(urecord->flag & UDF_RECORD_FLAG_ALLOW_DESTROY))
		return false;
	if (urecord->pickled_buf) {
		cf_free(urecord->pickled_buf);
		urecord->pickled_buf       = NULL;
		urecord->pickled_sz        = 0;
		urecord->pickled_void_time = 0;
	}
	if (urecord->pickled_rec_props.p_data) {
		cf_free(urecord->pickled_rec_props.p_data);
		as_rec_props_clear(&urecord->pickled_rec_props);
	}
	udf_record_close(urecord, false);
	return true;
} // end udf_record_destroy()

static as_bytes *
udf_record_digest (const as_rec *rec)
{
	if (!rec) {
		cf_warning(AS_UDF, "Invalid Paramters: record=%p", rec);
		return NULL;
	}

	udf_record *urecord = (udf_record *)as_rec_source(rec);
	if (urecord && urecord->flag & UDF_RECORD_FLAG_OPEN) {
		cf_digest *keyd = cf_malloc(sizeof(cf_digest));
		if (!keyd) {
			return NULL;
		}
		memcpy(keyd, &urecord->keyd, CF_DIGEST_KEY_SZ);
		as_bytes *b = as_bytes_new_wrap(keyd->digest, CF_DIGEST_KEY_SZ, true);
		return b;
	}
	return NULL;
}

const as_rec_hooks udf_record_hooks = {
	.get		= udf_record_get,
	.set		= udf_record_set,
	.remove		= udf_record_remove,
	.ttl		= udf_record_ttl,
	.gen		= udf_record_gen,
	.destroy	= udf_record_destroy,
	.digest		= udf_record_digest,
	.set_flags	= udf_record_set_flags,	// @LDT:: added for control over LDT Bins from Lua
	.set_type	= udf_record_set_type,	// @LDT:: added for control over Rec Types from Lua
	.numbins	= NULL,
};
