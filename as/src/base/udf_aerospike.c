/*
 * udf_aerospike.c
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

#include "base/feature.h" // Turn new AS Features on/off

#include "base/udf_aerospike.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <asm/byteorder.h>

#include "aerospike/as_aerospike.h"
#include "aerospike/as_boolean.h"
#include "aerospike/as_buffer.h"
#include "aerospike/as_bytes.h"
#include "aerospike/as_integer.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_string.h"
#include "aerospike/as_val.h"

#include "clock.h"
#include "fault.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/secondary_index.h"
#include "base/thr_rw_internal.h"
#include "base/transaction.h"
#include "base/udf_record.h"
#include "base/udf_rw.h"
#include "storage/storage.h"


static int udf_aerospike_rec_remove(const as_aerospike *, const as_rec *);
/*
 * Internal Function: udf_aerospike_delbin
 *
 * Parameters:
 * 		r 		- udf_record to be manipulated
 * 		bname 	- name of the bin to be deleted
 *
 * Return value:
 * 		0  on success
 * 	   -1  on failure
 *
 * Description:
 * 		The function deletes the bin with the name
 * 		passed in as parameter. The as_bin_destroy function
 * 		which is called here, only frees the data and
 * 		the bin is marked as not in use. The bin can then be reused later.
 *
 * 		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		udf_aerospike__apply_update_atomic
 * 		In this function, if it fails at the time of update, the record is set
 * 		to rollback all the updates till this point. The case where it fails in
 * 		rollback is not handled.
 *
 * 		Side Notes:
 * 		i.	write_to_device will be set to true on a successful bin destroy.
 * 		If all the updates from udf_aerospike__apply_update_atomic (including this) are
 * 		successful, the record will be written to disk and reopened so that the rest of
 * 		sets of updates can be applied.
 *
 * 		ii.	If delete from sindex fails, we do not handle it.
 */
static int
udf_aerospike_delbin(udf_record * urecord, const char * bname)
{
	if (bname == NULL || bname[0] == 0 ) {
		cf_warning(AS_UDF, "no bin name supplied");
		return -1;
	}

	size_t          blen    = strlen(bname);
	as_storage_rd  *rd      = urecord->rd;
	as_transaction *tr      = urecord->tr;
	as_bin * b = as_bin_get(rd, (byte *)bname, blen);

	if ( !b ) {
		//Can't get bin
		cf_warning(AS_UDF, "as_bin_get failed");
		return -1;
	} else if (blen > (AS_ID_BIN_SZ - 1 ) || !as_bin_name_within_quota(rd->ns, (byte *)bname, blen)) {
		// Can't read bin
		cf_warning(AS_UDF, "bin name %s too big. Bin not added", bname);
		return -1;
	}

	SINDEX_BINS_SETUP(delbin, 1);
	int sindex_ret = AS_SINDEX_OK;

	bool has_sindex = as_sindex_ns_has_sindex(rd->ns);
	if (has_sindex) {
		sindex_ret = as_sindex_sbin_from_bin(rd->ns, as_index_get_set_name(rd->r, rd->ns), b, delbin);
	}

	int32_t i = as_bin_get_index(rd, (byte *)bname, blen);
	if (i != -1) {
		if (has_sindex) {
			tr->flag |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
			if (AS_SINDEX_OK ==  sindex_ret) {
				as_sindex_delete_by_sbin(rd->ns, as_index_get_set_name(rd->r, rd->ns), 1, delbin, rd);
				//TODO: Check the error code returned through sindex_ret. (like out of sync )
			}
		}
		as_bin_destroy(rd, i);
		rd->write_to_device = true;
	} else {
		cf_warning(AS_UDF, "deleting non-existing bin %s ignored", bname);
	}

	if (has_sindex) {
		as_sindex_sbin_freeall(delbin, 1);
	}

	return 0;
}

/*
 * Internal function: udf_aerospike_setbin
 *
 * Parameters:
 * 		r 		-- udf_record to be manipulated
 * 		bname 	-- name of the bin to be deleted
 *		val		-- value to be updated with
 *
 * Return value:
 * 		0  on success
 * 	   -1  on failure
 *
 * Description:
 * 		The function sets the bin with the name
 * 		passed in as parameter to the value, passed as the third parameter.
 * 		Before updating the bin, it is checked if the value can fit in the storage
 *
 * 		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		udf_aerospike__apply_update_atomic
 * 		In this function, if it fails at the time of update, the record is set
 * 		to rollback all the updates till this point. The case where it fails in
 * 		rollback is not handled.
 *
 * 		Side Notes:
 * 		i.	write_to_device will be set to true on a successful bin update.
 * 		If all the updates from udf_aerospike__apply_update_atomic (including this) are
 * 		successful, the record will be written to disk and reopened so that the rest of
 * 		sets of updates can be applied.
 *
 * 		ii.	If put in sindex fails, we do not handle it.
 *
 * 		TODO make sure anything goes into setbin only if the bin value is
 * 		          changed
 */
static const int
udf_aerospike_setbin(udf_record * urecord, const char * bname, const as_val * val, bool is_hidden)
{
	if (bname == NULL || bname[0] == 0 ) {
		cf_warning(AS_UDF, "no bin name supplied");
		return -1;
	}

	uint8_t type = as_val_type(val);
	if (is_hidden &&
			((type != AS_MAP) && (type != AS_LIST))) {
		cf_warning(AS_UDF, "Hidden %d Type Not allowed", type);
		return -3;
	}

	size_t          blen    = strlen(bname);
	as_storage_rd * rd      = urecord->rd;
	as_transaction *tr      = urecord->tr;
	as_index_ref  * index   = urecord->r_ref;

	as_bin * b = as_bin_get(rd, (byte *)bname, blen);

	if ( !b && (blen > (AS_ID_BIN_SZ - 1 )
				|| !as_bin_name_within_quota(rd->ns, (byte *)bname, blen)) ) {
		// Can't write bin
		cf_warning(AS_UDF, "bin name %s too big. Bin not added", bname);
		return -1;
	}
	if ( !b ) {
		// See if there's a free one, the hope is you will always find the bin because
		// you have already allocated bin space before calling this function.
		b = as_bin_create(index->r, rd, (byte *)bname, blen, 0);
		if (!b) {
			cf_warning(AS_UDF, "ERROR: udf_aerospike_setbin: as_bin_create: bin not found, something went really wrong!");
			return -1;
		}
	}

	SINDEX_BINS_SETUP(oldbin, 1);
	SINDEX_BINS_SETUP(newbin, 1);
	bool needs_sindex_delete = false;
	bool needs_sindex_put    = false;
	bool needs_sindex_update = false;
	bool has_sindex          = as_sindex_ns_has_sindex(rd->ns);

	if (has_sindex
			&& (as_sindex_sbin_from_bin(rd->ns,
					as_index_get_set_name(rd->r, rd->ns),
					b, oldbin) == AS_SINDEX_OK)) {
		needs_sindex_delete = true;
	}

	// we know we are doing an update now, make sure there is particle data,
	// set to be 1 wblock size now @TODO!
	uint32_t pbytes = 0;
	int ret = 0;
	if (!rd->ns->storage_data_in_memory && !urecord->particle_data) {
		urecord->particle_data = cf_malloc(rd->ns->storage_write_block_size);
		urecord->cur_particle_data = urecord->particle_data;
		urecord->end_particle_data = urecord->particle_data + rd->ns->storage_write_block_size;
	}

	cf_detail(AS_UDF, "udf_setbin: bin %s type %d ", bname, type );

	switch(type) {
		case AS_STRING: {
			as_string * v   = as_string_fromval(val);
			byte *      s   = (byte *) as_string_tostring(v);
			size_t      l   = as_string_len(v);

			// Save for later.
			// cf_detail(AS_UDF, "udf_setbin: string: binname %s value is %s",bname,s);

			if ( !as_storage_bin_can_fit(rd->ns, l) ) {
				cf_warning(AS_UDF, "string: bin size too big");
				ret = -1;
				break;
			}
			if (rd->ns->storage_data_in_memory) {
				as_particle_frombuf(b, AS_PARTICLE_TYPE_STRING, s, l, NULL, true);
			} else {
				pbytes = l + as_particle_get_base_size(AS_PARTICLE_TYPE_STRING);
				if ((urecord->cur_particle_data + pbytes) < urecord->end_particle_data) {
					as_particle_frombuf(b, AS_PARTICLE_TYPE_STRING, s, l,
										urecord->cur_particle_data,
										rd->ns->storage_data_in_memory);
					urecord->cur_particle_data += pbytes;
				} else {
					cf_warning(AS_UDF, "string: bin data size too big: pbytes %d"
								" pdata %p cur_part+pbytes %p pend %p", pbytes,
								urecord->particle_data, urecord->cur_particle_data + pbytes,
								urecord->end_particle_data);
					ret = -1;
					break;
				}
			}
			break;
		}
		case AS_BYTES: {
			as_bytes *  v   = as_bytes_fromval(val);
			uint8_t *   s   = as_bytes_get(v);
			size_t      l   = as_bytes_size(v);

			if ( !as_storage_bin_can_fit(rd->ns, l) ) {
				cf_warning(AS_UDF, "bytes: bin size too big");
				ret = -1;
				break;
			}
			if (rd->ns->storage_data_in_memory) {
				as_particle_frombuf(b, AS_PARTICLE_TYPE_BLOB, s, l, NULL, true);
			} else {
				pbytes = l + as_particle_get_base_size(AS_PARTICLE_TYPE_BLOB);
				if ((urecord->cur_particle_data + pbytes) < urecord->end_particle_data) {
					as_particle_frombuf(b, AS_PARTICLE_TYPE_BLOB, s, l, urecord->cur_particle_data,
										rd->ns->storage_data_in_memory);
					urecord->cur_particle_data += pbytes;
				} else {
					cf_warning(AS_UDF, "bytes: bin data size too big pbytes %d"
								" pdata %p cur_part+pbytes %p pend %p", pbytes,
								urecord->particle_data, urecord->cur_particle_data + pbytes,
								urecord->end_particle_data);
					ret = -1;
					break;
				}
			}
			break;
		}
		case AS_BOOLEAN: {
			as_boolean *    v   = as_boolean_fromval(val);
			bool            d   = as_boolean_get(v);
			int64_t         i   = __be64_to_cpup((void *)&d);

			if ( !as_storage_bin_can_fit(rd->ns, 8) ) {
				cf_warning(AS_UDF, "bool: bin size too big");
				ret = -1;
				break;
			}
			if (rd->ns->storage_data_in_memory) {
				as_particle_frombuf(b, AS_PARTICLE_TYPE_INTEGER, (uint8_t *) &i, 8, NULL, true);
			} else {
				pbytes = 8 + as_particle_get_base_size(AS_PARTICLE_TYPE_INTEGER);
				if ((urecord->cur_particle_data + pbytes) < urecord->end_particle_data) {
					as_particle_frombuf(b, AS_PARTICLE_TYPE_INTEGER,
										(uint8_t *) &i, 8,
										urecord->cur_particle_data,
										rd->ns->storage_data_in_memory);
					urecord->cur_particle_data += pbytes;
				} else {
					cf_warning(AS_UDF, "bool: bin data size too big: pbytes %d %p %p %p",
								pbytes, urecord->particle_data, urecord->cur_particle_data,
								urecord->end_particle_data);
					ret = -1;
					break;
				}
			}
			break;
		}
		case AS_INTEGER: {
			as_integer *    v   = as_integer_fromval(val);
			int64_t         i   = as_integer_get(v);
			int64_t         j   = __be64_to_cpup((void *)&i);

			if ( !as_storage_bin_can_fit(rd->ns, 8) ) {
				cf_warning(AS_UDF, "int: bin size too big");
				ret = -1;
				break;
			}
			if (rd->ns->storage_data_in_memory) {
				as_particle_frombuf(b, AS_PARTICLE_TYPE_INTEGER, (uint8_t *) &j, 8, NULL, true);
			} else {
				pbytes = 8 + as_particle_get_base_size(AS_PARTICLE_TYPE_INTEGER);
				if ((urecord->cur_particle_data + pbytes) < urecord->end_particle_data) {
					as_particle_frombuf(b, AS_PARTICLE_TYPE_INTEGER,
										(uint8_t *) &j, 8, urecord->cur_particle_data,
										rd->ns->storage_data_in_memory);
					urecord->cur_particle_data += pbytes;
				} else {
					cf_warning(AS_UDF, "int: bin data size too big: pbytes %d %p %p %p",
								pbytes, urecord->particle_data, urecord->cur_particle_data,
								urecord->end_particle_data);
					ret = -1;
					break;
				}
			}
			break;
		}
		// @LDT : Possibly include AS_LDT in this list.  We need the LDT
		// bins to be updated by LDT lua calls, and that path takes us thru here.
		// However, we ALSO need to be able to set the particle type for the
		// bins -- so that requires extra processing here to take the LDT flags
		// and set the appropriate bin flags in the particle data.
		case AS_MAP:
		case AS_LIST: {
			as_buffer buf;
			as_buffer_init(&buf);
			as_serializer s;
			as_msgpack_init(&s);
			int rsp = 0;
			as_serializer_serialize(&s, (as_val *) val, &buf);

			if ( !as_storage_bin_can_fit(rd->ns, buf.size) ) {
				cf_warning(AS_UDF, "map-list: bin size too big");
				ret = -1;
				// Clean Up and jump out.
				as_serializer_destroy(&s);
				as_buffer_destroy(&buf);
				break; // can't continue if value too big.
			}
			uint8_t ptype;
			if(is_hidden) {
				ptype = as_particle_type_convert_to_hidden(to_particle_type(type));
			} else {
				ptype = to_particle_type(type);
			}
			if (rd->ns->storage_data_in_memory) {
				as_particle_frombuf(b, ptype, (uint8_t *) buf.data, buf.size, NULL, true);
			}
			else {
				pbytes = buf.size + as_particle_get_base_size(ptype);
				if ((urecord->cur_particle_data + pbytes) < urecord->end_particle_data) {
					as_particle_frombuf(b, ptype, (uint8_t *) buf.data, buf.size,
										urecord->cur_particle_data,	rd->ns->storage_data_in_memory);
					urecord->cur_particle_data += pbytes;
				} else {
					cf_warning(AS_UDF, "map-list: bin data size too big: pbytes %d %p %p %p",
								pbytes, urecord->particle_data, urecord->cur_particle_data,
								urecord->end_particle_data);
					rsp = -1;
				}
			}
			as_serializer_destroy(&s);
			as_buffer_destroy(&buf);
			if (rsp) {
				ret = rsp;
				break;
			}
			break;
		}
		default: {
			cf_warning(AS_UDF, "unrecognized object type %d, skipping", as_val_type(val) );
			break;
		}

	}

	// If something fail bailout
	if (ret) {
		as_sindex_sbin_freeall(oldbin, 1);
		as_sindex_sbin_freeall(newbin, 1);
		return ret;
	}

	rd->write_to_device = true;

	// Update sindex if required
	if (has_sindex) {
		if (as_sindex_sbin_from_bin(rd->ns,
				as_index_get_set_name(rd->r, rd->ns), b, newbin) == AS_SINDEX_OK) {
			if (!as_sindex_sbin_match(newbin, oldbin)) {
				needs_sindex_put    = true;
			} else {
				needs_sindex_update = true;
			}
		}

		if (needs_sindex_update) {
			tr->flag |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
			as_sindex_delete_by_sbin(rd->ns,
					as_index_get_set_name(rd->r, rd->ns), 1, oldbin, rd);
			as_sindex_put_by_sbin(rd->ns,
					as_index_get_set_name(rd->r, rd->ns), 1, newbin, rd);
		} else {
			if (needs_sindex_delete) {
				tr->flag |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
				as_sindex_delete_by_sbin(rd->ns,
					as_index_get_set_name(rd->r, rd->ns), 1, oldbin, rd);
			}
			if (needs_sindex_put) {
				tr->flag |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
				as_sindex_put_by_sbin(rd->ns,
					as_index_get_set_name(rd->r, rd->ns), 1, newbin, rd);
			}
		}
		as_sindex_sbin_freeall(oldbin, 1);
		as_sindex_sbin_freeall(newbin, 1);
	}
	return ret;
} // end udf_aerospike_setbin()


/*
 * Internal function: udf_aerospike_storage_commit
 *
 * Parameters:
 * 		rec -- udf_record to be committed
 *
 * Return value:
 * 		None
 *
 * Description:
 * 		This function first flushes all the commits to the storage
 * 		by call as_storage_record_close and then reopens it for the next
 * 		sets of updates. The generation and ttl and memory accounting is
 * 		done in the later part of the code.
 *
 * 		Special Notes:
 * 		This code incurs 2 I/O cost, so UDF doing frequent updates will be slow
 * 		and the developers should be recommended against it
 *
 * 		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		udf_aerospike__execute_updates
 * 		In this function, if applying the set of udf updates atomically was successful,
 * 		udf_aerospike__storage_commit is called.
 */
void
udf_aerospike__storage_commit(udf_record *urecord)
{
	as_transaction 	* tr 	= urecord->tr;
	as_index_ref   	* r_ref	= urecord->r_ref;
	as_storage_rd  	* rd	= urecord->rd;

	cf_detail(AS_UDF, "Record successfully updated: starting memory bytes = %d",
			  urecord->starting_memory_bytes);
	write_local_post_processing(tr, rd->ns, NULL /*as_partition_reservation*/,
								NULL /*pickled_buf*/,
								NULL /*pickled_sz*/,
								NULL /*pickled_void_time*/,
								NULL /*p_pickled_rec_props*/,
								true,
								NULL /*write_local_generation*/,
								r_ref->r, rd, urecord->starting_memory_bytes);

	if (as_bin_inuse_has(urecord->rd)) {
		// Close it so it gets flushed to the storage. Make sure enough
		// checks are done upfront so if this call succeed, write to
		// the disk should succeed.
		udf_storage_record_close(urecord);
	} else {
		// if there are no bins then delete record from storage and
		// open a new one
		udf_storage_record_destroy(urecord);
		// open up new storage
		as_storage_record_create(urecord->tr->rsv.ns, urecord->r_ref->r,
			urecord->rd, &urecord->tr->keyd);
	}
	udf_storage_record_open(urecord);

	// Set updated flag to true
	urecord->flag |= UDF_RECORD_FLAG_HAS_UPDATES;
	if (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) {
		cf_detail(AS_UDF, "Subrecord has updates %d", urecord->flag);
	}
	cf_detail(AS_UDF, "Record successfully updated: ending memory bytes = %d",
		urecord->starting_memory_bytes);
	return;
}

/*
 * Internal function: udf_aerospike__apply_update_atomic
 *
 * Parameters:
 * 		rec --	udf_record to be updated
 *
 * Return Values:
 * 		 0 success
 * 		-1 failure
 *
 * Description:
 * 		This function applies all the updates atomically. That is,
 * 		if one of the bin update/delete/create fails, the entire function
 * 		will fail. If the nth update fails, all the n-1 updates are rolled
 * 		back to their initial values
 *
 * 		Special Notes:
 * 		i. The basic checks of bin name being too long or if there is enough space
 * 		on the disk for the bin values is done before allocating space for any
 * 		of the bins.
 *
 * 		ii. If one of the updates to be rolled back is a bin creation,
 * 		udf_aerospike_delbin is called. This will not free up the bin metadata.
 * 		So there will be a small memory mismatch b/w replica (which did not get the
 * 		record at all and hence no memory is accounted) and the master will be seen.
 * 		To avoid such cases, we are doing checks upfront.
 *
 * 		Callers:
 * 		udf_aerospike__execute_updates
 * 		In this function, if udf_aerospike__apply_update_atomic fails, the record
 * 		is not committed to the storage. On success, record is closed which commits to
 * 		the storage and reopened for the next set of udf updates.
 * 		The return value from udf_aerospike__apply_update_atomic is passed on to the
 * 		callers of this function.
 */
int
udf_aerospike__apply_update_atomic(udf_record *urecord)
{
	int rc 					= 0;
	int failindex 			= 0;
	int new_bins 			= 0;	// How many new bins have to be created in this update
	as_storage_rd * rd		= urecord->rd;

	// This will iterate over all the updates and apply them to storage.
	// The items will remain, and be used as cache values. If an error
	// occurred during setbin(), we rollback all the operation which
	// is and return failure
	cf_detail(AS_UDF, "execute updates: %d updates", urecord->nupdates);

	// loop twice to make sure the updates are performed first so in case
	// something wrong it can be rolled back. The deletes will go through
	// successfully generally.

	// In first iteration, just calculate how many new bins need to be created
	for(int i = 0; i < urecord->nupdates; i++ ) {
		if ( urecord->updates[i].dirty ) {
			char *      k = urecord->updates[i].name;
			if ( k != NULL ) {
				if ( !as_bin_get(rd, (uint8_t *)k, strlen(k)) ) {
					new_bins++;
				}
			}
		}
	}
	// Free bins - total bins not in use in the record
	// Delta bins - new bins that need to be created
	int free_bins  = urecord->rd->n_bins - as_bin_inuse_count(urecord->rd);
	int delta_bins = new_bins - free_bins;
	cf_detail(AS_UDF, "Total bins %d, In use bins %d, Free bins %d , New bins %d, Delta bins %d",
			  urecord->rd->n_bins, as_bin_inuse_count(urecord->rd), free_bins, new_bins, delta_bins);

	// Allocate space for all the new bins that need to be created beforehand
	if (delta_bins > 0 && rd->ns->storage_data_in_memory && ! rd->ns->single_bin) {
		as_bin_allocate_bin_space(urecord->r_ref->r, rd, delta_bins);
	}

	bool has_sindex = as_sindex_ns_has_sindex(rd->ns); 
	if (has_sindex) {
		SINDEX_GRLOCK();
	}

	// In second iteration apply updates.
	for(int i = 0; i < urecord->nupdates; i++ ) {
		if ( urecord->updates[i].dirty && rc == 0) {

			char *      k = urecord->updates[i].name;
			as_val *    v = urecord->updates[i].value;
			bool        h = urecord->updates[i].ishidden;
			urecord->updates[i].oldvalue  = NULL;
			urecord->updates[i].washidden = false;

			if ( k != NULL ) {
				if ( v == NULL || v->type == AS_NIL ) {
					// if the value is NIL, then do a delete
					cf_detail(AS_UDF, "execute update: position %d deletes bin %s", i, k);
					urecord->updates[i].oldvalue = udf_record_storage_get(urecord, k);
					urecord->updates[i].washidden = udf_record_bin_ishidden(urecord, k);
					rc = udf_aerospike_delbin(urecord, k);
					if (rc) {
						failindex = i;
						goto Rollback;
					}
				}
				else {
					// otherwise, it is a set
					cf_detail(AS_UDF, "execute update: position %d sets bin %s", i, k);
					urecord->updates[i].oldvalue = udf_record_storage_get(urecord, k);
					urecord->updates[i].washidden = udf_record_bin_ishidden(urecord, k);
					rc = udf_aerospike_setbin(urecord, k, v, h);
					if (rc) {
						failindex = i;
						goto Rollback;
					}
				}
			}
		}
	}
	
	if (has_sindex) {
		SINDEX_GUNLOCK();
	}

	for(int i = 0; i < urecord->nupdates; i++ ) {
		if ((urecord->updates[i].dirty)
				&& (urecord->updates[i].oldvalue)) {
			as_val_destroy(urecord->updates[i].oldvalue);
			cf_debug(AS_UDF, "REGULAR as_val_destroy()");
		}
	}
	return rc;

Rollback:
	cf_debug(AS_UDF, "Rollback Called: FailIndex(%d)", failindex);
	for(int i = 0; i <= failindex; i++) {
		if (urecord->updates[i].dirty) {
			char *      k = urecord->updates[i].name;
			// Pick the oldvalue for rollback
			as_val *    v = urecord->updates[i].oldvalue;
			bool        h = urecord->updates[i].washidden;
			if ( k != NULL ) {
				if ( v == NULL || v->type == AS_NIL ) {
					// if the value is NIL, then do a delete
					cf_detail(AS_UDF, "execute rollback: position %d deletes bin %s", i, k);
					rc = udf_aerospike_delbin(urecord, k);
				}
				else {
					// otherwise, it is a set
					cf_detail(AS_UDF, "execute rollback: position %d sets bin %s", i, k);
					rc = udf_aerospike_setbin(urecord, k, v, h);
					if (rc) {
						cf_warning(AS_UDF, "Rollback failed .. not good ... !!");
					}
				}
			}
			if (v) {
				as_val_destroy(v);
				cf_debug(AS_UDF, "ROLLBACK as_val_destroy()");
			}
		}
	}
	if (has_sindex) {
		SINDEX_GUNLOCK();
	}
	return -1;
}

/*
 * Internal function: udf_aerospike_execute_updates
 *
 * Parameters:
 * 		rec - udf record to be updated
 *
 * Return values
 * 		 0 on success
 *		-1 on failure
 *
 * Description:
 * 		Execute set of udf_record updates. If these updates are successfully
 * 		applied atomically, the storage record is closed (committed to the disk)
 * 		and reopened. The cache is freed up at the end.
 *
 * 		Callers:
 * 		udf_aerospike_rec_create, interface func - aerospike:create(r)
 * 		udf_aerospike_rec_update, interface func - aerospike:update(r)
 * 		udf_aerospike__execute_updates is the key function which is executed in these
 * 		functions. The return value is directly passed on to the lua.
 */
int
udf_aerospike__execute_updates(udf_record * urecord)
{
	int rc = 0;
	as_storage_rd *rd    = urecord->rd;
	as_index_ref * r_ref = urecord->r_ref;

	if ( urecord->nupdates == 0 ) {
		cf_detail(AS_UDF, "No Update when execute update is called");
		return 0;
	}

	// fail updates in case update is not allowed. Queries and scans do not
	// not allow updates. Updates will never be true .. just being paranoid
	if (!(urecord->flag & UDF_RECORD_FLAG_ALLOW_UPDATES)) {
		// TODO: set the error message
		cf_warning(AS_UDF, "Udf: execute updates: allow updates false; FAIL");
		return -1;
	}

	// Commit semantics is either all the update make it or none of it
	rc = udf_aerospike__apply_update_atomic(urecord);

	// allocate down if bins are deleted / not in use
	if (rd->ns && rd->ns->storage_data_in_memory && ! rd->ns->single_bin) {
		int32_t delta_bins = (int32_t)as_bin_inuse_count(rd) - (int32_t)rd->n_bins;
		if (delta_bins) {
			as_bin_allocate_bin_space(r_ref->r, rd, delta_bins);
		}
	}


	// Commit to storage if apply was successful if not nothing would
	// have made it
	if (!rc) {
		// Before committing to storage set the rec_type_bits ..
		cf_detail(AS_RW, "TO INDEX              Digest=%"PRIx64" bits %d %p",
				*(uint64_t *)&urecord->tr->keyd.digest[8], urecord->ldt_rectype_bits, urecord);
		as_index_set_flags(r_ref->r, urecord->ldt_rectype_bits);

		// commit to the storage
		udf_aerospike__storage_commit(urecord);
	}

	// Clean up cache and start from 0 update again. All the changes
	// made here will if flush from write buffer to storage goes
	// then will never be backed out.
	udf_record_cache_free(urecord);
	return rc;
}

as_aerospike *
udf_aerospike_new()
{
	return as_aerospike_new(NULL, &udf_aerospike_hooks);
}

as_aerospike *
udf_aerospike_init(as_aerospike * as)
{
	return as_aerospike_init(as, NULL, &udf_aerospike_hooks);
}

static void
udf_aerospike_destroy(as_aerospike * as)
{
	as_aerospike_destroy(as);
}

static cf_clock
udf_aerospike_get_current_time(const as_aerospike * as)
{
	return cf_clock_getabsolute();
}

/**
 * aerospike::create(record)
 * Function: udf_aerospike_rec_create
 *
 * Parameters:
 * 		as - as_aerospike
 *		rec - as_rec
 *
 * Return Values:
 * 		1 if record is being read or on a create, it already exists
 * 		o/w return value of udf_aerospike__execute_updates
 *
 * Description:
 * 		Create a new record in local storage.
 * 		The record will only be created if it does not exist.
 * 		This assumes the record has a digest that is valid for local storage.
 *
 *		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		lua interfacing function, mod_lua_aerospike_rec_create
 * 		The return value of udf_aerospike_rec_create is pushed on to the lua stack
 *
 * 		Notes:
 * 		The 'read' and 'exists' flag of udf_record are set to true.
*/
static int
udf_aerospike_rec_create(const as_aerospike * as, const as_rec * rec)
{
	if (!as || !rec) {
		cf_warning(AS_UDF, " Invalid Paramters: as=%p, record=%p", as, rec);
		return 2;
	}

	udf_record * urecord  = (udf_record *) as_rec_source(rec);
	if (!urecord) {
		return 2;
	}

	// make sure record isn't already successfully read
	if (urecord->flag & UDF_RECORD_FLAG_OPEN) {
		cf_detail(AS_UDF, "udf_aerospike_rec_create: Record Already Exists");
		return 1;
	}
	as_transaction *tr    = urecord->tr;
	as_index_ref   *r_ref = urecord->r_ref;
	as_storage_rd  *rd    = urecord->rd;
	as_index_tree  *tree  = tr->rsv.tree;

	if (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) {
		tree      = tr->rsv.sub_tree;
	}

	// make sure we got the record as a create
	int rv = as_record_get_create(tree, &tr->keyd, r_ref, tr->rsv.ns);
	cf_detail_digest(AS_UDF, &tr->keyd, "Creating %sRecord",
			(urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD) ? "Sub" : "");

	// rv 0 means record exists, 1 means create, < 0 means fail
	// TODO: Verify correct result codes.
	if (rv == 0) {
		cf_warning(AS_UDF, "udf_aerospike_rec_create: Record Already Exists 2");
		as_record_done(r_ref, tr->rsv.ns);
		bzero(r_ref, sizeof(as_index_ref));
		return 1;
	} else if (rv < 0) {
		cf_warning(AS_UDF, "udf_aerospike_rec_create: Record Open Failed with rv=%d", rv);
		return rv;
	}

	// Associates the set name with the storage rec and index
	if(tr->msgp) {
		// Set the set name to index and close record if the setting the set name
		// is not successful
		int rv_set = as_record_set_set_from_msg(r_ref->r, tr->rsv.ns, &tr->msgp->msg);
		if (rv_set != 0) {
			cf_warning(AS_UDF, "udf_aerospike_rec_create: Failed to set setname");
			as_record_done(r_ref, tr->rsv.ns);
			// TODO bzero is expensive. Switch to use flag.
			bzero(r_ref, sizeof(as_index_ref));
			return 4;
		}
	}

	urecord->flag |= UDF_RECORD_FLAG_OPEN;
	cf_detail(AS_UDF, "Open %p %x %"PRIx64"", urecord, urecord->flag, *(uint64_t *)&tr->keyd);

	as_index *r    = r_ref->r;
	// open up storage
	as_storage_record_create(urecord->tr->rsv.ns, urecord->r_ref->r,
		urecord->rd, &urecord->tr->keyd);

	cf_detail(AS_UDF, "as_storage_record_create: udf_aerospike_rec_create: r %p rd %p",
		urecord->r_ref->r, urecord->rd);

	// if multibin storage, we will use urecord->stack_bins, so set the size appropriately
	if ( ! rd->ns->storage_data_in_memory && ! rd->ns->single_bin ) {
		rd->n_bins = sizeof(urecord->stack_bins) / sizeof(as_bin);
	}

	// side effect: will set the unused bins to properly unused
	rd->bins       = as_bin_get_all(r, rd, urecord->stack_bins);
	urecord->flag |= UDF_RECORD_FLAG_STORAGE_OPEN;

	cf_detail(AS_UDF, "Storage Open %p %x %"PRIx64"", urecord, urecord->flag, *(uint64_t *)&tr->keyd);
	cf_detail(AS_UDF, "udf_aerospike_rec_create: Record created %d", urecord->flag);

	int rc         = udf_aerospike__execute_updates(urecord);
	if(rc) {
		//  Creating the udf record failed, destroy the as_record
		if (!as_bin_inuse_has(urecord->rd)) {
			udf_aerospike_rec_remove(as, rec);
		}
	}
	return rc;
}

/**
 * aerospike::update(record)
 * Function: udf_aerospike_rec_update
 *
 * Parameters:
 *
 * Return Values:
 * 		-2 if record does not exist
 * 		o/w return value of udf_aerospike__execute_updates
 *
 * Description:
 * 		Updates an existing record in local storage.
 * 		The record will only be updated if it exists.
 *
 *		Synchronization : object lock acquired by the transaction thread executing UDF.
 * 		Partition reservation takes place just before the transaction starts executing
 * 		( look for as_partition_reserve_udf in thr_tsvc.c )
 *
 * 		Callers:
 * 		lua interfacing function, mod_lua_aerospike_rec_update
 * 		The return value of udf_aerospike_rec_update is pushed on to the lua stack
 *
 * 		Notes:
 * 		If the record does not exist or is not read by anyone yet, we cannot
 * 		carry on with the update. 'exists' and 'set' are set to false on record
 * 		init or record remove.
*/
static int
udf_aerospike_rec_update(const as_aerospike * as, const as_rec * rec)
{
	if (!as || !rec) {
		cf_warning(AS_UDF, "Invalid Paramters: as=%p, record=%p", as, rec);
		return 2;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);

	// make sure record exists and is already opened up
	if (!urecord || !(urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN)
			|| !(urecord->flag & UDF_RECORD_FLAG_OPEN) ) {
		cf_warning(AS_UDF, "Record not found to be open while updating %d", urecord->flag);
		return -2;
	}
	cf_detail_digest(AS_UDF, &urecord->rd->r->key, "Executing Updates");
	return udf_aerospike__execute_updates(urecord);
}

/**
 * Function udf_aerospike_rec_exists
 *
 * Parameters:
 *
 * Return Values:
 * 		1 if record exists
 * 		0 o/w
 *
 * Description:
 * Check to see if the record exists
 */
static int
udf_aerospike_rec_exists(const as_aerospike * as, const as_rec * rec)
{
	if (!as || !rec) {
		cf_warning(AS_UDF, "Invalid Paramters: as=%p, record=%p", as, rec);
		return 2;
	}

	udf_record * urecord = (udf_record *) as_rec_source(rec);

	return (urecord && (urecord->flag & UDF_RECORD_FLAG_OPEN)) ? true : false;
}

/*
 * Function: udf_aerospike_rec_remove
 *
 * Parameters:
 *
 * Return Values:
 *		1 if record does not exist
 *		0 on success
 *
 * Description:
 * Removes an existing record from local storage.
 * The record will only be removed if it exists.
 */
static int
udf_aerospike_rec_remove(const as_aerospike * as, const as_rec * rec)
{
	if (!as || !rec) {
		cf_warning(AS_UDF, "Invalid Paramters: as=%p, record=%p", as, rec);
		return 2;
	}
	udf_record * urecord = (udf_record *) as_rec_source(rec);

	// make sure record is already exists before removing it
	if (!urecord || !(urecord->flag & UDF_RECORD_FLAG_OPEN)) {
		return 1;
	}

	as_index_tree *tree  = urecord->tr->rsv.tree;
	// remove index from tree. Will decrement ref count, but object still retains
	if (urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD)
		tree = urecord->tr->rsv.sub_tree;

	// Reset starting memory bytes in case the same record is created again
	// in the same UDF
	as_index_delete(tree, &urecord->tr->keyd);
	urecord->starting_memory_bytes = 0;

	// Close the storage record associates with this UDF record
	// do not release the reservation yet !!
	udf_record_close(urecord, false);
	return 0;
}

/**
 * Writes a log message
 */
static int
udf_aerospike_log(const as_aerospike * a, const char * file, const int line, const int lvl, const char * msg)
{
	cf_fault_event(AS_UDF, lvl, file, NULL, line, (char *) msg);
	return 0;
}

// Would someone please explain the structure of these hooks?  Why are some null?
const as_aerospike_hooks udf_aerospike_hooks = {
	.rec_create       = udf_aerospike_rec_create,
	.open_subrec      = NULL,
	.close_subrec     = NULL,
	.update_subrec    = NULL,
	.create_subrec    = NULL,
	.rec_update       = udf_aerospike_rec_update,
	.rec_remove       = udf_aerospike_rec_remove,
	.rec_exists       = udf_aerospike_rec_exists,
	.log              = udf_aerospike_log,
	.get_current_time = udf_aerospike_get_current_time,
	.destroy          = udf_aerospike_destroy
};
