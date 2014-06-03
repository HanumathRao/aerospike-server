/*
 * udf_rw.c
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
 * User Defined Function execution engine
 *
 */

#include "base/udf_rw.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_buffer.h"
#include "aerospike/as_module.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_types.h"
#include "aerospike/mod_lua.h"

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"

#include "clock.h"
#include "fault.h"
#include "hist_track.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/ldt_aerospike.h"
#include "base/ldt_record.h"
#include "base/proto.h"
#include "base/rec_props.h"
#include "base/thr_rw_internal.h"
#include "base/thr_scan.h"
#include "base/thr_write.h"
#include "base/transaction.h"
#include "base/udf_aerospike.h"
#include "base/udf_arglist.h"
#include "base/udf_cask.h"
#include "base/udf_logger.h"
#include "base/udf_memtracker.h"
#include "base/udf_timer.h"
#include "base/write_request.h"

as_aerospike g_as_aerospike;

extern udf_call *as_query_get_udf_call(void *ptr);

/* Internal Function: Packs up passed in data into as_bin which is
 *                    used to send result after the UDF execution.
 */
static bool
make_send_bin(as_namespace *ns, as_bin *bin, uint8_t **sp_pp, uint sp_sz,
			  const char *key, size_t klen, int  vtype,  void *val, size_t vlen)
{
	uint        sz          = 0;
	int         tsz         = sz + vlen + as_particle_get_base_size(vtype);
	uint8_t *   v           = NULL;
	int64_t     swapped_int = 0;
	uint8_t     *sp_p = *sp_pp;

	if (tsz > sp_sz) {
		sp_p = cf_malloc(tsz);
		if (!sp_p) {
			cf_warning(AS_UDF, "data too much. malloc failed. going down. bin %s not sent back", key);
			return(-1);
		}
	}

	as_bin_init(ns, bin, (byte *) key/*name*/, klen/*namelen*/, 0/*version*/);

	switch (vtype) {
		case AS_PARTICLE_TYPE_NULL:
		{
			v = NULL;
			break;
		}
		case AS_PARTICLE_TYPE_INTEGER:
		{
			if (vlen != 8) {
				cf_crash(AS_UDF, "unexpected int %d", vlen);
			}
			swapped_int = __be64_to_cpup(val);
			v = (uint8_t *) &swapped_int;
			break;
		}
		case AS_PARTICLE_TYPE_BLOB:
		case AS_PARTICLE_TYPE_STRING:
		case AS_PARTICLE_TYPE_LIST:
		case AS_PARTICLE_TYPE_MAP:
			v = val;
			break;
		default:
		{
			cf_warning(AS_UDF, "unrecognized object type %d ignored", vtype);
			return -1;
		}
	}

	as_particle_frombuf(bin, vtype, v, vlen, sp_p, ns->storage_data_in_memory);
	*sp_pp = sp_p;
	return 0;
}

/* Internal Function: Workhorse function to send response back to the client
 * 					  after UDF execution.
 *
 * caller:
 * 		send_success
 * 		send_failure
 *
 * Assumption: The call should be setup properly pointing to the tr.
 *
 * Special Handling: If it is background scan udf job do not sent any
 * 					 response to client
 * 					 If it is scan job ...do not cleanup the fd it will
 * 					 be done by the scan thread after scan is finished
 */
static int
send_response(udf_call *call, const char *key, size_t klen, int vtype, void *val,
			  size_t vlen)
{
	as_transaction *    tr          = call->transaction;
	as_namespace *      ns          = tr->rsv.ns;
	uint32_t            generation  = tr->generation;
	uint                sp_sz       = 1024 * 16;
	uint32_t            void_time   = 0;
	uint                written_sz  = 0;
	bool                keep_fd     = false;
	as_bin              stack_bin;
	as_bin            * bin         = &stack_bin;

	// space for the stack particles
	uint8_t             stack_particle_buf[sp_sz];
	uint8_t *           sp_p        = stack_particle_buf;

	if (call->udf_type == AS_SCAN_UDF_OP_BACKGROUND) {
		// If we are doing a background UDF scan, do not send any result back
		cf_detail(AS_UDF, "UDF: Background transaction, send no result back. "
				  "Parent job id [%"PRIu64"]", ((tscan_job*)(tr->udata.req_udata))->tid);
		if(strncmp(key, "FAILURE", 8) == 0)  {
			cf_atomic_int_incr(&((tscan_job*)(tr->udata.req_udata))->n_obj_udf_failed);
		} else if(strncmp(key, "SUCCESS", 8) == 0) {
			cf_atomic_int_incr(&((tscan_job*)(tr->udata.req_udata))->n_obj_udf_success);
		}
		return 0;
	} else if(call->udf_type == AS_SCAN_UDF_OP_UDF) {
		// Do not release fd now, scan will do it at the end of all internal
		// 	udf transactions
		cf_detail(AS_UDF, "UDF: Internal udf transaction, do not release fd");
		keep_fd = true;
	}

	if (0 != make_send_bin(ns, bin, &sp_p, sp_sz, key, klen, vtype, val, vlen)) {
		return(-1);
	}

	// this is going to release the file descriptor
	if (keep_fd && tr->proto_fd_h) cf_rc_reserve(tr->proto_fd_h);

	single_transaction_response(
		tr, ns, NULL/*ops*/, &bin, 1,
		generation, void_time, &written_sz, NULL);

	// clean up.
	// TODO: check: is bin_inuse valid only when data_in_memory?
	// There must be another way to determine if the particle is used?
	if ( as_bin_inuse(bin) ) {
		as_particle_destroy(&stack_bin, ns->storage_data_in_memory);
	}

	if (sp_p != stack_particle_buf) {
		cf_free(sp_p);
	}
	return 0;
}

static inline int
send_failure(udf_call *call, int vtype, void *val, size_t vlen)
{
	call->transaction->result_code = AS_PROTO_RESULT_FAIL_UDF_EXECUTION;
	return send_response(call, "FAILURE", 7, vtype, val, vlen);
}

static inline int
send_success(udf_call *call, int vtype, void *val, size_t vlen)
{
	return send_response(call, "SUCCESS", 7, vtype, val, vlen);
}

/*
 * Internal Function: Entry function from UDF code path to send
 * 					  success result to the caller. Performs
 * 					  value translation.
 */
void
send_result(as_result * res, udf_call * call, void *udata)
{
	as_val * v = res->value;
	if ( res->is_success ) {

		if ( cf_context_at_severity(AS_UDF, CF_DETAIL) ) {
			char * str = as_val_tostring(v);
			cf_detail(AS_UDF, "SUCCESS: %s", str);
			cf_free(str);
		}

		if ( v != NULL ) {
			switch( as_val_type(v) ) {
				case AS_NIL:
				{
					send_success(call, AS_PARTICLE_TYPE_NULL, NULL, 0);
					break;
				}
				case AS_BOOLEAN:
				{
					as_boolean * b = as_boolean_fromval(v);
					int64_t bi = as_boolean_tobool(b) == true ? 1 : 0;
					send_success(call, AS_PARTICLE_TYPE_INTEGER, &bi, 8);
					break;
				}
				case AS_INTEGER:
				{
					as_integer * i = as_integer_fromval(v);
					int64_t ri = as_integer_toint(i);
					send_success(call, AS_PARTICLE_TYPE_INTEGER, &ri, 8);
					break;
				}
				case AS_STRING:
				{
					// this looks bad but it just pulls the pointer
					// out of the object
					as_string * s = as_string_fromval(v);
					char * rs = (char *) as_string_tostring(s);
					send_success(call, AS_PARTICLE_TYPE_STRING, rs, as_string_len(s));
					break;
				}
				case AS_BYTES:
				{
					as_bytes * b = as_bytes_fromval(v);
					uint8_t * rs = as_bytes_get(b);
					send_success(call, AS_PARTICLE_TYPE_BLOB, rs, as_bytes_size(b));
					break;
				}
				case AS_MAP:
				case AS_LIST:
				{
					as_buffer buf;
					as_buffer_init(&buf);

					as_serializer s;
					as_msgpack_init(&s);

					as_serializer_serialize(&s, v, &buf);

					send_success(call, to_particle_type(as_val_type(v)), buf.data, buf.size);
					// Not needed stack allocated - unless serialize has internal state
					// as_serializer_destroy(&s);
					as_buffer_destroy(&buf);
					break;
				}
				default:
				{
					cf_debug(AS_UDF, "SUCCESS: VAL TYPE UNDEFINED %d\n", as_val_type(v));
					send_success(call, AS_PARTICLE_TYPE_STRING, NULL, 0);
					break;
				}
			}
		} else {
			send_success(call, AS_PARTICLE_TYPE_NULL, NULL, 0);
		}
	} else { // Else -- NOT success
		as_string * s   = as_string_fromval(v);
		char *      rs  = (char *) as_string_tostring(s);

		cf_warning(AS_UDF, "FAILURE when calling %s %s %s", call->filename, call->function, rs);
		send_failure(call, AS_PARTICLE_TYPE_STRING, rs, as_string_len(s));
	}
}

/**
 * Initialize a new UDF. This populates the udf_call from information
 * in the current transaction. If passed in transaction has req_data it is
 * assumed to be internal and the UDF information is picked from the udata
 * associated with it. TODO: Do not overload please define flag for this.
 *
 *
 * Parameter:
 * 		tr the transaction to build a udf_call from
 *
 * Returns"
 * 		return a new udf_call (Caller needs to free it up)
 * 		NULL in case of failure
 */
int
udf_call_init(udf_call * call, as_transaction * tr)
{

	call->active   = false;
	call->udf_type = AS_SCAN_UDF_NONE;
	as_msg_field *  filename = NULL;
	as_msg_field *  function = NULL;
	as_msg_field *  arglist =  NULL;

	if (tr->udata.req_udata) {
		udf_call *ucall = NULL;
		if (tr->udata.req_type == UDF_SCAN_REQUEST) {
			ucall = &((tscan_job *)(tr->udata.req_udata))->call;
		} else if (tr->udata.req_type == UDF_QUERY_REQUEST) {
			ucall = as_query_get_udf_call(tr->udata.req_udata);
		}

		if (ucall) {
			strncpy(call->filename, ucall->filename, sizeof(ucall->filename));
			strncpy(call->function, ucall->function, sizeof(ucall->function));
			call->transaction = tr;
			call->active      = true;
			call->arglist     = ucall->arglist;
			call->udf_type    = ucall->udf_type;
			if (tr->udata.req_type == UDF_SCAN_REQUEST) {
				cf_atomic_int_incr(&g_config.udf_scan_rec_reqs);
			} else if (tr->udata.req_type == UDF_QUERY_REQUEST) {
				cf_atomic_int_incr(&g_config.udf_query_rec_reqs);
			}
		}
		// TODO: return proper macros
		return 0;
	}

	// Check the type of udf
	as_msg_field *  op = NULL;
	op = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_OP);
	if (!op) {
		// Normal udf operation, no special type
		call->udf_type = 0;
	} else {
		// We got a udf type from the server
		byte optype;
		memcpy(&optype, (byte *)op->data, sizeof(optype));
		if(optype == AS_SCAN_UDF_OP_UDF) {
			cf_debug(AS_UDF, "UDF scan op received");
			call->udf_type = AS_SCAN_UDF_OP_UDF;
		} else if(optype == AS_SCAN_UDF_OP_BACKGROUND) {
			cf_debug(AS_UDF, "UDF scan background op received");
			call->udf_type = AS_SCAN_UDF_OP_BACKGROUND;
		} else {
			cf_warning(AS_UDF, "Undefined udf type received over protocol");
			goto Cleanup;
		}
	}
	filename = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_FILENAME);
	if ( filename ) {
		function = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_FUNCTION);
		if ( function ) {
			arglist = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_UDF_ARGLIST);
			if ( arglist ) {
				call->transaction = tr;
				as_msg_field_get_strncpy(filename, &call->filename[0], sizeof(call->filename));
				as_msg_field_get_strncpy(function, &call->function[0], sizeof(call->function));
				call->arglist = arglist;
				call->active = true;
				cf_detail(AS_UDF, "UDF Request Unpacked %s %s", call->filename, call->function);
				return 0;
			}
		}
	}
Cleanup:
	call->transaction = NULL;
	call->filename[0] = 0;
	call->function[0] = 0;
	call->arglist = NULL;

	return 1;
}

/*
 * Cleans up udf call
 *
 * Returns: 0 on success
 */
void
udf_call_destroy(udf_call * call)
{
	call->transaction = NULL;
	call->arglist = NULL;
}

/* Internal Function: Does the post processing for the UDF record after the
 *					  UDF execution. Does the following:
 *		1. Record is closed
 *		2. urecord_op is updated to delete in case there is no bin left in it.
 *		3. record->pickled_buf is populated before the record is close in case
 *		   it was write operation
 *		4. UDF updates cache is cleared
 *
 *	Returns: Nothing
 *
 *	Parameters: urecord          - UDF record to operate on
 *				urecord_op (out) - Populated with the optype
 */
void
udf_rw_post_processing(udf_record *urecord, udf_optype *urecord_op)
{
	as_storage_rd      *rd   = urecord->rd;
	as_transaction     *tr   = urecord->tr;
	as_index_ref    *r_ref   = urecord->r_ref;

	// INIT
	urecord->pickled_buf     = NULL;
	urecord->pickled_sz      = 0;
	urecord->pickled_void_time     = 0;
	as_rec_props_clear(&urecord->pickled_rec_props);

	// TODO: optimize not to allocate buffer if it is single
	// node cluster. No remote to send data to
	if (urecord->flag & UDF_RECORD_FLAG_HAS_UPDATES) {
		*urecord_op  = UDF_OPTYPE_WRITE;
	} else if ((urecord->flag & UDF_RECORD_FLAG_PREEXISTS)
			   && !(urecord->flag & UDF_RECORD_FLAG_OPEN)) {
		*urecord_op  = UDF_OPTYPE_DELETE;
	} else {
		*urecord_op  = UDF_OPTYPE_READ;
	}

	cf_detail(AS_UDF, "FINISH working with LDT Record %p %p %p %p %d", &urecord,
			urecord->tr, urecord->r_ref, urecord->rd,
			(urecord->flag & UDF_RECORD_FLAG_STORAGE_OPEN));

	// If there exists a record reference but no bin of the record is in use,
	// delete the record. remove from the tree. Only LDT_RECORD here not needed
	// for LDT_SUBRECORD (only do it if requested by UDF). All the SUBRECORD of
	// removed LDT_RECORD will be lazily cleaned up by defrag.
	if (!(urecord->flag & UDF_RECORD_FLAG_IS_SUBRECORD)
			&& urecord->flag & UDF_RECORD_FLAG_OPEN
			&& !as_bin_inuse_has(rd)) {
		as_index_delete(tr->rsv.tree, &tr->keyd);
		urecord->starting_memory_bytes = 0;
		*urecord_op                    = UDF_OPTYPE_DELETE;
	} else if (*urecord_op == UDF_OPTYPE_WRITE) {
		cf_detail(AS_UDF, "Committing Changes %"PRIx64" n_bins %d", rd->keyd, as_bin_get_n_bins(r_ref->r, rd));

		// TODO - This probably gets done again just before the eventual call of
		// as_storage_record_close(). We may want to optimize by doing it once,
		// soon after as_storage_record_open(), instead.
		as_storage_record_get_key(rd);
		size_t  rec_props_data_size = as_storage_record_rec_props_size(rd);
		uint8_t rec_props_data[rec_props_data_size];
		if (rec_props_data_size > 0) {
			as_storage_record_set_rec_props(rd, rec_props_data);
		}

		// Generation is already updated, increment_generation should be false
		write_local_post_processing(tr, tr->rsv.ns, NULL, &urecord->pickled_buf,
			&urecord->pickled_sz, &urecord->pickled_void_time,
			&urecord->pickled_rec_props, false /*increment_generation*/,
			NULL, r_ref->r, rd, urecord->starting_memory_bytes);
	}

	// get set-id and generation before record-close
	as_generation generation = 0;
	uint16_t set_id = INVALID_SET_ID;


	// Collect the record information (for XDR) before closing the record
	//
	bool udf_xdr_ship_op = false;
	if ( (urecord->flag & UDF_RECORD_FLAG_OPEN)
			&& (tr->rsv.ns->ldt_enabled == false) &&
			(UDF_OP_IS_LDT(*urecord_op) == false)) {
		generation = r_ref->r->generation;
		set_id = as_index_get_set_id(r_ref->r);
		udf_xdr_ship_op = true;
	}
	// Close the record for all the cases
	udf_record_close(urecord, false);

	// Write to XDR pipe after closing the record, in order to release the record lock as
	// early as possible.
	if (udf_xdr_ship_op == true) {
		if (UDF_OP_IS_WRITE(*urecord_op)) {
			cf_detail(AS_UDF, "UDF write shipping for key %" PRIx64, tr->keyd);
			xdr_write(tr->rsv.ns, tr->keyd, generation, 0, false, set_id);
		} else if (UDF_OP_IS_DELETE(*urecord_op)) {
			cf_detail(AS_UDF, "UDF delete shipping for key %" PRIx64, tr->keyd);
			xdr_write(tr->rsv.ns, tr->keyd, generation, 0, true, set_id);
		}
	}

	// Replication happens when the main record replicates
	if (urecord->particle_data) {
		cf_free(urecord->particle_data);
		urecord->particle_data = 0;
	}
	udf_record_cache_free(urecord);
}

/*
 * Function based on the UDF result and the result of UDF call along
 * with the optype information update the UDF stats and LDT stats.
 *
 * Parameter:
 *  	op:           execute optype
 *  	is_success :  In case the UDF operation was successful
 *  	ret        :  return value of UDF execution
 *
 *  Returns: nothing
*/
void
udf_rw_update_stats(as_namespace *ns, udf_optype op, int ret, bool is_success)
{
	if (UDF_OP_IS_LDT(op)) {
		if (UDF_OP_IS_READ(op))        cf_atomic_int_incr(&ns->ldt_read_reqs);
		else if (UDF_OP_IS_DELETE(op)) cf_atomic_int_incr(&ns->ldt_delete_reqs);
		else if (UDF_OP_IS_WRITE (op)) cf_atomic_int_incr(&ns->ldt_write_reqs);

		if (ret == 0) {
			if (is_success) {
				if (UDF_OP_IS_READ(op))        cf_atomic_int_incr(&ns->ldt_read_success);
				else if (UDF_OP_IS_DELETE(op)) cf_atomic_int_incr(&ns->ldt_delete_success);
				else if (UDF_OP_IS_WRITE (op)) cf_atomic_int_incr(&ns->ldt_write_success);
			} else {
				cf_atomic_int_incr(&ns->ldt_errs);
			}
		} else {
			cf_atomic_int_incr(&g_config.udf_lua_errs);
		}
	} else {
		if (UDF_OP_IS_READ(op))        cf_atomic_int_incr(&g_config.udf_read_reqs);
		else if (UDF_OP_IS_DELETE(op)) cf_atomic_int_incr(&g_config.udf_delete_reqs);
		else if (UDF_OP_IS_WRITE (op)) cf_atomic_int_incr(&g_config.udf_write_reqs);

		if (ret == 0) {
			if (is_success) {
				if (UDF_OP_IS_READ(op))        cf_atomic_int_incr(&g_config.udf_read_success);
				else if (UDF_OP_IS_DELETE(op)) cf_atomic_int_incr(&g_config.udf_delete_success);
				else if (UDF_OP_IS_WRITE (op)) cf_atomic_int_incr(&g_config.udf_write_success);
			} else {
				if (UDF_OP_IS_READ(op))        cf_atomic_int_incr(&g_config.udf_read_errs_other);
				else if (UDF_OP_IS_DELETE(op)) cf_atomic_int_incr(&g_config.udf_delete_errs_other);
				else if (UDF_OP_IS_WRITE (op)) cf_atomic_int_incr(&g_config.udf_write_errs_other);
			}
		} else {
            cf_info(AS_UDF,"lua error, ret:%d",ret);
			cf_atomic_int_incr(&g_config.udf_lua_errs);
		}
	}
}

/*
 *  Write the record to the storage in case there are write and closes the
 *  record and frees up the stuff. With the pickled buf for each udf_record
 *  it create single pickled buf for the entire LDT to be sent to the remote
 *  for replica.
 *
 *  Parameter:
 *  	lrecord : LDT record to operate on
 *  	pickled_* (out) to be populated is null if there was delete
 *		lrecord_op (out) is set properly for the entire ldt
 *
 *  Returns: true always
 */
bool
udf_rw_finish(ldt_record *lrecord, write_request *wr, udf_optype * lrecord_op)
{
	// LDT: Commit all the changes being done to the all records.
	// TODO: remove limit of 6 (note -- it's temporarily up to 20)
	udf_optype urecord_op = UDF_OPTYPE_READ;
	*lrecord_op           = UDF_OPTYPE_READ;
	udf_record *h_urecord = as_rec_source(lrecord->h_urec);
	bool is_ldt           = false;

	udf_rw_post_processing(h_urecord, &urecord_op);

	if (urecord_op == UDF_OPTYPE_DELETE) {
		wr->pickled_buf      = NULL;
		wr->pickled_sz       = 0;
		wr->pickled_void_time      = 0;
		as_rec_props_clear(&wr->pickled_rec_props);
		wr->ldt_rectype_bits = h_urecord->ldt_rectype_bits;
		*lrecord_op  = UDF_OPTYPE_DELETE;
	} else {

		if (urecord_op == UDF_OPTYPE_WRITE) {
			*lrecord_op = UDF_OPTYPE_WRITE;
		}

		FOR_EACH_SUBRECORD(i, lrecord) {
			is_ldt = true;
			udf_record *c_urecord = &lrecord->chunk[i].c_urecord;
			udf_rw_post_processing(c_urecord, &urecord_op);
			if (urecord_op == UDF_OPTYPE_WRITE) {
				*lrecord_op = UDF_OPTYPE_LDT_WRITE;
			}
		}

		if (is_ldt) {
			// Create the multiop pickled buf for thr_rw.c
			ldt_record_pickle(lrecord, &wr->pickled_buf, &wr->pickled_sz, &wr->pickled_void_time);
			FOR_EACH_SUBRECORD(i, lrecord) {
				udf_record *c_urecord = &lrecord->chunk[i].c_urecord;
				udf_record_cleanup(c_urecord, false);
			}
		} else {
			// Normal UDF case simply pass on pickled buf created for the record
			wr->pickled_buf       = h_urecord->pickled_buf;
			wr->pickled_sz        = h_urecord->pickled_sz;
			wr->pickled_void_time = h_urecord->pickled_void_time;
			wr->pickled_rec_props = h_urecord->pickled_rec_props;
			wr->ldt_rectype_bits = h_urecord->ldt_rectype_bits;
			udf_record_cleanup(h_urecord, false);
		}
	}
	udf_record_cleanup(h_urecord, false);
	return true;
}

/*
 * UDF time tracker hook
 */
uint64_t
as_udf__end_time(time_tracker *tt)
{
	ldt_record *lr  = (ldt_record *) tt->udata;
	if (!lr) return -1;
	udf_record *r   = (udf_record *) as_rec_source(lr->h_urec);
	if (!r)  return -1;
	// If user has not specified timeout pick the max on server
	// side
	return (r->tr->end_time)
		   ? r->tr->end_time
		   : r->tr->start_time + g_config.transaction_max_ms;
}

/*
 * UDF memory tracker hook
 * Todo comments
 */
bool
as_udf__mem_op(mem_tracker *mt, uint32_t num_bytes, memtracker_op op)
{
	bool ret = true;
	if (!mt || !mt->udata) {
		return false;
	}
	uint64_t val = 0;

	udf_record *r = (udf_record *) mt->udata;
	if (r) return false;

	if (op == MEM_RESERVE) {
		val = cf_atomic_int_add(&g_config.udf_runtime_gmemory_used, num_bytes);
		if (val > g_config.udf_runtime_max_gmemory) {
			cf_atomic_int_sub(&g_config.udf_runtime_gmemory_used, num_bytes);
			ret = false;
			goto END;
		}

		val = cf_atomic_int_add(&r->udf_runtime_memory_used, num_bytes);
		if (val > g_config.udf_runtime_max_memory) {
			cf_atomic_int_sub(&r->udf_runtime_memory_used, num_bytes);
			cf_atomic_int_sub(&g_config.udf_runtime_gmemory_used, num_bytes);
			ret = false;
			goto END;
		}
	} else if (op == MEM_RELEASE) {
		cf_atomic_int_sub(&r->udf_runtime_memory_used, num_bytes);
	} else if (op == MEM_RESET) {
		cf_atomic_int_sub(&g_config.udf_runtime_gmemory_used,
			r->udf_runtime_memory_used);
		r->udf_runtime_memory_used = 0;
	} else {
		ret = false;
	}
END:
	return ret;
}

/*
 * Wrapper function over call to lua.  Setup arglist and memory tracker before
 * making the call.
 *
 * Returns: return from the UDF execution
 *
 * Parameter: call - udf call being executed
 * 			  rec  - as_rec on which UDF needs to be operated. The caller
 * 			         sets it up
 * 			  res  - Result to be populated by execution
 *
 */
int
udf_apply_record(udf_call * call, as_rec *rec, as_result *res)
{
	as_list         arglist;
	as_list_init(&arglist, call->arglist, &udf_arglist_hooks);

	cf_detail(AS_UDF, "Calling %s.%s()", call->filename, call->function);
	mem_tracker udf_mem_tracker = {
		.udata  = as_rec_source(rec),
		.cb     = as_udf__mem_op,
	};
	udf_memtracker_setup(&udf_mem_tracker);

	// Setup time tracker
	time_tracker udf_timer_tracker = {
		.udata     = as_rec_source(rec),
		.end_time  = as_udf__end_time
	};
	udf_timer_setup(&udf_timer_tracker);
	as_timer timer;
	as_timer_init(&timer, &udf_timer_tracker, &udf_timer_hooks);

	as_udf_context ctx = {
		.as         = &g_ldt_aerospike,
		.timer      = &timer,
		.memtracker = NULL
	};

	uint64_t now = cf_getms();
	int ret_value = as_module_apply_record(&mod_lua, &ctx,
			call->filename, call->function, rec, &arglist, res);
	cf_hist_track_insert_data_point(g_config.ut_hist, now);
	udf_memtracker_cleanup();
	udf_timer_cleanup();
	as_list_destroy(&arglist);

	return ret_value;
}

/*
 * Current send response call back for the UDF execution
 *
 * Side effect : Will clean up response udata and data in it.
 *               caller should not refer to it after this
 */
int udf_rw_sendresponse(as_transaction *tr, int retcode)
{
	cf_detail(AS_UDF, "Sending UDF Request response=%p retcode=%d", tr->udata.res_udata, retcode);
	udf_call      * call = ((udf_response_udata *)tr->udata.res_udata)->call;
	as_result     * res  = ((udf_response_udata *)tr->udata.res_udata)->res;
	tr->result_code      = retcode;
	call->transaction    = tr;
	send_result(res, call, NULL);
	as_result_destroy(res);
	udf_call_destroy(call);
	cf_free(call);
	cf_free(tr->udata.res_udata);
	return 0;
}

/*
 * Function to set up the response callback functions
 * See udf_rw_complete for the details of logic
 */
int udf_rw_addresponse(as_transaction *tr, void *udata)
{
	if (!tr) {
		cf_warning(AS_UDF, "Invalid Transaction");
		return -1;
	}
	tr->udata.res_cb    = udf_rw_sendresponse;
	tr->udata.res_udata = udata;
	return 0;
}

/*
 * Main workhorse function which is parallel to write_local called from
 * internal_rw_start. Does the following
 *
 * 1. Opens up the record if it exists
 * 2. Sets up UDF record
 * 3. Sets up encapsulating LDT record (Before execution we do not know if
 * 	  UDF is for record or LDT)
 * 4. Calls function to run UDF
 * 5. Call udf_rw_finish to wrap up execution
 * 6. Either sends response back to client or based on response
 *    setup response callback in transaction.
 *
 * Parameter:
 * 		call - UDF call to be executed
 * 		pickled_buf
 * 		pickled_sz
 * 		pickled_void_time (OUT) Filled up when request returns in case
 * 		                  there was a write. This is used for
 * 		                  replication
 * 		op   - (OUT) Returns op type of operation performed by UDF
 *
 * Returns: Always 0
 *
 * Side effect
 * 	pickled buf is populated user should free it up.
 *  Setups response callback should be called at the end of transaction.
 */
int
udf_rw_local(udf_call * call, write_request *wr, udf_optype *op)
{
	// Step 1: Setup UDF Record and LDT record
	as_transaction *tr = call->transaction;
	as_index_ref    r_ref;
	r_ref.skip_lock = false;

	as_storage_rd  rd;
	bzero(&rd, sizeof(as_storage_rd));

	udf_record urecord;
	udf_record_init(&urecord);

	ldt_record lrecord;
	ldt_record_init(&lrecord);

	urecord.tr                 = tr;
	urecord.r_ref              = &r_ref;
	urecord.rd                 = &rd;
	// TODO: replace bzero
	bzero(&urecord.updates, sizeof(udf_record_bin) * UDF_RECORD_BIN_ULIMIT);
	as_rec          urec;
	as_rec_init(&urec, &urecord, &udf_record_hooks);
	as_rec          lrec;
	as_rec_init(&lrec, &lrecord, &ldt_record_hooks);

	// Link lrecord and urecord
	lrecord.h_urec             = &urec;
	urecord.lrecord            = &lrecord;
	urecord.keyd               = tr->keyd;

	// Step 2: Setup Storage Record
	int rec_rv = as_record_get(tr->rsv.tree, &tr->keyd, &r_ref, tr->rsv.ns);
	if (!rec_rv) {
		urecord.flag   |= UDF_RECORD_FLAG_OPEN;
		urecord.flag   |= UDF_RECORD_FLAG_PREEXISTS;
		cf_detail(AS_UDF, "Open %p %x %"PRIx64"", &urecord, urecord.flag, *(uint64_t *)&tr->keyd);
		udf_storage_record_open(&urecord);
		// While opening parent record read the record from the disk. Property
		// map is created from LUA world. The version can only be get in case
		// the property map bin is there. If not there the record is normal
		// record
		int rv = as_ldt_parent_storage_get_version(&rd, &lrecord.version);
		cf_detail(AS_LDT, "LDT_VERSION Read Version From Storage %p:%ld rv=%d",
				  *(uint64_t *)&urecord.keyd, lrecord.version, rv);
	} else {
		urecord.flag   &= ~(UDF_RECORD_FLAG_OPEN
							| UDF_RECORD_FLAG_STORAGE_OPEN
							| UDF_RECORD_FLAG_PREEXISTS);
	}

	// entry point for all SMD-UDF's(LDT) calls, not called for other UDF's.
	cf_detail(AS_UDF, "START working with LDT Record %p %p %p %p %d", &urecord,
			urecord.tr, urecord.r_ref, urecord.rd,
			(urecord.flag & UDF_RECORD_FLAG_STORAGE_OPEN));

	// Step 3: Run UDF
	as_result       *res = as_result_new();
	int ret_value        = udf_apply_record(call, &lrec, res);
	as_namespace *  ns   = tr->rsv.ns;
	// Capture the success of the Lua call to use below
	bool success = res->is_success;

	if (ret_value == 0) {

		udf_rw_finish(&lrecord, wr, op);
		if (UDF_OP_IS_READ(*op)) {
			send_result(res, call, NULL);
			as_result_destroy(res);
		} else {
			udf_response_udata *udata = cf_malloc(sizeof(udf_response_udata));
			udata->call               =  call;
			udata->res                =  res;
			cf_detail(AS_UDF, "Setting UDF Request Response data=%p with udf op %d", udata, *op);
			udf_rw_addresponse(tr, udata);
		}

		// TODO this is not the right place for counter, put it at proper place
		if(tr->udata.req_udata) {
			cf_atomic_int_add(&((tscan_job*)(tr->udata.req_udata))->n_obj_udf_updated, (*op == UDF_OPTYPE_WRITE));
		}

	} else {
		udf_record_close(&urecord, false);
		char *rs = as_module_err_string(ret_value);
		call->transaction->result_code = AS_PROTO_RESULT_FAIL_UDF_EXECUTION;
		send_response(call, "FAILURE", 7, AS_PARTICLE_TYPE_STRING, rs, strlen(rs));
		cf_free(rs);
		as_result_destroy(res);
	}

	udf_rw_update_stats(ns, *op, ret_value, success);

	// free everything we created - the rec destroy with ldt_record hooks
	// destroys the ldt components and the attached "base_rec"
	as_rec_destroy(&lrec);

	return 0;
}

/*
 * Function called to determine if needs to call udf_rw_complete.
 * See the definition below for detail
 *
 * NB: Callers needs to hold necessary protection
 */
bool
udf_rw_needcomplete_wr( write_request *wr )
{
	if (!wr) {
		cf_warning(AS_UDF, "Invalid write request");
		return false;
	}
	ureq_data *ureq = (ureq_data *)&wr->udata;
	if (ureq->req_cb && ureq->req_udata) {
		return true;
	}
	if (ureq->res_cb && ureq->res_udata) {
		return true;
	}
	return false;
}

/*
 * Function called to determine if needs to call udf_rw_complete.
 * See the definition below for detail
 *
 * NB: Callers needs to hold necessary protection
 */
bool
udf_rw_needcomplete( as_transaction *tr )
{
	if (!tr) {
		cf_warning(AS_UDF, "Invalid write request");
		return false;
	}
	ureq_data *ureq = (ureq_data *)&tr->udata;
	if (ureq->req_cb && ureq->req_udata) {
		return true;
	}
	if (ureq->res_cb && ureq->res_udata) {
		return true;
	}
	return false;
}

/*
 * Function called when the transaction performing UDF finishes.
 * It looks at the request and response callback set with the request
 * and response udata.
 *
 * Request Callback:  It is set by special request to be run at the end
 * 					  of a request. Current user is UDF scan which sets
 * 					  it up in the internal transaction to perform run
 * 					  UDF of scanned data.
 * Response Callback: It is set for sending response to client at the
 * 					  end of the transaction. Currently used by UDF
 * 					  to send back special results after the replication
 * 					  has finished.
 *
 * NB: Callback function is response to make sure the data is intact
 *     and properly handled. And its synchronization if required.
 *
 * Returns : Nothing
 *
 * Caller:
 * 		proxy_msg_fn
 * 		proxy_retransmit_reduce_fn
 * 		write_request_destructor
 * 		internal_rw_start
 * 		as_rw_start
 * 		rw_retransmit_reduce_fn
 * 		thr_tsvc_read
 * 		udf_rw_complete
 */
void
udf_rw_complete(as_transaction *tr, int retcode, char *filename, int lineno )
{
	cf_debug(AS_UDF, "[ENTER] file(%s) line(%d)", filename, lineno );

	if ( !tr ) {
		cf_warning(AS_UDF, "Invalid internal request");
	}
	ureq_data *ureq = &tr->udata;
	if (ureq->req_cb && ureq->req_udata) {
		ureq->req_cb( tr, retcode );
	}

	if (ureq->res_cb && ureq->res_udata) {
		ureq->res_cb( tr, retcode);
	}
	cf_detail(AS_UDF, "UDF_COMPLETED:[%s:%d] %p %p %p %p %p",
			filename, lineno, tr , ureq->req_cb, ureq->req_udata, ureq->res_cb, ureq->res_udata);
	UREQ_DATA_RESET(&tr->udata);
}

/*
 * Internal Function: Serialize passed in as_val into passed in buffer.
 * 					  msgpack is used for packing
 *
 * 					  Note that passed in buffer should have enough space
 * 					  .. caller is responsible for figuring out space required
 * 					  and allocate it.
 *
 * 					  If passed in buffer is NULL only the required size is
 * 					  calculated.
 *
 * Parameters:
 * 		val     : as_val which needs to be laid out on buffer
 * 		buf     : buffer to lay serialize value on.
 * 		size    : size of the laid out data
 *
 * Return value : nothing. If all goes good the buffer is properly
 * 				  filled up and size value is set.
 *
 * Callers:
 * 		as_msg_make_val_response_bufbuilder
 * 		as_query__add_val_response
 */
void
as_val_tobuf(const as_val *v, uint8_t *buf, uint32_t *size)
{
	if ( v != NULL ) {
		switch( as_val_type(v) ) {
			case AS_NIL:
			{
				*size = 0;
				break;
			}
			case AS_INTEGER:
			{
				*size = 8;
				if (buf) {
					as_integer * i = as_integer_fromval(v);
					int64_t ri = __cpu_to_be64(as_integer_toint(i));
					memcpy(buf, &ri, *size);
				}
				break;
			}
			case AS_STRING:
			{
				as_string * s = as_string_fromval(v);
				*size = as_string_len(s);
				if (buf) {
					char * rs = (char *) as_string_tostring(s);
					memcpy(buf, rs, *size);
				}
				break;
			}
			case AS_MAP:
			case AS_LIST:
			{
				as_buffer asbuf;
				as_buffer_init(&asbuf);

				as_serializer s;
				as_msgpack_init(&s);

				as_serializer_serialize(&s, (as_val*)v, &asbuf);

				*size = asbuf.size;
				if (buf) {
					memcpy(buf, asbuf.data, asbuf.size);
				}
				// not needed as it is stack allocated
				// as_serializer_destroy(&s);
				as_buffer_destroy(&asbuf);
				break;
			}
			// TODO: Resolve. Can we actually MAKE a value (the bin name) for
			// an LDT value?  Users should never see a real LDT value.
			case AS_LDT:
			{
				as_buffer asbuf;
				as_buffer_init(&asbuf);

				as_serializer s;
				as_msgpack_init(&s);

				as_string as_str;
				as_string_init( &as_str, "INT LDT BIN NAME", false );

				as_serializer_serialize(&s, (as_val*) &as_str, &asbuf);

				*size = asbuf.size;
				if (buf) {
					memcpy(buf, asbuf.data, asbuf.size);
				}
				// not needed as it is stack allocated
				// as_serializer_destroy(&s);
				as_buffer_destroy(&asbuf);
				break;

			}
			default:
			{
				cf_debug(AS_UDF, "SUCCESS: VAL TYPE UNDEFINED %d\n",
						 as_val_type(v));
				*size = 0;
			}
		}
	}
	else {
		*size = 0;
	}
}

/*
 * Internal Function: Convert value in passed in as_bin into as_val.
 *                    This function allocates memory. Caller needs
 *                    to free it.
 *
 * Parameters:
 * 		bin    : bin for which as_val needs to be created
 *
 * Return value :
 * 		value  (as_val*) in case of success
 * 		NULL  in case of failure
 *
 * Description:
 * 		Based on the type of data as_val is allocated and data
 * 		and filled into it before returning. For the collections
 * 		map/list data is de-serialized before putting it into as_val.
 *
 * Callers:
 * 		udf_record_storage_get
 */
as_val *
as_val_frombin(as_bin *bb)
{
	as_val *value = NULL;
	uint8_t type = as_particle_type_convert(as_bin_get_particle_type(bb));

	switch ( type ) {
		case AS_PARTICLE_TYPE_INTEGER:
		{
			int64_t     i = 0;
			uint32_t    sz = 8;
			as_particle_tobuf(bb, (uint8_t *) &i, &sz);
			i = __cpu_to_be64(i);
			value = (as_val *) as_integer_new(i);
			break;
		}
		case AS_PARTICLE_TYPE_STRING:
		{
			uint32_t psz = 32;
			as_particle_tobuf(bb, NULL, &psz);

			char * buf = cf_malloc(psz + 1);
			if (!buf) {
				return value;
			}

			as_particle_tobuf(bb, (uint8_t *) buf, &psz);

			buf[psz] = '\0';

			value = (as_val *) as_string_new(buf, true /*ismalloc*/);
			break;
		}
		case AS_PARTICLE_TYPE_BLOB:
		{

			uint8_t *pbuf;
			uint32_t psz;

			as_particle_p_get(bb, &pbuf, &psz);

			uint8_t *buf = cf_malloc(psz);
			if (!buf) {
				return value;
			}
			memcpy(buf, pbuf, psz);

			value = (as_val *) as_bytes_new_wrap(buf, psz, true);
			break;
		}
		case AS_PARTICLE_TYPE_MAP:
		case AS_PARTICLE_TYPE_LIST:
		{

			as_buffer     buf;
			as_buffer_init(&buf);

			as_serializer s;
			as_msgpack_init(&s);

			uint32_t      sz = 0;

			as_particle_p_get(bb, (uint8_t **) &buf.data, &sz);
			buf.capacity = sz;
			buf.size = sz;

			as_serializer_deserialize(&s, &buf, &value);
			as_serializer_destroy(&s);
			break;
		}

		default:
		{
			value = NULL;
			break;
		}
	}
	return value;
}

int
to_particle_type(int from_as_type)
{
	switch (from_as_type) {
		case AS_NIL:
			return AS_PARTICLE_TYPE_NULL;
			break;
		case AS_BOOLEAN:
		case AS_INTEGER:
			return AS_PARTICLE_TYPE_INTEGER;
			break;
		case AS_STRING:
			return AS_PARTICLE_TYPE_STRING;
		case AS_BYTES:
			return AS_PARTICLE_TYPE_BLOB;
		case AS_LIST:
			return AS_PARTICLE_TYPE_LIST;
		case AS_MAP:
			return AS_PARTICLE_TYPE_MAP;
		case AS_UNKNOWN:
		case AS_REC:
		case AS_PAIR:
		default:
			cf_warning(AS_UDF, "unmappable type %d", from_as_type);
			break;
	}
	return AS_PARTICLE_TYPE_NULL;
}

void
as_udf_init(void)
{
	// Configure mod_lua.
	as_module_configure(&mod_lua, &g_config.mod_lua);

	// Setup logger for mod_lua.
	if (! mod_lua.logger) {
		mod_lua.logger = udf_logger_new(AS_UDF);
	}

	if (0 > udf_cask_init()) {
		cf_crash(AS_UDF, "failed to initialize UDF cask");
	}

	as_aerospike_init(&g_as_aerospike, NULL, &udf_aerospike_hooks);
	// Assuming LDT UDF also comes in from udf_rw.
	ldt_init();
}
