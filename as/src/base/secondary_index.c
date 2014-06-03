/*
 * secondary_index.c
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
 * SYNOPSIS
 * Abstraction to support secondary indexes with multiple implementations.
 * Currently there are two variants of secondary indexes supported.
 *
 * -  Aerospike Index B-tree, this is full fledged index implementation and
 *    maintains its own metadata and data structure for list of those indexes.
 *
 * -  Citrusleaf foundation indexes which are bare bone tree implementation
 *    with ability to insert delete update indexes. For these the current code
 *    manage all the data structure to manage different trees. [Will be
 *    implemented when required]
 *
 * This file implements all the translation function which can be called from
 * citrusleaf to prepare to do the operations on secondary index. Also
 * implements locking to make Aerospike Index (single threaded) code multi threaded.
 *
 */

#include <errno.h>
#include <limits.h>
#include <string.h>

#include "ai_globals.h"
#include "bt_iterator.h"

#include "base/thr_scan.h"
#include "base/secondary_index.h"
#include "base/thr_sindex.h"
#include "base/system_metadata.h"

#include "ai_btree.h"

#define SINDEX_CRASH(str, ...) \
	cf_crash(AS_SINDEX, "SINDEX_ASSERT: "str, ##__VA_ARGS__);

// Internal Functions
bool as_sindex__setname_match(as_sindex_metadata *imd, const char *setname);
int  as_sindex__pre_op_assert(as_sindex *si, int op);
int  as_sindex__post_op_assert(as_sindex *si, int op);
void as_sindex__process_ret(as_sindex *si, int ret, as_sindex_op op, uint64_t starttime, int pos);
void as_sindex__dup_meta(as_sindex_metadata *imd, as_sindex_metadata **qimd, bool refcounted);
// Methods for creating secondary index key
int  as_sindex__skey_from_rd(as_sindex_metadata *imd, as_sindex_key *skey, as_storage_rd *rd);
int  as_sindex__skey_release(as_sindex_key *skey);

// Translation from sindex error code to string
const char *as_sindex_err_str(int op_code) {
	switch(op_code) {
		case AS_SINDEX_ERR_FOUND:               return "INDEX FOUND";          break;
		case AS_SINDEX_ERR_NO_MEMORY:           return "NO MEMORY";             break;
		case AS_SINDEX_ERR_UNKNOWN_KEYTYPE:     return "UNKNOWN KEYTYPE";       break;
		case AS_SINDEX_ERR_BIN_NOTFOUND:        return "BIN NOT FOUND";         break;
		case AS_SINDEX_ERR_NOTFOUND:            return "NO INDEX";              break;
		case AS_SINDEX_ERR_PARAM:               return "ERR PARAM";             break;
		case AS_SINDEX_ERR_TYPE_MISMATCH:       return "KEY TYPE MISMATCH";     break;
		case AS_SINDEX_ERR:                     return "ERR GENERIC";           break;
		case AS_SINDEX_OK:                      return "OK";                    break;
		default:                                return "Unknown Code";          break;
	}
}

/*
 * Notes-
 * 		Translation from sindex internal error code to generic client visible
 * 		Aerospike error code
 */
int as_sindex_err_to_clienterr(int err, char *fname, int lineno) {
	switch(err) {
		case AS_SINDEX_ERR_FOUND:        return AS_PROTO_RESULT_FAIL_INDEX_FOUND;
		case AS_SINDEX_ERR_NOTFOUND:     return AS_PROTO_RESULT_FAIL_INDEX_NOTFOUND;
		case AS_SINDEX_ERR_NO_MEMORY:    return AS_PROTO_RESULT_FAIL_INDEX_OOM;
		case AS_SINDEX_ERR_PARAM:        return AS_PROTO_RESULT_FAIL_PARAMETER;
		case AS_SINDEX_ERR_NOT_READABLE: return AS_PROTO_RESULT_FAIL_INDEX_NOTREADABLE;
		case AS_SINDEX_OK:               return AS_PROTO_RESULT_OK;
		
		// Defensive internal error
		case AS_SINDEX_ERR_SET_MISMATCH:
		case AS_SINDEX_ERR_UNKNOWN_KEYTYPE:
		case AS_SINDEX_ERR_BIN_NOTFOUND:
		case AS_SINDEX_ERR_TYPE_MISMATCH:
		case AS_SINDEX_ERR:
		default: cf_warning(AS_SINDEX, "%s Error at %s,%d",
							 as_sindex_err_str(err), fname, lineno);
											return AS_PROTO_RESULT_FAIL_INDEX_GENERIC;
	}
}

/// LOOKUP
int
as_sindex__simatch_by_set_binid(as_namespace *ns, char * set, int binid)
{
	int simatch = -1;
	// add 14 for number of characters in any uint32
	char set_binid[AS_SET_NAME_MAX_SIZE + 14];
	memset(set_binid, 0, AS_SET_NAME_MAX_SIZE + 14);
	if (set == NULL) {
		sprintf(set_binid, "_%d", binid);
	}
	else {
		sprintf(set_binid, "%s_%d", set, binid);
	}
	int rv      = shash_get(ns->sindex_set_binid_hash, (void *)set_binid, (void *)&simatch);
	cf_detail(AS_SINDEX, "Found binid simatch %d->%d rv=%d", binid, simatch, rv);
	if (rv) {
		return -1;
	}
	
	return simatch;
}
int
as_sindex__simatch_by_iname(as_namespace *ns, char *idx_name)
{
	int simatch = -1;
	char iname[AS_ID_INAME_SZ]; memset(iname, 0, AS_ID_INAME_SZ);
	snprintf(iname, strlen(idx_name) + 1, "%s", idx_name);
	int rv      = shash_get(ns->sindex_iname_hash, (void *)iname, (void *)&simatch);
	cf_detail(AS_SINDEX, "Found iname simatch %s->%d rv=%d", iname, simatch, rv);
		
	if (rv) {
		return -1;
	}
	return simatch;
}

/*
 * Single cluttered interface for lookup. iname precedes binid
 * i.e if both are specified search is done with iname
 */
#define AS_SINDEX_LOOKUP_FLAG_SETCHECK     0x01
#define AS_SINDEX_LOOKUP_FLAG_ISACTIVE     0x02
#define AS_SINDEX_LOOKUP_FLAG_NORESERVE    0x04
as_sindex *
as_sindex__lookup_lockfree(as_namespace *ns, char *iname, int binid, char *set, char flag)
{
	int simatch   = -1;
	as_sindex *si = NULL;

	if (iname) {
		simatch   = as_sindex__simatch_by_iname(ns, iname);
	} else {
		simatch   = as_sindex__simatch_by_set_binid(ns, set, binid);
	}

	if (simatch != -1) {
		si      = &ns->sindex[simatch];
	
		if ((flag & AS_SINDEX_LOOKUP_FLAG_ISACTIVE)
			&& !as_sindex_isactive(si)) {
			si = NULL;
			goto END;
		}

		if ((flag & AS_SINDEX_LOOKUP_FLAG_SETCHECK)
			&& !as_sindex__setname_match(si->imd, set)) {
			si = NULL;
			goto END;
		}

		if (simatch != si->simatch) {
			cf_warning(AS_SINDEX, "Inconsistent internal reference ... ignoring..");
		}
		if (!(flag & AS_SINDEX_LOOKUP_FLAG_NORESERVE))
			AS_SINDEX_RESERVE(si);
	}
END:
	return si;
}

as_sindex *
as_sindex__lookup(as_namespace *ns, char *iname, int binid, char *set, char flag)
{
	SINDEX_GRLOCK();
	as_sindex *si = as_sindex__lookup_lockfree(ns, iname, binid, set, flag);
	SINDEX_GUNLOCK();
	return si;
}

/*
 *	Arguments
 *		imd     - To match the setname of sindex metadata.
 *		setname - set name to be matched
 *
 *	Returns
 * 		TRUE    - If setname given matches the one in imd
 * 		FALSE   - Otherwise
 */
bool
as_sindex__setname_match(as_sindex_metadata *imd, const char *setname)
{
	// If passed in setname does not match the one on imd
	if (setname && ((!imd->set) || strcmp(imd->set, setname))) goto Fail;
	if (!setname && imd->set)                                  goto Fail;
	return true;
Fail:
	SITRACE(imd->si, META, debug, "Index Mismatch %s %s", imd->set, setname);
	return false;
}

/*
 * Returns -
 * 		AS_SINDEX_OK  - On success.
 *		Else on failure one of these -
 * 			AS_SINDEX_ERR
 * 			AS_SINDEX_ERR_OTHER
 * 			AS_SINDEX_ERR_NOT_READABLE
 * Notes -
 * 		Assert anything which is inconsistent with the way DML/DML/DDL/defrag_th/destroy_th
 * 		running in multi-threaded environment.
 * 		This is called before acquiring secondary index lock.
 *
 * Synchronization -
 * 		reserves the imd.
 * 		Caller of DML should always reserves si.
 */
int
as_sindex__pre_op_assert(as_sindex *si, int op)
{
	int ret = AS_SINDEX_ERR;
	if (!si) {
		SINDEX_CRASH("DML with NULL si"); return ret;
	}
	if (!si->imd) {
		SINDEX_CRASH("DML with NULL imd"); return ret;
	}

	// Caller of DML should always reserves si, If the state of si is not DESTROY and
	// if the count is 1 then caller did not reserve fail the assertion
	int count = cf_rc_count(si->imd);
	SITRACE(si, RESERVE, debug, "DML on index %s in %d state with reference count %d < 2", si->imd->iname, si->state, count);
	if ((count < 2) && (si->state != AS_SINDEX_DESTROY)) {
		cf_warning(AS_SINDEX, "Secondary index is improperly ref counted ... cannot be used");
		return ret;
	}
	ret = AS_SINDEX_OK;
	
	switch (op)
	{
		case AS_SINDEX_OP_READ:
			// First one signifies that index is still getting built
			// Second on signifies because of some failure secondary
			// is not in sync with primary
			if (!(si->flag & AS_SINDEX_FLAG_RACTIVE)
				|| ( (si->desync_cnt > 0)
					&& !(si->config.flag & AS_SINDEX_CONFIG_IGNORE_ON_DESYNC))) {
				ret = AS_SINDEX_ERR_NOT_READABLE;
			}
			break;
		case AS_SINDEX_OP_INSERT:
		case AS_SINDEX_OP_DELETE:
			break;
		default:
			cf_warning(AS_SINDEX, "Unidentified Secondary Index Op .. Ignoring!!");
	}
	return ret;
}

/*
 * Assert anything which is inconsistent with the way DML/DML/DDL/
 * defrag_th/destroy_th running in multi-threaded environment. This is called
 * after releasing secondary index lock
 */
int
as_sindex__post_op_assert(as_sindex *si, int op)
{
	int ret = -1;
	if (!si) {
		SINDEX_CRASH("DML with NULL si"); return ret;
	}
	if (!si->imd) {
		SINDEX_CRASH("DML with NULL imd"); return ret;
	}

	// Caller of DML should always reserves si, If the state of si is not DESTROY and
	// if the count is 1 then caller did not reserve fail the assertion
	if ((cf_rc_count(si->imd) < 2) && (si->state != AS_SINDEX_DESTROY)) {
		SINDEX_CRASH("DML on imd in %d state with < 2 reference count", si->state);
		return ret;
	}
	return AS_SINDEX_OK;
}

/*
 * Function as_sindex_pktype_from_sktype
 * 		Translation function from KTYPE to PARTICLE Type
 *
 * 	Returns -
 * 		On failure - AS_SINDEX_ERR_UNKNOWN_KEYTYPE
 */
as_particle_type
as_sindex_pktype_from_sktype(as_sindex_ktype t)
{
	switch(t) {
		case AS_SINDEX_KTYPE_LONG:    return AS_PARTICLE_TYPE_INTEGER;
		case AS_SINDEX_KTYPE_FLOAT:   return AS_PARTICLE_TYPE_FLOAT;
		case AS_SINDEX_KTYPE_DIGEST:  return AS_PARTICLE_TYPE_STRING;
		default: cf_crash(AS_SINDEX, "Key type not known");
	}
	return AS_SINDEX_ERR_UNKNOWN_KEYTYPE;
}

/*
 * Create duplicate copy of sindex metadata. New lock is created
 * used by index create by user at runtime or index creation at the boot time
 */
void
as_sindex__dup_meta(as_sindex_metadata *imd, as_sindex_metadata **qimd,
		bool refcounted)
{
	if (!imd) return;
	as_sindex_metadata *qimdp;

	if (refcounted) qimdp = cf_rc_alloc(sizeof(as_sindex_metadata));
	else            qimdp = cf_malloc(  sizeof(as_sindex_metadata));
	memset(qimdp, 0, sizeof(as_sindex_metadata));

	qimdp->ns_name = cf_strdup(imd->ns_name);

	// Set name is optional for create
	if (imd->set) {
		qimdp->set = cf_strdup(imd->set);
	} else {
		qimdp->set = NULL;
	}
	
	qimdp->iname    = cf_strdup(imd->iname);
	qimdp->itype    = imd->itype;
	qimdp->nprts    = imd->nprts;
	qimdp->mfd_slot = imd->mfd_slot;

	qimdp->num_bins = imd->num_bins;
	for (int i = 0; i < imd->num_bins; i++) {
		qimdp->bnames[i] = cf_strdup(imd->bnames[i]);
		qimdp->btype[i]  = imd->btype[i];
		qimdp->binid[i]  = imd->binid[i];
	}

    qimdp->oindx = imd->oindx;

	pthread_rwlockattr_t rwattr;
	if (pthread_rwlockattr_init(&rwattr))
		cf_crash(AS_AS,  "pthread_rwlockattr_init: %s",
					cf_strerror(errno));
	if (pthread_rwlockattr_setkind_np(&rwattr,
				PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)) {
		cf_crash( AS_TSVC, "pthread_rwlockattr_setkind_np: %s",
				cf_strerror(errno));
	}
	if (pthread_rwlock_init(&qimdp->slock, NULL)) {
		cf_crash(AS_SINDEX,
				"Could not create secondary index dml mutex ");
	}
	qimdp->flag |= IMD_FLAG_LOCKSET;

	*qimd = qimdp;
}

/*
 * Find matching index based on passed in binid's, Reserve those indexes and
 * return the imatch array to the client
 * TODO: Make this and other similar ones return return imatch bitmap in
 * one go doing it one by one is not good enough
 */
int
as_sindex__get_simatches_by_sbin(as_namespace *ns, const char *set,
								 as_sindex_bin *sbin, int num_bins,
								 int8_t *simatches, bool isw, int *matches)
{
	if (ns->single_bin) {
		GTRACE(META, debug, "No Index On Namespace with Single Bin");
		return AS_SINDEX_ERR_NOTFOUND; // No secondary index for single bin
	}
	if (!sbin) {
		GTRACE(META, debug, "Null Sbin, No Index Matches");
		return AS_SINDEX_ERR_PARAM;
	}
	if (!num_bins) {
		GTRACE(META, debug, "Zero Bins In Insert Request, No Index Matches");
		return AS_SINDEX_ERR_PARAM;
	}
	GTRACE(META, debug, "Searching for matching index for sbin");

	SINDEX_GRLOCK();
	int nmatch = 0;
	for (int k = 0; k < num_bins; k++) {
		as_sindex  *si = as_sindex__lookup_lockfree(ns, NULL, sbin[k].id,
							(char *)set,
							AS_SINDEX_LOOKUP_FLAG_ISACTIVE
							| AS_SINDEX_LOOKUP_FLAG_SETCHECK);

		if (si) {
			cf_detail(AS_SINDEX, "Index: %s has matching index " \
					"[bimatch: %d,simatch: %d] %d @ %d\n",
					si->imd->iname, si->imd->bimatch, si->simatch, si->state, nmatch);
			simatches[nmatch]  = si->simatch;
			nmatch++;
		}
	
	}
	SINDEX_GUNLOCK();

	if (nmatch == 0) {
		GTRACE(META, debug, "No Matching Index Found");
		return AS_SINDEX_ERR_NOTFOUND;
	} else {
		*matches = nmatch;
		cf_detail(AS_SINDEX, "Num Matches %d", nmatch);
		return AS_SINDEX_OK;
	}
}


/*
 * Function to perform validation check on the return type and increment
 * decrement all the statistics.
 */
void
as_sindex__process_ret(as_sindex *si, int ret, as_sindex_op op,
		uint64_t starttime, int pos)
{
	switch(op) {
		case AS_SINDEX_OP_INSERT:
			if (AS_SINDEX_ERR_NO_MEMORY == ret) {
				cf_atomic_int_incr(&si->desync_cnt);
			}
			if (ret && ret != AS_SINDEX_KEY_FOUND) {
				cf_debug(AS_SINDEX,
						"SINDEX_FAIL: Insert into %s failed at %d with %d",
						si->imd->iname, pos, ret);
				cf_atomic64_incr(&si->stats.write_errs);
			} else if (!ret) {
				cf_atomic64_incr(&si->stats.n_objects);
			}
			cf_atomic64_incr(&si->stats.n_writes);
			SINDEX_HIST_INSERT_DATA_POINT(si, write_hist, starttime);
			break;
		case AS_SINDEX_OP_DELETE:
			if (ret && ret != AS_SINDEX_KEY_NOTFOUND) {
				cf_debug(AS_SINDEX,
						"SINDEX_FAIL: Delete from %s failed at %d with %d",
	                    si->imd->iname, pos, ret);
				cf_atomic64_incr(&si->stats.delete_errs);
			} else if (!ret) {
				cf_atomic64_decr(&si->stats.n_objects);
			}
			cf_atomic64_incr(&si->stats.n_deletes);
			SINDEX_HIST_INSERT_DATA_POINT(si, delete_hist, starttime);
			break;
		case AS_SINDEX_OP_READ:
			if (ret < 0) { // AS_SINDEX_CONTINUE(1) also OK
				cf_debug(AS_SINDEX,
						"SINDEX_FAIL: Read from %s failed at %d with %d",
						si->imd->iname, pos, ret);
				cf_atomic64_incr(&si->stats.read_errs);
			}
			cf_atomic64_incr(&si->stats.n_reads);
			break;
		default:
			cf_crash(AS_SINDEX, "Invalid op");
	}
}

/*
 * Info received from bin should be actually cleaned up using free later
 */
int
as_sindex__skey_from_rd(as_sindex_metadata *imd, as_sindex_key *skey,
		as_storage_rd *rd)
{
	int ret = AS_SINDEX_OK;
	skey->num_binval = imd->num_bins;
	SINDEX_GRLOCK();
	for (int i = 0; i < imd->num_bins; i++) {
		as_bin *b = as_bin_get(rd, (uint8_t *)imd->bnames[i],
										strlen(imd->bnames[i]));
		if (b && (as_bin_get_particle_type(b) ==
					as_sindex_pktype_from_sktype(imd->btype[i]))) {
			// optimize do not copy
			ret = as_sindex_sbin_from_bin(imd->si->ns, imd->set,
											b, &skey->b[i]);
			if (ret != AS_SINDEX_OK) {
				SITRACE(imd->si, DML, debug, "Warning: Did not find matching index for %s", imd->bnames[i]);
				cf_warning(AS_SINDEX, "Warning: Did not find matching index for %s", imd->bnames[i]);
				ret = AS_SINDEX_ERR_NOTFOUND; goto Cleanup;
			}
			// Populate digest value in skey so Aerospike Index B-tree hashing for
			// string works
			if (skey->b[i].type == AS_PARTICLE_TYPE_STRING) {
				cf_digest_compute(skey->b[i].u.str, skey->b[i].valsz, &skey->b[i].digest);
			}
		} else if (!b) {
			SITRACE(imd->si, DML, debug, "No bin for %s", imd->bnames[i]);
			ret = AS_SINDEX_ERR_BIN_NOTFOUND;
			goto Cleanup;
		} else {
			SITRACE(imd->si, DML, debug, "%d != %d",
							as_bin_get_particle_type(b),
							as_sindex_pktype_from_sktype(imd->btype[i]));
			ret = AS_SINDEX_ERR_TYPE_MISMATCH;
			goto Cleanup;
		}
	}
	SINDEX_GUNLOCK();
	return ret;

Cleanup:
	SINDEX_GUNLOCK();
	as_sindex__skey_release(skey);
	return ret;
}

int
as_sindex__skey_from_sbin(as_sindex_key *skey, int idx, as_sindex_bin *sbin)
{
	GTRACE(CALLSTACK, debug, "as_sindex__skey_from_sbin");
	skey->b[idx].id     = sbin->id;
	skey->b[idx].type   = sbin->type;
	skey->b[idx].valsz  = sbin->valsz;
	skey->b[idx].u.i64  = sbin->u.i64;
	skey->b[idx].u.str  = sbin->u.str;
	if (sbin->type == AS_PARTICLE_TYPE_STRING) {
		cf_digest_compute(sbin->u.str, sbin->valsz, &skey->b[idx].digest);
	}
	skey->b[idx].flag   = 0;
	return AS_SINDEX_OK;
}

/*
 * Info received from bin should be actually cleaned up using free later
 */
int
as_sindex__skey_from_sbin_rd(as_sindex_metadata *imd, int numbins,
		as_sindex_bin *sbin, as_storage_rd *rd,
		as_sindex_key *skey)
{
	int ret = AS_SINDEX_OK;
	if (imd->num_bins == 1) { // if not compound bins will have bin value
		int i;
		// loop is O(N). hash-table is O(1) but probably overkill?
		for (i = 0; i < numbins; i++) {
			if ((sbin[i].id == imd->binid[0]) &&
					(sbin[i].type ==
					 as_sindex_pktype_from_sktype(imd->btype[0]))) break;
		}
		if (i < numbins) {
			as_sindex__skey_from_sbin(skey, 0, &sbin[i]);
		} else {
			ret = AS_SINDEX_ERR_NOTFOUND;
			goto Cleanup;
		}
	} else {
		ret = AS_SINDEX_ERR_NOTFOUND;
	}
	// Set it in end .. when everything is set
	skey->num_binval = imd->num_bins;
	return ret;

Cleanup:
	as_sindex__skey_release(skey);
	return ret;
}

/*
 * assumes bin names are set
 */
int
as_sindex__populate_binid(as_namespace *ns, as_sindex_metadata *imd)
{
	int i = 0;
	for (i = 0; i < imd->num_bins; i++) {
		// Bin id should be around if not create it
		char bname[AS_ID_BIN_SZ];
		memset(bname, 0, AS_ID_BIN_SZ);
		int bname_len = strlen(imd->bnames[i]);
		if ( (bname_len > (AS_ID_BIN_SZ - 1 ))
				|| !as_bin_name_within_quota(ns, (byte *)imd->bnames[i], bname_len)) {
			cf_warning(AS_SINDEX, "bin name %s too big. Bin not added", bname);
			return -1;
		}
		strncpy(bname, imd->bnames[i], bname_len);
		imd->binid[i] = as_bin_get_or_assign_id(ns, bname);
		SITRACE(imd->si, META, debug, " Assigned %d for %s %s", imd->binid[i], imd->bnames[i], bname);
	}
	for (i = imd->num_bins; i < AS_SINDEX_BINMAX; i++) {
		imd->binid[i] = -1;
	}
	return AS_SINDEX_OK;
}

int
as_sindex__skey_release(as_sindex_key *skey)
{
	if (!skey) return AS_SINDEX_ERR_PARAM;
	else {
		for (int i = 0; i < skey->num_binval; i++) {
			if (skey->b[i].flag & SINDEX_FLAG_BIN_DOFREE) {
				cf_free(skey->b[i].u.str);
			}
		}
	}
	return AS_SINDEX_OK;
}

int
as_sindex__op_by_skey(as_sindex   *si, as_sindex_key *skey,
					  as_storage_rd *rd, as_sindex_op op)
{
	as_sindex_metadata *imd = si->imd;
	SINDEX_RLOCK(&imd->slock);
	int ret = as_sindex__pre_op_assert(si, op);
	if (AS_SINDEX_OK != ret) {
		SINDEX_UNLOCK(&imd->slock);
		as_sindex__skey_release(skey);
		return AS_SINDEX_CONTINUE;
	}
	as_sindex_pmetadata *pimd = &imd->pimd[ai_btree_key_hash(imd, &skey->b[0])];
	uint64_t starttime = 0;
	if (op == AS_SINDEX_OP_DELETE) {
		starttime = cf_getus();
		SINDEX_WLOCK(&pimd->slock);
		ret       = ai_btree_delete(imd, pimd, skey,(void *)&rd->keyd);
		SINDEX_UNLOCK(&pimd->slock);
		if (ret != AS_SINDEX_OK) {
			SITRACE(si, DML, debug, "AS_SINDEX_OP_DELETE: Fail %d", ret);
		}
	} else if (op == AS_SINDEX_OP_INSERT) {
		starttime = cf_getus();
		SINDEX_WLOCK(&pimd->slock);
		ret       = ai_btree_put(imd, pimd, skey, (void *)&rd->keyd);
		SINDEX_UNLOCK(&pimd->slock);
		if (ret != AS_SINDEX_OK) {
			SITRACE(si, DML, debug, "AS_SINDEX_OP_INSERT: Fail %d", ret);
		}
	} else {
		cf_warning(AS_SINDEX, "Unimplemented op %d on the index %s, skipped",
				   op, imd->iname);
	}
	as_sindex__process_ret(si, ret, op, starttime, __LINE__);
	SITRACE(si, DML, debug, " Secondary Index Op Finish------------- ");
	SINDEX_UNLOCK(&imd->slock);
	as_sindex__skey_release(skey);
	return AS_SINDEX_OK;
}

/*
 * Based on skey_bins matching secondary indexes are picked [Meta data lock is
 * acquired while finding match]. Based on skey_bins and rd & matching index
 * secondary index key is created and perform op on secondary index. [sindex
 * lock is acquired while deleting]
 */
int
as_sindex__op_by_sbin(as_namespace *ns, const char *set,
					  int numbins, as_sindex_bin *sbin, as_storage_rd *rd,
					  as_sindex_op op)
{
	cf_detail(AS_SINDEX, "as_sindex__op_by_sbin\n");
	if (!rd) {
		cf_warning(AS_SINDEX, "Passing Null Storage record... aborting %s",
				(op == AS_SINDEX_OP_DELETE) ? "Delete" : "Insert" );
		return AS_SINDEX_ERR_PARAM;
	}
	if (numbins == 0) {
		GTRACE(META, debug, "Insert of 0 bins. No-op");
		return AS_SINDEX_ERR_PARAM;
	}

	// Maximum matches are equal to number of bins
	int8_t simatches[numbins];
	int num_matches = 0;
	if (as_sindex__get_simatches_by_sbin(ns, set, sbin,
				numbins, simatches, (op != AS_SINDEX_OP_READ), &num_matches) != AS_SINDEX_OK) {
		// It is ok to not find index
		return AS_SINDEX_ERR_NOTFOUND;
	}

	int ret[num_matches];
	for (int i = 0; i < num_matches; i++) {
		ret[i]                  = AS_SINDEX_OK;
		as_sindex          *si  = &ns->sindex[simatches[i]];
		as_sindex_metadata *imd = si->imd;
		if (!imd) {
			cf_warning(AS_SINDEX, "Selected imd at index %d has null imd",
					   simatches[i]);
			continue;
		}
		SITRACE(si, DML, debug, " Secondary Index Op Start------------- ");
		// single column index special case
		as_sindex_key skey; skey.num_binval = 0;
		ret[i] = as_sindex__skey_from_sbin_rd(imd, numbins, sbin, rd, &skey);
		// Record may not have all bins for the selected index
		if (AS_SINDEX_OK != ret[i]) {
			SITRACE(si, DML, debug, "No matching skey could be formed");
			continue;
		}
        ret[i] = as_sindex__op_by_skey(si, &skey, rd, op);
	}

	for (int i = 0; i < num_matches; i++) {
		as_sindex *si = &ns->sindex[simatches[i]];
		cf_detail(AS_SINDEX, "Index: %s release" \
						"[bimatch: %d,simatch: %d] %d @ %d\n",
						si->imd->iname, si->imd->bimatch, si->simatch, si->state, i);
		AS_SINDEX_RELEASE(si);
	}
	// TODO: Resolve what to return
	//return ret[0];
	return AS_SINDEX_OK;
}

void
as_sindex__stats_clear(as_sindex *si) {
	as_sindex_stat *s = &si->stats;
	
	s->n_objects            = 0;
	
	s->n_reads              = 0;
	s->read_errs            = 0;

	s->n_writes             = 0;
	s->write_errs           = 0;

	s->n_deletes            = 0;
	s->delete_errs          = 0;
	
	s->loadtime             = 0;
	s->recs_pending         = 0;

	s->n_defrag_records     = 0;
	s->defrag_time          = 0;

	// Aggregation stat
	s->n_aggregation        = 0;
	s->agg_response_size    = 0;
	s->agg_num_records      = 0;
	s->agg_errs             = 0;
	// Lookup stats
	s->n_lookup             = 0;
	s->lookup_response_size = 0;
	s->lookup_num_records   = 0;
	s->lookup_errs          = 0;

	si->enable_histogram = false;
	histogram_clear(s->_write_hist);
	histogram_clear(s->_delete_hist);
	histogram_clear(s->_query_hist);
	histogram_clear(s->_query_cl_hist);
	histogram_clear(s->_query_ai_hist);
	histogram_clear(s->_query_rcnt_hist);
	histogram_clear(s->_query_diff_hist);
}

void
as_sindex_gconfig_default(as_config *c)
{
	c->sindex_data_max_memory         = ULONG_MAX;
	c->sindex_data_memory_used        = 0;
	c->sindex_populator_scan_priority = 3;  // default priority = normal
}
void
as_sindex__config_default(as_sindex *si)
{
	si->config.defrag_period    = 1000;
	si->config.defrag_max_units = 1000;
	si->config.data_max_memory  = ULONG_MAX; // No Limit
	// related non config value defaults
	si->data_memory_used        = 0;
	si->config.flag             = 1; // Default is - index is active
}

void
as_sindex_config_var_copy(as_sindex *to_si, as_sindex_config_var *from_si_cfg)
{
	to_si->config.defrag_period    = from_si_cfg->defrag_period;
	to_si->config.defrag_max_units = from_si_cfg->defrag_max_units;
	to_si->config.data_max_memory  = from_si_cfg->data_max_memory;
	to_si->trace_flag              = from_si_cfg->trace_flag;
	to_si->enable_histogram        = from_si_cfg->enable_histogram;
	to_si->config.flag             = from_si_cfg->ignore_not_sync_flag;
}

void
as_sindex__create_pmeta(as_sindex *si, int simatch, int nptr)
{
	if (!si) return;
	if (nptr == 0) return;
	si->imd->pimd = cf_malloc(nptr * sizeof(as_sindex_pmetadata));
	memset(si->imd->pimd, 0, nptr*sizeof(as_sindex_pmetadata));

	pthread_rwlockattr_t rwattr;
	if (pthread_rwlockattr_init(&rwattr))
		cf_crash(AS_AS,
				"pthread_rwlockattr_init: %s", cf_strerror(errno));
	if (pthread_rwlockattr_setkind_np(&rwattr,
				PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP))
		cf_crash(AS_TSVC,
				"pthread_rwlockattr_setkind_np: %s",cf_strerror(errno));

	for (int i = 0; i < nptr; i++) {
		as_sindex_pmetadata *pimd = &si->imd->pimd[i];
		if (pthread_rwlock_init(&pimd->slock, NULL)) {
			cf_crash(AS_SINDEX,
					"Could not create secondary index dml mutex ");
		}
		if (ai_post_index_creation_setup_pmetadata(si->imd, pimd,
													simatch, i)) {
			cf_crash(AS_SINDEX,
					"Something went reallly bad !!!");
		}
	}
}

// Hash a binname string.
static inline uint32_t
as_sindex__set_binid_hash_fn(void* p_key)
{
	return (uint32_t)cf_hash_fnv(p_key, sizeof(uint32_t));
}

// Hash a binname string.
static inline uint32_t
as_sindex__iname_hash_fn(void* p_key)
{
	return (uint32_t)cf_hash_fnv(p_key, strlen((const char*)p_key));
}

void
as_sindex__setup_histogram(as_sindex *si)
{
	char hist_name[AS_ID_INAME_SZ+64];
	sprintf(hist_name, "%s_write_hist", si->imd->iname);

	if (NULL == (si->stats._write_hist = histogram_create(hist_name)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex write histogram");

	sprintf(hist_name, "%s_delete_hist", si->imd->iname);
	if (NULL == (si->stats._delete_hist = histogram_create(hist_name)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex delete histogram");

	sprintf(hist_name, "%s_query_hist", si->imd->iname);
	if (NULL == (si->stats._query_hist = histogram_create(hist_name)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex query histogram");

	sprintf(hist_name, "%s_query_ai_hist", si->imd->iname);
	if (NULL == (si->stats._query_ai_hist = histogram_create(hist_name)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex query alc histogram");

	sprintf(hist_name, "%s_query_cl_hist", si->imd->iname);
	if (NULL == (si->stats._query_cl_hist = histogram_create(hist_name)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex query cl histogram");

	sprintf(hist_name, "%s_query_row_count_hist", si->imd->iname);
	if (NULL == (si->stats._query_rcnt_hist = histogram_create(hist_name)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex query row count histogram");

	sprintf(hist_name, "%s_query_diff_hist", si->imd->iname);
	if (NULL == (si->stats._query_diff_hist = histogram_create(hist_name)))
		cf_warning(AS_SINDEX, "couldn't create histogram for sindex query diff histogram");

}

int
as_sindex__destroy_histogram(as_sindex *si)
{
	if (si->stats._write_hist)  cf_free(si->stats._write_hist);
	if (si->stats._delete_hist) cf_free(si->stats._delete_hist);
	if (si->stats._query_hist)  cf_free(si->stats._query_hist);
	if (si->stats._query_ai_hist) cf_free(si->stats._query_ai_hist);
	if (si->stats._query_cl_hist)  cf_free(si->stats._query_cl_hist);
	if (si->stats._query_rcnt_hist) cf_free(si->stats._query_rcnt_hist);
	if (si->stats._query_diff_hist) cf_free(si->stats._query_diff_hist);
	return 0;
}

/*
 * Main initialization function. Talks to Aerospike Index to pull up all the indexes
 * and populates sindex hanging from namespace
 */
int
as_sindex_init(as_namespace *ns)
{
	ns->sindex = cf_malloc(sizeof(as_sindex) * AS_SINDEX_MAX);
	if (!ns->sindex)
		cf_crash(AS_SINDEX,
				"Could not allocation memory for secondary index");

	ns->sindex_cnt = 0;
	for (int i = 0; i < AS_SINDEX_MAX; i++) {
		as_sindex *si = &ns->sindex[i];
		memset(si, 0, sizeof(as_sindex));
		si->state     = AS_SINDEX_INACTIVE;
		si->stats._delete_hist = NULL;
		si->stats._query_hist = NULL;
		si->stats._query_ai_hist = NULL;
		si->stats._query_cl_hist = NULL;
		si->stats._query_rcnt_hist = NULL;
		si->stats._query_diff_hist = NULL;
	}
	
	// binid to simatch lookup
	if (SHASH_OK != shash_create(&ns->sindex_set_binid_hash,
						as_sindex__set_binid_hash_fn, AS_SET_NAME_MAX_SIZE + 14, sizeof(uint32_t),
						AS_SINDEX_MAX, 0)) {
		cf_crash(AS_AS, "Couldn't create sindex binid hash");
	}

	// iname to simatch lookup
	if (SHASH_OK != shash_create(&ns->sindex_iname_hash,
						as_sindex__iname_hash_fn, AS_ID_INAME_SZ, sizeof(uint32_t),
						AS_SINDEX_MAX, 0)) {
		cf_crash(AS_AS, "Couldn't create sindex iname hash");
	}

	return AS_SINDEX_OK;
}

/*
 * Cleanup function NOOP
 */
void
as_sindex_shutdown(as_namespace *ns)
{
	// NO OP
	return;
}

// Reserve the sindex so it does not get deleted under the hood
int
as_sindex_reserve(as_sindex *si, char *fname, int lineno)
{
	if (si->imd) cf_rc_reserve(si->imd);
	int count = cf_rc_count(si->imd);
	SITRACE(si, RESERVE, debug, "Index %s in %d state Reserved to reference count %d < 2 at %s:%d", si->imd->iname, si->state, count, fname, lineno);
	return AS_SINDEX_OK;
}

int
as_sindex_reserve_all(as_namespace *ns, int *imatch)
{
	int count = 0;
	SINDEX_GRLOCK();
	// May want to let the delete go through ...
	if (ns->sindex) {
		for (int i = 0; i < AS_SINDEX_MAX; i++) {
			as_sindex *si = &ns->sindex[i];
			if (!as_sindex_isactive(si))    continue;
			imatch[count++] = i;
			AS_SINDEX_RESERVE(si);
		}
	}
	SINDEX_GUNLOCK();
	return count;
}


/*
 * Release, queue up the request for the destroy to clean up Aerospike Index thread,
 * Not done inline because main write thread could release the last reference.
 */
int
as_sindex_release(as_sindex *si, char *fname, int lineno)
{
	if (!si) return AS_SINDEX_OK;
	// Can be checked without locking
	uint64_t val = cf_rc_release(si->imd);
	if (val == 0) {
		cf_assert((si->state == AS_SINDEX_DESTROY),
					AS_SINDEX, CF_CRITICAL,
					" Invalid state at cleanup");
		cf_assert(!(si->state & AS_SINDEX_FLAG_DESTROY_CLEANUP),
					AS_SINDEX, CF_CRITICAL,
					" Invalid state at cleanup");
		si->flag |= AS_SINDEX_FLAG_DESTROY_CLEANUP;
		if (CF_QUEUE_OK != cf_queue_push(g_sindex_destroy_q, &si)) {
			return AS_SINDEX_ERR;
		}
	}
	else {
		SINDEX_RLOCK(&si->imd->slock);
		SITRACE(si, RESERVE, debug, "Index %s in %d state Released "
					"to reference count %d < 2 at %s:%d",
					si->imd->iname, si->state, val, fname, lineno);
		// Display a warning when rc math is messed-up during sindex-delete
		if(si->state == AS_SINDEX_DESTROY){
			cf_warning(AS_SINDEX,"Returning from a sindex destroy op for: %s with reference count %"PRIu64"", si->imd->iname, val);
		}
		SINDEX_UNLOCK(&si->imd->slock);
	}
	return AS_SINDEX_OK;
}

// Free if IMD has allocated the info in it
int
as_sindex_imd_free(as_sindex_metadata *imd)
{
	if (!imd) return 1;
	if (imd->ns_name)  cf_free(imd->ns_name);
	if (imd->iname)    cf_free(imd->iname);
	if (imd->set)      cf_free(imd->set);
	if (imd->flag & IMD_FLAG_LOCKSET)           pthread_rwlock_destroy(&imd->slock);
	if (imd->num_bins) {
		for (int i=0; i<imd->num_bins; i++) {
			if (imd->bnames[i]) {
				cf_free(imd->bnames[i]);
				imd->bnames[i] = NULL;
			}
		}
	}
	return AS_SINDEX_OK;
}


void
as_sindex_destroy_pmetadata(as_sindex *si)
{
	pthread_rwlockattr_t rwattr;
	if (pthread_rwlockattr_init(&rwattr))
		cf_crash(AS_AS,
				"pthread_rwlockattr_init: %s", cf_strerror(errno));
	if (pthread_rwlockattr_setkind_np(&rwattr,
				PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP))
		cf_crash(AS_TSVC,
				"pthread_rwlockattr_setkind_np: %s",cf_strerror(errno));

	for (int i = 0; i < si->imd->nprts; i++) {
		as_sindex_pmetadata *pimd = &si->imd->pimd[i];
		pthread_rwlock_destroy(&pimd->slock);
	}
	as_sindex__destroy_histogram(si);
	cf_free(si->imd->pimd);
	si->imd->pimd = NULL;
}

int as_sindex_create_check_params(as_namespace* ns, as_sindex_metadata *imd);

/*
 * Description     : Checks whether an index with the same defn already exists.
 *                   Index defn ={index_name, bin_name, bintype, set_name, ns_name}
 *
 * Parameters      : ns  -> namespace in which index is created
 *                   imd -> imd for create request (does not have binid populated)
 *
 * Returns         : true  if index with the given defn already exists.
 *                   false otherwise
 *
 * Synchronization : Required lock acquired by lookup functions.
 */
bool
as_sindex_exists_by_defn(as_namespace* ns, as_sindex_metadata* imd)
{
	char *iname   = imd->iname;
	char *set     = imd->set;
	as_sindex* si = as_sindex__lookup(ns, iname, -1, set,
			AS_SINDEX_LOOKUP_FLAG_ISACTIVE
			| AS_SINDEX_LOOKUP_FLAG_SETCHECK);
	if(!si) {
		return false;
	}
	int binid     = as_bin_get_id(ns, imd->bnames[0]);
	for (int i = 0; i < AS_SINDEX_BINMAX; i++) {
		if(si->imd->bnames[i] && imd->bnames[i]) {
			if (binid == si->imd->binid[i]
					&& !strcmp(imd->bnames[i], si->imd->bnames[i])
					&& imd->btype[i] == si->imd->btype[i]) {
				AS_SINDEX_RELEASE(si);
				return true;
			}
		}
	}
	AS_SINDEX_RELEASE(si);
	return false;
}

// Always make this function get cfg default from as_sindex structure's values,
// instead of hard-coding it : useful when the defaults change.
// This also gets called during config-file init, at that time, we are using a
// dummy variable init.
void
as_sindex_config_var_default(as_sindex_config_var *si_cfg)
{
	// Mandatory memset, Totally worth the cost
	// Do not remove
	memset(si_cfg, 0, sizeof(as_sindex_config_var));

	as_sindex from_si;
	as_sindex__config_default(&from_si);

	// 2 of the 6 variables : enable-histogram and trace-flag are not a part of default-settings for si
	si_cfg->defrag_period        = from_si.config.defrag_period;
	si_cfg->defrag_max_units     = from_si.config.defrag_max_units;
	// related non config value defaults
	si_cfg->data_max_memory      = from_si.config.data_max_memory;
	si_cfg->ignore_not_sync_flag = from_si.config.flag;
}

/*
 * Client API to create new secondary index
 */
int
as_sindex_create(as_namespace *ns, as_sindex_metadata *imd, bool user_create)
{
	int ret = -1;
	// Ideally there should be one lock per namespace, but because the
	// Aerospike Index metadata is single global structure we need a overriding
	// lock for that. NB if it becomes per namespace have a file lock
	SINDEX_GWLOCK();
	if (as_sindex__lookup_lockfree(ns, imd->iname, -1, imd->set,
					AS_SINDEX_LOOKUP_FLAG_NORESERVE)) {
		cf_detail(AS_SINDEX,"Index %s already exists", imd->iname);
		SINDEX_GUNLOCK();
		return AS_SINDEX_ERR_FOUND;
	}
	int i, chosen_id;
	as_sindex *si = NULL;
	for (i = 0; i < AS_SINDEX_MAX; i++) {
		if (ns->sindex[i].state == AS_SINDEX_INACTIVE) {
			si = &ns->sindex[i];
			chosen_id = i; break;
		}
	}

	if (!si || (i == AS_SINDEX_MAX))  {
		cf_warning(AS_SINDEX,
				"Maxed out secondary index limit no more indexes allowed");
		SINDEX_GUNLOCK();
		return AS_SINDEX_ERR;
	}

	imd->nprts  = NUM_SINDEX_PARTITIONS;
	int id      = chosen_id;
	si          = &ns->sindex[id];
	as_sindex_metadata *qimd;
	if (!imd->oindx) {
		if (as_sindex__populate_binid(ns, imd)) {
			SINDEX_GUNLOCK();
			return AS_SINDEX_ERR_PARAM;
		}
	}

	// Reason for doing it upfront is to fail fast. Without doing
	// whole bunch of Aerospike Index work

	// add 14 for number of characters in any uint32
	char set_binid[AS_SET_NAME_MAX_SIZE + 14];
	memset(set_binid, 0, AS_SET_NAME_MAX_SIZE + 14);

	if (imd->set == NULL ) {
		// sindex can be over a NULL set
		sprintf(set_binid, "_%d", imd->binid[0]);
	}
	else {
		sprintf(set_binid, "%s_%d", imd->set, imd->binid[0]);
	}

	if (SHASH_OK != shash_put(ns->sindex_set_binid_hash, (void *)set_binid, (void *)&chosen_id)) {
		cf_warning(AS_SINDEX, "Internal error ... Duplicate element found sindex binid hash [%s %s]",
						imd->iname, as_bin_get_name_from_id(ns, imd->binid[0]));
		SINDEX_GUNLOCK();
		return AS_SINDEX_ERR;
	}
	cf_detail(AS_SINDEX, "Put binid simatch %d->%d", imd->binid[0], chosen_id);

	char iname[AS_ID_INAME_SZ]; memset(iname, 0, AS_ID_INAME_SZ);
	snprintf(iname, strlen(imd->iname)+1, "%s", imd->iname);
	if (SHASH_OK != shash_put(ns->sindex_iname_hash, (void *)iname, (void *)&chosen_id)) {
		cf_warning(AS_SINDEX, "Internal error ... Duplicate element found sindex iname hash [%s %s]",
						imd->iname, as_bin_get_name_from_id(ns, imd->binid[0]));
		shash_delete(ns->sindex_set_binid_hash, (void *)set_binid);
		SINDEX_GUNLOCK();
		return AS_SINDEX_ERR;
	}
	cf_detail(AS_SINDEX, "Put iname simatch %s:%d->%d", iname, strlen(imd->iname), chosen_id);

	as_sindex__dup_meta(imd, &qimd, true);
	qimd->si    = si;
	qimd->nprts = imd->nprts;
	int bimatch = -1;

	ret = ai_btree_create(qimd, id, &bimatch, imd->nprts);
	if (!ret) { // create ref counted index metadata & hang it from sindex
		si->imd         = qimd;
		si->imd->bimatch = bimatch;
		si->state       = AS_SINDEX_ACTIVE;
		si->trace_flag  = 0;
		si->desync_cnt  = 0;
		si->flag        = AS_SINDEX_FLAG_WACTIVE;
		si->new_imd     = NULL;
		as_sindex__create_pmeta(si, id, imd->nprts);
		// Always tune si to default settings to start with
		as_sindex__config_default(si);

		// If this si has a valid config-item and this si was created by smd boot-up,
		// then set the config-variables from si_cfg_array.

		// si_cfg_var_hash is deliberately kept as a transient structure.
		// Its applicable only for boot-time si-creation via smd
		// In the case of sindex creations via aql, i.e dynamic si creation,
		// this array wont exist and all si configs will be default configs.
		if (ns->sindex_cfg_var_hash) {
			// Check for duplicate si stanzas with the same si-name ?
			as_sindex_config_var check_si_conf;

			if (SHASH_OK == shash_get(ns->sindex_cfg_var_hash, (void *)iname, (void *)&check_si_conf)){
				// A valid config stanza exists for this si entry
				// Copy the config over to the new si
				// delete the old hash entry
				cf_info(AS_SINDEX,"Found custom configuration for SI:%s, applying", imd->iname);
				as_sindex_config_var_copy(si, &check_si_conf);
				shash_delete(ns->sindex_cfg_var_hash,  (void *)iname);
				check_si_conf.conf_valid_flag = true;
				shash_put_unique(ns->sindex_cfg_var_hash, (void *)iname, (void *)&check_si_conf);
			}
		}

		as_sindex__setup_histogram(si);
		as_sindex__stats_clear(si);

		ns->sindex_cnt++;
		si->ns          = ns;
		si->simatch     = chosen_id;
		as_sindex_reserve_data_memory(si->imd, ai_btree_get_isize(si->imd));
		
		// Only trigger scan if this create is done after boot
		if (user_create && g_sindex_boot_done) {
			// Reserve it before pushing it into queue
			AS_SINDEX_RESERVE(si);
			SINDEX_GUNLOCK();
			int rv = cf_queue_push(g_sindex_populate_q, &si);
			if (CF_QUEUE_OK != rv) {
				cf_warning(AS_SINDEX, "Failed to queue up for population... index=%s "
							"Internal Queue Error rv=%d, try dropping and recreating",
							si->imd->iname, rv);
			}
		} else {
			// Internal create is called before storage is initialized. Loading
			// of storage will fill up the indexes no need to queue it up for scan
			SINDEX_GUNLOCK();
		}
	} else {
		// TODO: When alc_btree_create fails, accept_cb should have a better
		//       way to handle failure. Currently it maintains a dummy si
		//       structures with not created flag. accept_cb should repair
		//       such dummy si structures and retry alc_btree_create.
		shash_delete(ns->sindex_set_binid_hash, (void *)set_binid);
		shash_delete(ns->sindex_iname_hash, (void *)iname);
		as_sindex_imd_free(qimd);
		cf_debug(AS_SINDEX, "Create index %s failed ret = %d",
				imd->iname, ret);
		SINDEX_GUNLOCK();
	}
	return ret;
}


/*
 * Client API to mark index population finished, tick it ready for read
 */
int
as_sindex_populate_done(as_sindex *si)
{
	int ret = AS_SINDEX_OK;
	SINDEX_WLOCK(&si->imd->slock);
	// Setting flag is atomic: meta lockless
	si->flag |= AS_SINDEX_FLAG_RACTIVE;
	si->flag &= ~AS_SINDEX_FLAG_POPULATING;
	SINDEX_UNLOCK(&si->imd->slock);
	return ret;
}

bool
as_sindex_delete_checker(as_namespace *ns, as_sindex_metadata *imd)
{
	if (as_sindex__lookup_lockfree(ns, imd->iname, 0, NULL,
			AS_SINDEX_LOOKUP_FLAG_NORESERVE
			| AS_SINDEX_LOOKUP_FLAG_ISACTIVE)) {
		return true;
	} else {
		return false;
	}
}

/*
 * Client API to destroy secondary index, mark destroy
 * Deletes via smd or info-command user-delete requests.
 */
int
as_sindex_destroy(as_namespace *ns, as_sindex_metadata *imd)
{
	SINDEX_GWLOCK();
	as_sindex *si   = as_sindex__lookup_lockfree(ns, imd->iname, 0, NULL,
						AS_SINDEX_LOOKUP_FLAG_NORESERVE
						| AS_SINDEX_LOOKUP_FLAG_ISACTIVE);

	if (si) {
		if (imd->post_op == 1) {
			as_sindex__dup_meta(imd, &si->new_imd, false);
		}
		else {
			si->new_imd = NULL;
		}
		si->state = AS_SINDEX_DESTROY;
		AS_SINDEX_RELEASE(si);
		SINDEX_GUNLOCK();
		return AS_SINDEX_OK;
	} else {
		SINDEX_GUNLOCK();
		return AS_SINDEX_ERR_NOTFOUND;
	}
}

/*
 * Client API to insert value into the index, pick value from passed in
 * record. Acquires the imd lock, caller should have reserved si.
 */
int
as_sindex_putall_rd(as_namespace *ns, as_storage_rd *rd)
{
	as_sindex *sindex_arr[AS_SINDEX_MAX];
	SINDEX_GRLOCK();
	int ret = AS_SINDEX_OK;
	int cnt = 0;

	// Reserve it before pushing it into queue
	for (int i = 0; i < AS_SINDEX_MAX; i++) {
		as_sindex *si = &ns->sindex[i];
		if (!as_sindex_isactive(si))  continue;
		AS_SINDEX_RESERVE(si);
		sindex_arr[cnt++] = si;
	}
	SINDEX_GUNLOCK();

	for (int i=0; i<cnt; i++) {
		as_sindex *si = sindex_arr[i];
		as_sindex_put_rd(si, rd);
		AS_SINDEX_RELEASE(si);
	}
	return ret;
}

/*
 * Client API to insert value into the index, pick value from passed in
 * record. Acquires the imd lock, caller should have reserved si.
 */
int
as_sindex_put_rd(as_sindex *si, as_storage_rd *rd)
{
	int ret = -1;
	as_sindex_metadata *imd = si->imd;
	as_sindex_key skey; memset(&skey, 0, sizeof(as_sindex_key));

	// Validate Set name. Other function do this check while
	// performing searching for simatch.
	const char *setname = NULL;
	if (as_index_has_set(rd->r)) setname = as_index_get_set_name(rd->r, si->ns);
	SINDEX_RLOCK(&imd->slock);
	if (!as_sindex__setname_match(imd, setname)) {
		SINDEX_UNLOCK(&imd->slock);
		return AS_SINDEX_OK;
	}
	ret = as_sindex__pre_op_assert(si, AS_SINDEX_OP_INSERT);
	if (AS_SINDEX_OK != ret) {
		SINDEX_UNLOCK(&imd->slock);
		return ret;
	}

	uint64_t starttime = cf_getus();
	if (AS_SINDEX_OK == as_sindex__skey_from_rd(imd, &skey, rd)) {
		as_sindex_pmetadata *pimd = &imd->pimd[ai_btree_key_hash(imd, &skey.b[0])];
		SINDEX_WLOCK(&pimd->slock);
		ret = ai_btree_put(imd, pimd, &skey, (void *)&rd->keyd);
		SINDEX_UNLOCK(&pimd->slock);
		as_sindex__process_ret(si, ret, AS_SINDEX_OP_INSERT, starttime,
							    __LINE__);
		as_sindex__skey_release(&skey);
	}
	SINDEX_UNLOCK(&imd->slock);
	return ret;
}

/*
 * Returns -
 * 		AS_SINDEX_ERR_PARAM
 *		o/w return value from ai_btree_query
 *
 * Notes -
 * 		Client API to do range get from index based on passed in range key, returns
 * 		digest list
 *
 * Synchronization -
 * 		
 */
int
as_sindex_query(as_sindex *si, as_sindex_range *srange, as_sindex_qctx *qctx)
{
	if ((!si || !srange)) return AS_SINDEX_ERR_PARAM;
	as_sindex_metadata *imd = si->imd;
	SINDEX_RLOCK(&imd->slock);
	SINDEX_RLOCK(&imd->pimd[qctx->pimd_idx].slock);
	int ret = as_sindex__pre_op_assert(si, AS_SINDEX_OP_READ);
	if (AS_SINDEX_OK != ret) {
		SINDEX_UNLOCK(&imd->pimd[qctx->pimd_idx].slock);
		SINDEX_UNLOCK(&imd->slock);
		return ret;
	}
	uint64_t starttime = cf_getus();
	ret = ai_btree_query(imd, srange, qctx);
	as_sindex__process_ret(si, ret, AS_SINDEX_OP_READ, starttime, __LINE__);
	SINDEX_UNLOCK(&imd->pimd[qctx->pimd_idx].slock);
	SINDEX_UNLOCK(&imd->slock);
	return ret;
}

int
as_sindex_repair(as_namespace *ns, as_sindex_metadata *imd)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;
	as_sindex *si = as_sindex__lookup(ns, imd->iname, -1 , NULL,
						AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if (si) {
		if (si->desync_cnt == 0) {
			return AS_SINDEX_OK;
		}
		int rv = cf_queue_push(g_sindex_populate_q, &si);
		if (CF_QUEUE_OK != rv) {
			cf_warning(AS_SINDEX, "Failed to queue up for population... index=%s "
					"Internal Queue Error rv=%d, retry repair", si->imd->iname, rv);
			AS_SINDEX_RELEASE(si);
			return AS_SINDEX_ERR;
		}
		return AS_SINDEX_OK;
	}
	return AS_SINDEX_ERR_NOTFOUND;
}

int
as_sindex_stats_str(as_namespace *ns, as_sindex_metadata *imd, cf_dyn_buf *db)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;

	as_sindex *si = as_sindex__lookup(ns, imd->iname, -1 , NULL,
						AS_SINDEX_LOOKUP_FLAG_ISACTIVE);

	if (!si)
		return AS_SINDEX_ERR_NOTFOUND;

	// A good thing to cache the stats first.
	int      ns_objects  = ns->n_objects;
	uint64_t si_objects  = cf_atomic64_get(si->stats.n_objects);
	uint64_t pending     = cf_atomic64_get(si->stats.recs_pending);
	uint64_t si_memory   = cf_atomic64_get(si->data_memory_used);
	// To protect the pimd while accessing it.
	SINDEX_RLOCK(&si->imd->slock);
	uint64_t n_keys      = ai_btree_get_numkeys(si->imd);
	SINDEX_UNLOCK(&si->imd->slock);
	cf_dyn_buf_append_string(db, "keys=");
	cf_dyn_buf_append_uint64(db,  n_keys);
	cf_dyn_buf_append_string(db, ";objects=");
	cf_dyn_buf_append_int(   db,  si_objects);
	SINDEX_RLOCK(&si->imd->slock);
	uint64_t i_size      = ai_btree_get_isize(si->imd);
	uint64_t n_size      = ai_btree_get_nsize(si->imd);
	SINDEX_UNLOCK(&si->imd->slock);
	cf_dyn_buf_append_string(db, ";ibtr_memory_used=");
	cf_dyn_buf_append_uint64(db,  i_size);
	cf_dyn_buf_append_string(db, ";nbtr_memory_used=");
	cf_dyn_buf_append_uint64(db,  n_size);
	cf_dyn_buf_append_string(db, ";si_accounted_memory=");
	cf_dyn_buf_append_uint64(db,  si_memory);
	cf_dyn_buf_append_string(db, ";load_pct=");
	if (si->flag & AS_SINDEX_FLAG_RACTIVE) {
		cf_dyn_buf_append_string(db, "100");
	} else {
		if (pending > ns_objects || pending < 0) {
			cf_dyn_buf_append_uint64(db, 100);
		} else {
			cf_dyn_buf_append_uint64(db, (ns_objects == 0) ? 100 : 100 - ((100 * pending) / ns_objects));
		}
	}

	cf_dyn_buf_append_string(db, ";loadtime=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.loadtime));
	// writes
	cf_dyn_buf_append_string(db, ";stat_write_reqs=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.n_writes));
	cf_dyn_buf_append_string(db, ";stat_write_success=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.n_writes) - cf_atomic64_get(si->stats.write_errs));
	cf_dyn_buf_append_string(db, ";stat_write_errs=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.write_errs));
	// delete
	cf_dyn_buf_append_string(db, ";stat_delete_reqs=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.n_deletes));
	cf_dyn_buf_append_string(db, ";stat_delete_success=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.n_deletes) - cf_atomic64_get(si->stats.delete_errs));
	cf_dyn_buf_append_string(db, ";stat_delete_errs=");
	cf_dyn_buf_append_uint64(db, cf_atomic64_get(si->stats.delete_errs));
	// defrag
	cf_dyn_buf_append_string(db, ";stat_gc_recs=");
	cf_dyn_buf_append_int(   db, cf_atomic64_get(si->stats.n_defrag_records));
	cf_dyn_buf_append_string(db, ";stat_gc_time=");
	cf_dyn_buf_append_int(   db, cf_atomic64_get(si->stats.defrag_time));

	// Cache values
	uint64_t agg        = cf_atomic64_get(si->stats.n_aggregation);
	uint64_t agg_rec    = cf_atomic64_get(si->stats.agg_num_records);
	uint64_t agg_size   = cf_atomic64_get(si->stats.agg_response_size);
	uint64_t lkup       = cf_atomic64_get(si->stats.n_lookup);
	uint64_t lkup_rec   = cf_atomic64_get(si->stats.lookup_num_records);
	uint64_t lkup_size  = cf_atomic64_get(si->stats.lookup_response_size);
	uint64_t query      = agg      + lkup;
	uint64_t query_rec  = agg_rec  + lkup_rec;
	uint64_t query_size = agg_size + lkup_size;

	// Query
	cf_dyn_buf_append_string(db, ";query_reqs=");
	cf_dyn_buf_append_uint64(db,   query);
	cf_dyn_buf_append_string(db, ";query_avg_rec_count=");
	cf_dyn_buf_append_uint64(db,   query     ? query_rec  / query     : 0);
	cf_dyn_buf_append_string(db, ";query_avg_record_size=");
	cf_dyn_buf_append_uint64(db,   query_rec ? query_size / query_rec : 0);
	// Aggregation
	cf_dyn_buf_append_string(db, ";query_agg=");
	cf_dyn_buf_append_uint64(db,   agg);
	cf_dyn_buf_append_string(db, ";query_agg_avg_rec_count=");
	cf_dyn_buf_append_uint64(db,   agg       ? agg_rec    / agg       : 0);
	cf_dyn_buf_append_string(db, ";query_agg_avg_record_size=");
	cf_dyn_buf_append_uint64(db,   agg_rec   ? agg_size   / agg_rec   : 0);
	//Lookup
	cf_dyn_buf_append_string(db, ";query_lookups=");
	cf_dyn_buf_append_uint64(db,   lkup);
	cf_dyn_buf_append_string(db, ";query_lookup_avg_rec_count=");
	cf_dyn_buf_append_uint64(db,   lkup      ? lkup_rec   / lkup      : 0);
	cf_dyn_buf_append_string(db, ";query_lookup_avg_record_size=");
	cf_dyn_buf_append_uint64(db,   lkup_rec  ? lkup_size  / lkup_rec  : 0);

	//CONFIG
	cf_dyn_buf_append_string(db, ";gc-period=");
	cf_dyn_buf_append_uint64(db, si->config.defrag_period);
	cf_dyn_buf_append_string(db, ";gc-max-units=");
	cf_dyn_buf_append_uint32(db, si->config.defrag_max_units);
	cf_dyn_buf_append_string(db, ";data-max-memory=");
	if (si->config.data_max_memory == ULONG_MAX) {
		cf_dyn_buf_append_uint64(db, si->config.data_max_memory);
	} else {
		cf_dyn_buf_append_string(db, "ULONG_MAX");
	}

	cf_dyn_buf_append_string(db, ";tracing=");
	cf_dyn_buf_append_uint64(db, si->trace_flag);
	cf_dyn_buf_append_string(db, ";histogram=");
	cf_dyn_buf_append_string(db, si->enable_histogram ? "true" : "false");
	cf_dyn_buf_append_string(db, ";ignore-not-sync=");
	cf_dyn_buf_append_string(db, (si->config.flag & AS_SINDEX_CONFIG_IGNORE_ON_DESYNC) ? "true" : "false");

	AS_SINDEX_RELEASE(si);
	// Release reference
	return AS_SINDEX_OK;
}

// NB:  These are distinctly different from the column names in AA!
static char *as_col_type_defs[] =
  { "NONE",      "NUMERIC",   "NUMERIC",   "STRING", "FLOAT", "UNDEFINED",
    "UNDEFINED", "UNDEFINED", "UNDEFINED", "UNDEFINED" };

/*
 * Client API to describe index based passed in imd, populates passed info fully
 */
int
as_sindex_describe_str(as_namespace *ns, as_sindex_metadata *imd, cf_dyn_buf *db)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;

	as_sindex *si = as_sindex__lookup(ns, imd->iname, -1, NULL, 0);
	if ((si) && (si->imd)) {
		SINDEX_RLOCK(&si->imd->slock)
		as_sindex_metadata *imd = si->imd;
		cf_dyn_buf_append_string(db, "indexname=");
		cf_dyn_buf_append_string(db, imd->iname);
		cf_dyn_buf_append_string(db, ";ns=");
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_string(db, ";set=");
		cf_dyn_buf_append_string(db, imd->set == NULL ? "NULL" : imd->set);
		cf_dyn_buf_append_string(db, ";numbins=");
		cf_dyn_buf_append_uint64(db, imd->num_bins);
		cf_dyn_buf_append_string(db, ";bins=");
		for (int i = 0; i < imd->num_bins; i++) {
			if (i) cf_dyn_buf_append_string(db, ",");
			cf_dyn_buf_append_buf(db, (uint8_t *)imd->bnames[i], strlen(imd->bnames[i]));
			cf_dyn_buf_append_string(db, ":");
			// HACKY
			cf_dyn_buf_append_string(db, as_col_type_defs[as_sindex_pktype_from_sktype(imd->btype[i])]);
		}

		// Index State
		if (si->state == AS_SINDEX_ACTIVE) {
			if (si->flag & AS_SINDEX_FLAG_RACTIVE) {
				cf_dyn_buf_append_string(db, ";state=RW");
			}
			else if (si->flag & AS_SINDEX_FLAG_WACTIVE) {
				cf_dyn_buf_append_string(db, ";state=WO");
			}
			else {
				cf_dyn_buf_append_string(db, ";state=A");
			}
		}
		else if (si->state == AS_SINDEX_INACTIVE) {
			cf_dyn_buf_append_string(db, ";state=I");
		}
		else {
			cf_dyn_buf_append_string(db, ";state=D");
		}
		SINDEX_UNLOCK(&si->imd->slock)
		AS_SINDEX_RELEASE(si);
		return AS_SINDEX_OK;
	} else {
		return AS_SINDEX_ERR_NOTFOUND;
	}
	return AS_SINDEX_OK;
}

/*
 * Client API to start namespace scan to populate secondary index. The scan
 * is only performed in the namespace is warm start or if its data is not in
 * memory and data is loaded from. For cold start with data in memory the indexes
 * are populate upfront.
 *
 * This call is only made at the boot time.
 */
int
as_sindex_boot_populateall()
{
	int ns_cnt            = 0;
	int old_priority   = g_config.sindex_populator_scan_priority;
	int old_g_priority = g_config.scan_priority;
	int old_g_sleep    = g_config.scan_sleep;

	// Go full throttle
	g_config.scan_priority                  = UINT_MAX;
	g_config.scan_sleep                     = 0;
	g_config.sindex_populator_scan_priority = MAX_SCAN_THREADS; // use all of it
	
	// Trigger namespace scan to populate all secondary indexes
	// mark all secondary index for a namespace as populated
	for (int i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];
		if (!ns || (ns->sindex_cnt == 0)) {
			continue;
		}
	
		// If FAST START
		// OR (Data not in memory AND load data at startup)
		if (!ns->cold_start
			|| (!ns->storage_data_in_memory)) {
			as_tscan_sindex_populateall(ns);
			cf_info(AS_SINDEX, "Queuing namespace %s for sindex population ", ns->name);
		} else {
			as_sindex_boot_populateall_done(ns);
		}
		ns_cnt++;
	}
	for (int i = 0; i < ns_cnt; i++) {
		int ret;
		// blocking call, wait till an item is popped out of Q :
		cf_queue_pop(g_sindex_populateall_done_q, &ret, CF_QUEUE_FOREVER);
		// TODO: Check for failure .. is generally fatal if it fails
	}
	g_config.scan_priority                  = old_g_priority;
	g_config.scan_sleep                     = old_g_sleep;
	g_config.sindex_populator_scan_priority = old_priority;
	g_sindex_boot_done                      = true;

	// This above flag indicates that the basic sindex boot-up loader is done
	// Go and destroy the sindex_cfg_var_hash here to prevent run-time
	// si's from getting the config-file settings.
	for (int i = 0; i < g_config.namespaces; i++) {
		as_namespace *ns = g_config.namespace[i];

		if (ns->sindex_cfg_var_hash) {
			shash_reduce(ns->sindex_cfg_var_hash, as_sindex_cfg_var_hash_reduce_fn, NULL);
	    	shash_destroy(ns->sindex_cfg_var_hash);

	    	// Assign hash to NULL at the start and end of its lifetime
			ns->sindex_cfg_var_hash = NULL;
		}

	}

	return AS_SINDEX_OK;
}

/*
 * Client API to mark all the indexes in namespace populated and ready for read
 */
int
as_sindex_boot_populateall_done(as_namespace *ns)
{
	SINDEX_GWLOCK();
	int ret = AS_SINDEX_OK;

	for (int i = 0; i < AS_SINDEX_MAX; i++) {
		as_sindex *si = &ns->sindex[i];
		if (!as_sindex_isactive(si))  continue;
		// This sindex is getting populating by it self scan
		if (si->flag & AS_SINDEX_FLAG_POPULATING) continue;
		si->flag |= AS_SINDEX_FLAG_RACTIVE;
	}
	SINDEX_GUNLOCK();
	cf_queue_push(g_sindex_populateall_done_q, &ret);
	cf_info(AS_SINDEX, "Namespace %s sindex population done", ns->name);
	return ret;
}

/*
 * Client API to check if there is secondary index on given namespace
 */
int
as_sindex_ns_has_sindex(as_namespace *ns)
{
	return (ns->sindex_cnt > 0);
}

/*
 * Client API to list all the indexes in a namespace, returns list of imd with
 * index information, Caller should free it up
 */
int
as_sindex_list_str(as_namespace *ns, cf_dyn_buf *db)
{
	SINDEX_GRLOCK();
	for (int i = 0; i < AS_SINDEX_MAX; i++) {
		if (&(ns->sindex[i]) && (ns->sindex[i].imd)) {
			as_sindex si = ns->sindex[i];
			AS_SINDEX_RESERVE(&si);
			SINDEX_RLOCK(&si.imd->slock);
			cf_dyn_buf_append_string(db, "ns=");
			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_string(db, ":set=");
			cf_dyn_buf_append_string(db, (si.imd->set) ? si.imd->set : "NULL");
			cf_dyn_buf_append_string(db, ":indexname=");
			cf_dyn_buf_append_string(db, si.imd->iname);
			cf_dyn_buf_append_string(db, ":num_bins=");
			cf_dyn_buf_append_uint64(db, si.imd->num_bins);
			cf_dyn_buf_append_string(db, ":bins=");
			for (int i = 0; i < si.imd->num_bins; i++) {
				if (i) cf_dyn_buf_append_string(db, ",");
				cf_dyn_buf_append_buf(db, (uint8_t *)si.imd->bnames[i], strlen(si.imd->bnames[i]));
				cf_dyn_buf_append_string(db, ":type=");
				cf_dyn_buf_append_string(db, Col_type_defs[as_sindex_pktype_from_sktype(si.imd->btype[i])]);
			}
			cf_dyn_buf_append_string(db, ":sync_state=");
			if (si.desync_cnt > 0) {
				cf_dyn_buf_append_string(db, "needsync");
			}
			else {
				cf_dyn_buf_append_string(db, "synced");
			}
			// Index State
			if (si.state == AS_SINDEX_ACTIVE) {
				if (si.flag & AS_SINDEX_FLAG_RACTIVE) {
					cf_dyn_buf_append_string(db, ":state=RW;");
				}
				else if (si.flag & AS_SINDEX_FLAG_WACTIVE) {
					cf_dyn_buf_append_string(db, ":state=WO;");
				}
				else {
					// should never come here.
					cf_dyn_buf_append_string(db, ":state=A;");
				}
			}
			else if (si.state == AS_SINDEX_INACTIVE) {
				cf_dyn_buf_append_string(db, ":state=I;");
			}
			else {
				cf_dyn_buf_append_string(db, ":state=D;");
			}
			SINDEX_UNLOCK(&si.imd->slock);
			AS_SINDEX_RELEASE(&si);
		}
	}
	SINDEX_GUNLOCK();
	return AS_SINDEX_OK;
}


/*
 * Calls workhorse function as_sindex__op_by_sbin to do the insert
 */
int
as_sindex_put_by_sbin(as_namespace *ns, const char *set,
		int numbins, as_sindex_bin *sbin, as_storage_rd *rd)
{
	int ret;
	GTRACE(CALLSTACK, debug, "Insert into secondary index for namespace %s",ns->name);
	ret = as_sindex__op_by_sbin(ns, set, numbins, sbin, rd,
			AS_SINDEX_OP_INSERT);
	// it is ok not to find index
	if (ret == AS_SINDEX_ERR_NOTFOUND) ret = AS_SINDEX_OK;
	return ret;
}

/*
 * Calls workhorse function as_sindex__op_by_sbin to do the delete
 */
int
as_sindex_delete_by_sbin(as_namespace *ns, const char *set,
		int numbins, as_sindex_bin *sbin,
		as_storage_rd *rd)
{
	int ret;
	GTRACE(CALLSTACK, debug, "as_sindex_delete_by_sbin");
	ret = as_sindex__op_by_sbin(ns, set, numbins, sbin, rd,
			AS_SINDEX_OP_DELETE);
	// it is ok not to find index
	if (ret == AS_SINDEX_ERR_NOTFOUND) ret = AS_SINDEX_OK;
	return ret;
}


int
as_sindex_update_by_sbin(as_namespace *ns, const char *set,
					int obins, as_sindex_bin *osbin,
					int nbins, as_sindex_bin *nsbin,
					as_storage_rd *rd)
{
	GTRACE(CALLSTACK, debug, "as_sindex_update_by_sbin");

	// walk through the sbin and filter out duplicates if value matches.
	
	int sindex_ret = AS_SINDEX_OK;
	if (obins) {
		sindex_ret = as_sindex_delete_by_sbin(ns, set, obins, osbin, rd);
	}
	if (nbins) {
		sindex_ret = as_sindex_put_by_sbin(ns, set, nbins, nsbin, rd);
	}
	return sindex_ret;
}

/*
 * Returns -
 * 		NULL - On failure
 * 		si   - On success.
 * Notes -
 * 		Releases the si if imd is null or bin type is mis matched.
 *
 */
as_sindex *
as_sindex_from_range(as_namespace *ns, char *set, as_sindex_range *srange)
{
	GTRACE(CALLSTACK, debug, "as_sindex_from_range");
	if (ns->single_bin) return NULL;
	as_sindex *si = as_sindex__lookup(ns, NULL, srange->start.id, set,
						AS_SINDEX_LOOKUP_FLAG_ISACTIVE
						| AS_SINDEX_LOOKUP_FLAG_SETCHECK);
	if (si && si->imd) {
		// Do the type check
		as_sindex_metadata *imd = si->imd;
		for (int i = 0; i < imd->num_bins; i++) {
			if ((imd->binid[i] == srange->start.id)
					&& (srange->start.type !=
						as_sindex_pktype_from_sktype(imd->btype[i]))) {
				cf_warning(AS_SINDEX, "Query and Index Bin Type Mismatch: "
						"[binid %d : Index Bin type %d : "
						"Query Bin Type %d]",
						imd->binid[i],
						as_sindex_pktype_from_sktype(imd->btype[i]),
						srange->start.type );
				AS_SINDEX_RELEASE(si);
				return NULL;
			}
		}
	}
	return si;
}

/*
 * The way to filter out imd information from the as_msg which is primarily
 * query with all the details. For the normal operations the imd is formed out
 * of the as_op.
 */
/*
 * Returns -
 * 		NULL      - On failure.
 * 		as_sindex - On success.
 *
 * Description -
 * 		Firstly obtains the simatch using ns name and set name.
 * 		Then returns the corresponding slot from sindex array.
 *
 * TODO
 * 		log messages
 */
as_sindex *
as_sindex_from_msg(as_namespace *ns, as_msg *msgp)
{
	GTRACE(CALLSTACK, debug, "as_sindex_from_msg");
	as_msg_field *ifp  = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_INDEX_NAME);
	as_msg_field *sfp  = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_SET);

	if (!ifp) {
		GTRACE(QUERY, debug, "Index name not found in the query request");
		return NULL;
	}

	char *setname = NULL;
	char *iname   = NULL;

	if (sfp) {
		setname   = cf_strndup((const char *)sfp->data, as_msg_field_get_value_sz(sfp));
	}
	iname         = cf_strndup((const char *)ifp->data, as_msg_field_get_value_sz(ifp));
	
	as_sindex *si = as_sindex__lookup(ns, iname, -1, setname,
						AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if (!si) {
		cf_detail(AS_SINDEX, "Search did not find index ");
	}

	if (sfp)   cf_free(setname);
	if (iname) cf_free(iname);
	return si;
}


/*
 * Internal Function - as_sindex_range_free
 * 		frees the sindex range
 *
 * Returns
 * 		AS_SINDEX_OK - In every case
 */
int
as_sindex_range_free(as_sindex_range **range)
{
	GTRACE(CALLSTACK, debug, "as_sindex_range_free");
	as_sindex_range *sk = (*range);
	as_sindex_sbin_freeall(&sk->start, sk->num_binval);
	as_sindex_sbin_freeall(&sk->end, sk->num_binval);
	cf_free(sk);
	return AS_SINDEX_OK;
}

/*
 * Extract out range information from the as_msg and create the irange structure
 * if required allocates the memory.
 * NB: It is responsibility of caller to call the cleanup routine to clean the
 * range structure up and free up its memory
 *
 * query range field layout: contains - numranges, binname, start, end
 *
 * generic field header
 * 0   4 size = size of data only
 * 4   1 field_type = CL_MSG_FIELD_TYPE_INDEX_RANGE
 *
 * numranges
 * 5   1 numranges (max 255 ranges)
 *
 * binname
 * 6   1 binnamelen b
 * 7   b binname
 *
 * particle (start & end)
 * +b    1 particle_type
 * +b+1  4 start_particle_size x
 * +b+5  x start_particle_data
 * +b+5+x      4 end_particle_size y
 * +b+5+x+y+4   y end_particle_data
 *
 * repeat "numranges" times from "binname"
 */


/*
 * Function as_sindex_assert_query
 * Returns -
 * 		Return value of as_sindex__pre_op_assert
 */
int
as_sindex_assert_query(as_sindex *si, as_sindex_range *range)
{
	return as_sindex__pre_op_assert(si, AS_SINDEX_OP_READ);
}

/*
 * Function as_sindex_binlist_from_msg
 *
 * Returns -
 * 		binlist - On success
 * 		NULL    - On failure
 *
 */
cf_vector *
as_sindex_binlist_from_msg(as_namespace *ns, as_msg *msgp)
{
	GTRACE(CALLSTACK, debug, "as_sindex_binlist_from_msg");
	as_msg_field *bfp = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_QUERY_BINLIST);
	if (!bfp) {
		return NULL;
	}
	const uint8_t *data = bfp->data;
	int numbins         = *data++;
	cf_vector *binlist  = cf_vector_create(AS_ID_BIN_SZ, numbins, 0);

	for (int i = 0; i < numbins; i++) {
		int binnamesz = *data++;
		char binname[AS_ID_BIN_SZ];
		memcpy(&binname, data, binnamesz);
		binname[binnamesz] = 0;
		cf_vector_set(binlist, i, (void *)binname);
		data     += binnamesz;
	}

	GTRACE(QUERY, debug, "Queried Bin List %d ", numbins);
	for (int i = 0; i < cf_vector_size(binlist); i++) {
		char binname[AS_ID_BIN_SZ];
		cf_vector_get(binlist, i, (void*)&binname);
		GTRACE(QUERY, debug,  " String Queried is |%s| \n", binname);
	}

	return binlist;
}

/*
 * Returns -
 *		AS_SINDEX_OK        - On success.
 *		AS_SINDEX_ERR_PARAM - On failure.
 *		AS_SINDEX_ERR_OTHER - On failure.
 *
 * Description -
 *		Frames a sane as_sindex_range from msg.
 *
 *		We are not supporting multiranges right now. So numrange is always expected to be 1.
 */
int
as_sindex_range_from_msg(as_namespace *ns, as_msg *msgp, as_sindex_range *srange)
{
	GTRACE(CALLSTACK, debug, "as_sindex_range_from_msg");
	srange->num_binval = 0;
	// getting ranges
	as_msg_field *rfp = as_msg_field_get(msgp, AS_MSG_FIELD_TYPE_INDEX_RANGE);
	if (!rfp) {
		cf_warning(AS_SINDEX, "Required Index Range Not Found");
		return AS_SINDEX_ERR_PARAM;
	}
	const uint8_t *data = rfp->data;
	int numrange        = *data++;

	if (numrange != 1) {
		cf_warning(AS_SINDEX,
					"can't handle multiple ranges right now %d", rfp->data[0]);
		return AS_SINDEX_ERR_PARAM;
	}
	memset(srange, 0, sizeof(as_sindex_range));

	for (int i = 0; i < numrange; i++) {
		as_sindex_bin *start = &(srange->start);
		as_sindex_bin *end   = &(srange->end);
		// Populate Bin id
		uint8_t blen         = *data++;
		if (blen >= BIN_NAME_MAX_SZ) {
			cf_warning(AS_SINDEX, "Bin name size %d exceeds the max length %d", blen, BIN_NAME_MAX_SZ);
			return AS_SINDEX_ERR_PARAM;
		}
		char binname[BIN_NAME_MAX_SZ];
		memset(binname, 0, BIN_NAME_MAX_SZ);
		strncpy(binname, (char *)data, blen);
		binname[blen] = '\0';
		int16_t id = as_bin_get_id(ns, binname);
		if (id != -1) {
			start->id   = id;
			end->id     = id;
		} else {
			return AS_SINDEX_ERR_BIN_NOTFOUND;
		}
		data       += blen;

		// Populate type
		int type    = *data++;
		start->type = type;
		end->type   = start->type;
		start->flag &= ~SINDEX_FLAG_BIN_DOFREE;
		end->flag   &= ~SINDEX_FLAG_BIN_DOFREE;
	
		if ((type == AS_PARTICLE_TYPE_INTEGER)) {
			// get start point
			uint32_t startl  = ntohl(*((uint32_t *)data));
			data            += sizeof(uint32_t);
			if (startl != 8) {
				cf_warning(AS_SINDEX,
					"Can only handle 8 byte numerics right now %ld", startl);
				goto Cleanup;
			}
			start->valsz  = startl;
			start->u.i64  = __cpu_to_be64(*((uint64_t *)data));
			data         += sizeof(uint64_t);

			// get end point
			uint32_t endl = ntohl(*((uint32_t *)data));
			data         += sizeof(uint32_t);
			if (endl != 8) {
				cf_warning(AS_SINDEX,
						"can only handle 8 byte numerics right now %ld", endl);
				goto Cleanup;
			}
			end->valsz  = endl;
			end->u.i64  = __cpu_to_be64(*((uint64_t *)data));
			data       += sizeof(uint64_t);
			if (start->u.i64 > end->u.i64) {
				cf_warning(AS_SINDEX,
                     "Invalid range from %ld to %ld", start->u.i64, end->u.i64);
				goto Cleanup;
			} else if (start->u.i64 == end->u.i64) {
				srange->isrange = FALSE;
			} else {
				srange->isrange = TRUE;
			}
			GTRACE(QUERY, debug, "Range is equal %d,%d",
								start->u.i64, end->u.i64);
		} else if (type == AS_PARTICLE_TYPE_STRING) {
			// get start point
			uint32_t startl    = ntohl(*((uint32_t *)data));
			data              += sizeof(uint32_t);
			start->valsz       = startl;
			start->u.str       = (char *)data;
			data              += startl;
			srange->isrange    = FALSE;

			if ((start->valsz <= 0) || (start->valsz >= AS_SINDEX_MAX_STRING_KSIZE)) {
				cf_warning(AS_SINDEX, "Out of bound query key size %ld", start->valsz);
				goto Cleanup;
			}
			// get end point
			uint32_t endl      = ntohl(*((uint32_t *)data));
			data              += sizeof(uint32_t);
			end->valsz         = endl;
			end->u.str         = (char *)data;
			if (strncmp(start->u.str, end->u.str, start->valsz)) {
				cf_warning(AS_SINDEX,
                           "Only Equality Query Supported in Strings %s-%s",
                           start->u.str, end->u.str);
				goto Cleanup;
			}
            cf_digest_compute(start->u.str, start->valsz, &(start->digest));
			GTRACE(QUERY, debug, "Range is equal %s ,%s",
                               start->u.str, end->u.str);
		} else {
			cf_warning(AS_SINDEX, "Only handle String and Numeric type");
			goto Cleanup;
		}
	}
	srange->num_binval = numrange;
	return AS_SINDEX_OK;

Cleanup:
	return AS_SINDEX_ERR;
}

/*
 * Function as_sindex_rangep_from_msg
 *
 * Arguments
 * 		ns     - the namespace on which srange has to be build
 * 		msgp   - the msgp from which sent
 * 		srange - it builds this srange
 *
 * Returns
 * 		AS_SINDEX_OK - On success
 * 		else the return value of as_sindex_range_from_msg
 *
 * Description
 * 		Allocating space for srange and then calling as_sindex_range_from_msg.
 */
int
as_sindex_rangep_from_msg(as_namespace *ns, as_msg *msgp, as_sindex_range **srange)
{
	GTRACE(CALLSTACK, debug, "as_sindex_rangep_from_msg");
	*srange         = cf_malloc(sizeof(as_sindex_range));
	if (!(*srange)) {
		cf_warning(AS_SINDEX,
                 "Could not Allocate memory for range key. Aborting Query ...");
		return AS_SINDEX_ERR_NO_MEMORY;
	}

	int ret = as_sindex_range_from_msg(ns, msgp, *srange);
	if (AS_SINDEX_OK != ret) {
		as_sindex_range_free(srange);
		*srange = NULL;
		return ret;
	}
	return AS_SINDEX_OK;
}

int
as_sindex_sbin_from_op(as_msg_op *op, as_sindex_bin *sbin, int binid)
{
	GTRACE(CALLSTACK, debug, "as_sindex_sbin_from_op");
	sbin->id    = binid;
	sbin->type  = op->particle_type;
	sbin->flag &= ~SINDEX_FLAG_BIN_DOFREE;
	sbin->flag &= ~SINDEX_FLAG_BIN_ISVALID;

	if (op->particle_type == AS_PARTICLE_TYPE_STRING) {
		sbin->u.str = (char *)as_msg_op_get_value_p(op);
		sbin->valsz = as_msg_op_get_value_sz(op);
		sbin->flag |= SINDEX_FLAG_BIN_ISVALID;
		cf_digest_compute(sbin->u.str, sbin->valsz, &sbin->digest);
	} else if (op->particle_type == AS_PARTICLE_TYPE_INTEGER) {
		sbin->u.i64 = __cpu_to_be64(
						*(uint64_t *)as_msg_op_get_value_p(op));
		sbin->valsz = sizeof(uint64_t);
		sbin->flag |= SINDEX_FLAG_BIN_ISVALID;
	} else {
		cf_warning(AS_SINDEX, "Invalid particle type in op");
		return AS_SINDEX_ERR_PARAM;
	}

	return AS_SINDEX_OK;
}


/*
 * Info received from bin should be actually cleaned up using free later
 *
 * NB: caller should have SINDEX_GRLOCK. Because this function gets called for
 *     every bin pushing it down to this level is very costly. Given that G*LOCK
 *     has not contention in absence DDL. It is ok to hold it for longer duration.
 *     Caller general acquire this lock to perform all the validation for a
 *     given set of bin to be changed under single lock-unlock pair.
 */
int
as_sindex_sbin_from_bin(as_namespace *ns, const char *set, as_bin *b, as_sindex_bin *sbin)
{
	GTRACE(CALLSTACK, debug, "as_sindex_sbin_from_bin");
	if (!b) {
		GTRACE(META, debug, " Null Bin Passed, No sbin created");
		return AS_SINDEX_ERR_BIN_NOTFOUND;
	}
	
	if (!ns) {
		cf_warning(AS_SINDEX, "NULL Namespace Passed");
		return AS_SINDEX_ERR_PARAM;
	}

	if (as_bin_inuse(b)) {
		if (!as_sindex__lookup_lockfree(ns, NULL, b->id, (char *)set,
								AS_SINDEX_LOOKUP_FLAG_ISACTIVE
								| AS_SINDEX_LOOKUP_FLAG_SETCHECK
								| AS_SINDEX_LOOKUP_FLAG_NORESERVE)) {
			cf_detail(AS_SINDEX, "No Index Found on [%s:%s:%s]", ns->name, set, as_bin_get_name_from_id(ns, b->id));
			return AS_SINDEX_ERR_NOTFOUND;
		} else {
			cf_detail(AS_SINDEX, "Index Found on [%s:%s:%s]", ns->name, set, as_bin_get_name_from_id(ns, b->id));
		}

		sbin->id    = b->id;
		sbin->type  = as_bin_get_particle_type(b);
		sbin->flag &= ~SINDEX_FLAG_BIN_ISVALID;
		as_particle_tobuf(b, 0, &sbin->valsz);

		// when copying from record the new copy is made. because
		// inserts happens after the tree has done the stuff
		switch(as_bin_get_particle_type(b)) {
			case AS_PARTICLE_TYPE_INTEGER:  {
				sbin->flag &= ~SINDEX_FLAG_BIN_DOFREE;
				as_particle_tobuf(b, (byte *)&sbin->u.i64, &sbin->valsz);
				uint64_t val    = __cpu_to_be64(sbin->u.i64);
				sbin->u.i64     = val;
				sbin->flag     |= SINDEX_FLAG_BIN_ISVALID;
				return AS_SINDEX_OK;
			}
			case AS_PARTICLE_TYPE_STRING: {
				if (sbin->valsz < SINDEX_STRONSTACK_VALSZ) {
					sbin->u.str       = sbin->stackstr;
					sbin->stackstr[0] = '\0';
				}
				else  {
					sbin->u.str   = cf_malloc(sbin->valsz + 1);
					sbin->flag   |= SINDEX_FLAG_BIN_DOFREE;
				}
				as_particle_tobuf(b, (byte *)sbin->u.str, &sbin->valsz);
				sbin->u.str[sbin->valsz] = '\0';
				sbin->flag |= SINDEX_FLAG_BIN_ISVALID;
				cf_digest_compute(sbin->u.str, sbin->valsz, &sbin->digest);
				return AS_SINDEX_OK;
			}
			// No index stuff for the non integer non string bins
			default: {
				return AS_SINDEX_ERR;
			}
		}
	} else {
		GTRACE(META, debug, " Bin Not In Use");
		return AS_SINDEX_ERR_PARAM;
	}
}


/*
 * return 0 in case of SUCCESS
 * TODO Handle partial failure case in making sbins.
 */
int
as_sindex_sbin_from_rd(as_storage_rd *rd, uint16_t from_bin, uint16_t to_bin, as_sindex_bin delbin[], uint16_t * del_success)
{
	int ret = 0;
	uint16_t count = 0;

	for (uint16_t i = from_bin; i < to_bin; i++) {
		as_bin *b = &rd->bins[i];
		if( as_sindex_sbin_from_bin(rd->ns, as_index_get_set_name(rd->r, rd->ns), b, &delbin[count] ) != AS_SINDEX_OK) {
			GTRACE(CALLER, debug, "Failed to get sbin ");
		} else {
			count++;
		}
	}
	*del_success = count;
	return ret;
}

int
as_sindex_sbin_free(as_sindex_bin *sbin)
{
	if (sbin->flag & SINDEX_FLAG_BIN_ISVALID) {
		sbin->flag &= ~SINDEX_FLAG_BIN_ISVALID;
		if (sbin->flag & SINDEX_FLAG_BIN_DOFREE) {
			cf_free(sbin->u.str);
			sbin->flag &= ~SINDEX_FLAG_BIN_DOFREE;
		}
	}
	return AS_SINDEX_OK;
}

int
as_sindex_sbin_freeall(as_sindex_bin *sbin, int numbins)
{
	for (int i = 0; i < numbins; i++)  as_sindex_sbin_free(&sbin[i]);
	return AS_SINDEX_OK;
}

/*
 * Function to decide if current node has partition of the passed int digest
 * valid for returning result. Used by secondary index scan to filter out digest
 */
bool
as_sindex_partition_isactive(as_namespace *ns, cf_digest *digest)
{
	as_partition *p = NULL;
	cf_assert(ns, AS_SINDEX, CF_CRITICAL, "invalid namespace");
	int pid = as_partition_getid(*(digest));
	p = &ns->partitions[pid];

	if (0 != pthread_mutex_lock(&p->lock))
		cf_crash(AS_SINDEX, "couldn't acquire partition state lock: %s", cf_strerror(errno));

	bool is_active = (p->qnode == g_config.self_node);

	if (0 != pthread_mutex_unlock(&p->lock))
		cf_crash(AS_SINDEX, "couldn't release partition state lock: %s", cf_strerror(errno));

	return is_active;
}


/* This find out of the record can be defragged 
 * AS_SINDEX_GC_ERROR if found and cannot defrag
 * AS_SINDEX_GC_OK if can defrag
 * AS_SINDEX_GC_SKIP_ITERATION if skip current gc iteration. (partition lock timed out)
 */  
as_sindex_gc_status
as_sindex_can_defrag_record(as_namespace *ns, cf_digest *keyd)
{
	as_partition_reservation rsv;
	as_partition_id pid = as_partition_getid(*keyd);
	
	int timeout = 2; // 2 ms
	if (as_partition_reserve_migrate_timeout(ns, pid, &rsv, 0, timeout) != 0 ) {
		cf_atomic_int_add(&g_config.sindex_gc_timedout, 1);
		return AS_SINDEX_GC_SKIP_ITERATION;
	}

	int rv = AS_SINDEX_GC_ERROR;
	if (as_record_exists(rsv.tree, keyd, rsv.ns) != 0) {
		rv = AS_SINDEX_GC_OK;
	}
	as_partition_release(&rsv);
	return rv;

}

/*
 * Function as_sindex_isactive
 *
 * Returns sindex state
 */
inline bool as_sindex_isactive(as_sindex *si)
{
	if ((!si) || (!si->imd)) return FALSE;
	bool ret;
	if (si->state == AS_SINDEX_ACTIVE) {
		ret = TRUE;
	} else {
		ret = FALSE;
	}
	return ret;
}

int
as_sindex_histogram_dumpall(as_namespace *ns)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;
	SINDEX_GRLOCK();

	for (int i = 0; i < ns->sindex_cnt; i++) {
		if (ns->sindex[i].state != AS_SINDEX_ACTIVE) continue;
		if (!ns->sindex[i].enable_histogram)         continue;
		as_sindex *si = &ns->sindex[i];
		if (si->stats._write_hist)
			histogram_dump(si->stats._write_hist);
		if (si->stats._delete_hist)
			histogram_dump(si->stats._delete_hist);
		if (si->stats._query_hist)
			histogram_dump(si->stats._query_hist);
		if (si->stats._query_ai_hist)
			histogram_dump(si->stats._query_ai_hist);
		if (si->stats._query_cl_hist)
			histogram_dump(si->stats._query_cl_hist);
		if (si->stats._query_rcnt_hist)
			histogram_dump(si->stats._query_rcnt_hist);
		if (si->stats._query_diff_hist)
			histogram_dump(si->stats._query_diff_hist);
	}
	SINDEX_GUNLOCK();
	return AS_SINDEX_OK;
}

int
as_sindex_histogram_enable(as_namespace *ns, as_sindex_metadata *imd, bool enable)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;

	as_sindex *si = as_sindex__lookup(ns, imd->iname, -1, NULL, 0);
	if (!si) {
		return AS_SINDEX_ERR_NOTFOUND;
	}

	si->enable_histogram = enable;
	AS_SINDEX_RELEASE(si);
	return AS_SINDEX_OK;
}

/*
 * Function to check to make sure if two bins match.
 * Returns true if it matches
 */
bool
as_sindex_sbin_match(as_sindex_bin *b1, as_sindex_bin *b2)
{
	if (!b1 || !b2)                                         return false;
	if (b1->id    != b2->id)                                return false;
	if (b1->type  != b2->type)                              return false;
	if (b1->valsz != b1->valsz)                             return false;

	if ((b1->type == AS_PARTICLE_TYPE_INTEGER)
			&& (b1->u.i64 != b2->u.i64))                    return false;
	if ((b1->type == AS_PARTICLE_TYPE_STRING)
			&& (strncmp(b1->u.str, b2->u.str, b1->valsz)))  return false;

	return true;
}

/*
 * ACCOUNTING ACCOUNTING ACCOUNTING
 *
 * Internal function API for tracking sindex memory usage. This get called
 * from inside Aerospike Index.
 *
 * TODO: Make accounting subsystem cache friendly. At high speed it
 *       cache misses it causes starts to matter
 *
 * Reserve locally first then globally
 */
bool
as_sindex_reserve_data_memory(as_sindex_metadata *imd, uint64_t bytes)
{
	if (!bytes)                           return true;
	if (!imd || !imd->si || !imd->si->ns) return false;
	as_namespace *ns = imd->si->ns;
	bool g_reserved  = false;
	bool ns_reserved = false;
	bool si_reserved = false;
	uint64_t val     = 0;

	// Global reservation
	val = cf_atomic_int_add(&g_config.sindex_data_memory_used, bytes);
	g_reserved = true;
	if (val > g_config.sindex_data_max_memory) goto FAIL;
	
	// Namespace reservation
	val = cf_atomic_int_add(&ns->sindex_data_memory_used, bytes);
	ns_reserved = true;
	if (val > ns->sindex_data_max_memory)      goto FAIL;
	
	// Secondary Index Specific
	val = cf_atomic_int_add(&imd->si->data_memory_used, bytes);
	si_reserved = true;
	if (val > imd->si->config.data_max_memory) goto FAIL;
	
	return true;

FAIL:
	if (ns_reserved) cf_atomic_int_sub(&ns->sindex_data_memory_used, bytes);
	if (g_reserved)  cf_atomic_int_sub(&g_config.sindex_data_memory_used, bytes);
	if (si_reserved)  cf_atomic_int_sub(&imd->si->data_memory_used, bytes);
	cf_warning(AS_SINDEX, "Data Memory Cap Hit for Secondary Index %s "
							"while reserving %ld bytes", imd->iname, bytes);
	return false;
}

// release locally first then globally
bool
as_sindex_release_data_memory(as_sindex_metadata *imd, uint64_t bytes)
{
	as_namespace *ns = imd->si->ns;
	if ((ns->sindex_data_memory_used < bytes)
		|| (imd->si->data_memory_used < bytes)
		|| (g_config.sindex_data_memory_used < bytes)) {
		cf_warning(AS_SINDEX, "Sindex memory usage accounting corrupted");
	}
	cf_atomic_int_sub(&ns->sindex_data_memory_used, bytes);
	cf_atomic_int_sub(&g_config.sindex_data_memory_used, bytes);
	cf_atomic_int_sub(&imd->si->data_memory_used, bytes);
	return true;
}

uint64_t
as_sindex_get_ns_memory_used(as_namespace *ns)
{
	if (as_sindex_ns_has_sindex(ns)) {
		return ns->sindex_data_memory_used;
	}
	return 0;
}

extern int as_info_parameter_get(char *param_str, char *param, char *value, int  *value_len);

/*
 * Client API function to set configuration parameters for secondary indexes
 */
int
as_sindex_set_config(as_namespace *ns, as_sindex_metadata *imd, char *params)
{
	if (!ns)
		return AS_SINDEX_ERR_PARAM;
	as_sindex *si = as_sindex__lookup(ns, imd->iname, -1, imd->set,
					AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if (!si) {
		return AS_SINDEX_ERR_NOTFOUND;
	}
	SINDEX_WLOCK(&si->imd->slock);
	if (si->state == AS_SINDEX_ACTIVE) {
		char context[100];
		int  context_len = sizeof(context);
		if (0 == as_info_parameter_get(params, "ignore-not-sync", context, &context_len)) {
			if (strncmp(context, "true", 4)==0 || strncmp(context, "yes", 3)==0) {
				cf_info(AS_INFO,"Changing value of ignore-not-sync of ns %s sindex %s to %s", ns->name, imd->iname, context);
				si->config.flag |= AS_SINDEX_CONFIG_IGNORE_ON_DESYNC;
			} else if (strncmp(context, "false", 5)==0 || strncmp(context, "no", 2)==0) {
				cf_info(AS_INFO,"Changing value of ignore-not-sync of ns %s sindex %s to %s", ns->name, imd->iname, context);
				si->config.flag &= ~AS_SINDEX_CONFIG_IGNORE_ON_DESYNC;
			} else {
				goto Error;
			}
		}
		else if (0 == as_info_parameter_get(params, "data-max-memory", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_detail(AS_INFO, "data-max-memory = %"PRIu64"",val);
			// Protect so someone does not reduce memory to below 1/2 current value, allow it
			// in case value is ULONG_MAX
			if (((si->config.data_max_memory != ULONG_MAX)
				&& (val < (si->config.data_max_memory / 2L)))
				|| (val < cf_atomic64_get(si->data_memory_used))) {
				goto Error;
			}
			cf_info(AS_INFO,"Changing value of data-max-memory of ns %s sindex %s from %"PRIu64"to %"PRIu64"",
							ns->name, imd->iname, si->config.data_max_memory, val);
			si->config.data_max_memory = val;
		}
		else if (0 == as_info_parameter_get(params, "gc-period", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_detail(AS_INFO, "gc-period = %"PRIu64"",val);
			if (val < 0) {
				goto Error;
			}
			cf_info(AS_INFO,"Changing value of gc-period of ns %s sindex %s from %"PRIu64"to %"PRIu64"",
							ns->name, imd->iname, si->config.defrag_period, val);
			si->config.defrag_period = val;
		}
		else if (0 == as_info_parameter_get(params, "gc-max-units", context, &context_len)) {
			uint64_t val = atoll(context);
			cf_detail(AS_INFO, "gc-limit = %"PRIu64"",val);
			if (val < 0) {
				goto Error;
			}
			cf_info(AS_INFO,"Changing value of gc-max-units of ns %s sindex %s from %"PRIu64"to %"PRIu64"",
							ns->name, imd->iname, si->config.defrag_max_units, val);
			si->config.defrag_max_units = val;
		}
		else {
			goto Error;
		}
	}
	SINDEX_UNLOCK(&si->imd->slock);
	AS_SINDEX_RELEASE(si);
	return AS_SINDEX_OK;

Error:
	SINDEX_UNLOCK(&si->imd->slock);
	AS_SINDEX_RELEASE(si);
	return AS_SINDEX_ERR_PARAM;
}




// System Metadata Integration : System Metadata Integration
// System Metadata Integration : System Metadata Integration

/*
 *                +------------------+
 *  client -->    |  Secondary Index |
 *                +------------------+
 *                     /|\
 *                      | 4 accept
 *                  +----------+   2
 *                  |          |<-------   +------------------+ 1 request
 *                  | SMD      | 3 merge   |  Secondary Index | <------------|
 *                  |          |<------->  |                  | 5 response   | CLIENT
 *                  |          | 4 accept  |                  | ------------>|
 *                  |          |-------->  +------------------+
 *                  +----------+
 *                     |   4 accept
 *                    \|/
 *                +------------------+
 *  client -->    |  Secondary Index |
 *                +------------------+
 *
 *
 *  System Metadta module sits in the middle of multiple secondary index
 *  module on multiple nodes. The changes which eventually are made to the
 *  secondary index are always triggerred from SMD. Here is the flow.
 *
 *  Step1: Client send (could possibly be secondary index thread) triggers
 *         create / delete / update related to secondary index metadata.
 *
 *  Step2: The request passed through secondary index module (may be few
 *         node specific info is added on the way) to the SMD.
 *
 *  Step3: SMD send out the request to the paxos master.
 *
 *  Step4: Paxos master request the relevant metadata info from all the
 *         nodes in the cluster once it has all the data... [SMD always
 *         stores copy of the data, it is stored when the first time
 *         create happens]..it call secondary index merge callback
 *         function. The function is responsible for resolving the winning
 *         version ...
 *
 *  Step5: Once winning version is decided for all the registered module
 *         the changes are sent to all the node.
 *
 *  Step6: At each node accept_fn is called for each module. Which triggers
 *         the call to the secondary index create/delete/update functions
 *         which would be used to in-memory operation and make it available
 *         for the system.
 *
 *  There are two types of operations which look at the secondary index
 *  operations.
 *
 *  a) Normal operation .. they all look a the in-memory structure and
 *     data which is in sindex and ai_btree layer.
 *
 *  b) Other part which do DDL operation like which work through the SMD
 *     layer. Multiple operation happening from the multiple nodes which
 *     come through this layer. The synchronization is responsible of
 *     SMD layer. The part sindex / ai_btree code is responsible is to
 *     make sure when the call from the SMD comes there is proper sync
 *     between this and operation in section a
 *
 *  Set of function implemented below are merge function and accept function.
 */

int
as_sindex_smd_create()
{
	// Init's the smd interface.
	//
	// as_smd_module_create("SINDEX", as_sindex_smd_merge_cb, NULL, as_sindex_smd_accept_cb, NULL);
	//
	// Expectation: This would read stuff up from the JSON file which SMD
	//              maintance and make sure the requested data is loaded
	//              and the accept_fn is called. All the real logic is
	//              inside accept_fn function.
	return 0;
}

int
as_sindex_smd_merge_cb(char *module, as_smd_item_list_t **item_list_out,
						as_smd_item_list_t **item_lists_in, size_t num_lists,
						void *udata)
{
	// Applies merge and decides who wins. This is algorithm which should
	// be independent of who is running is .. any node which becomes paxos
	// master can execute it and get the same result.
	return 0;
}

extern int as_info_parse_params_to_sindex_imd(char *, as_sindex_metadata *, cf_dyn_buf *, bool, bool*);
/*
 * Description     : Checks whether an index with the same defn already exists.
 * 				     Index defn ={index_name, bin_name, bintype, set_name, ns_name}
 * Parameters      : namespace, index metadata
 *
 * Returns         : AS_SINDEX_OK if index with the given defn already exists.
 * 			         AS_SINDEX_ERR otherwise
 *
 * Synchronization : No locks taken inside this function.
 */
int
as_sindex_check_index_defn(as_namespace* ns, as_sindex_metadata* imd)
{
	char* iname       = imd->iname;
	char* set         = imd->set;
	as_sindex* sindex = as_sindex__lookup(ns,iname,-1,set,AS_SINDEX_LOOKUP_FLAG_ISACTIVE);
	if(sindex == NULL){
		return AS_SINDEX_ERR;
	}
	int binid         = as_bin_get_id(ns, imd->bnames[0]);
	for (int i=0; i<AS_SINDEX_BINMAX; i++) {
		if(sindex->imd->bnames[i] && imd->bnames[i])
		if (binid == sindex->imd->binid[i] && !strcmp(imd->bnames[i],sindex->imd->bnames[i])
			&& imd->btype[i] == sindex->imd->btype[i])
			return AS_SINDEX_OK;
	}

	return AS_SINDEX_ERR;
}

/*
 * Description     : When a index has to be dropped and recreated during cluster state change
 * 				     this function is called.
 * Parameters      : imd, which is constructed from the final index defn given by paxos principal.
 * 
 * Returns         : 0 on all cases. Check log for errors.
 *
 * Synchronization : Does not explicitly take any locks
 */
int
as_sindex_update(as_sindex_metadata* imd)
{
	as_namespace *ns = as_namespace_get_byname(imd->ns_name);
	int ret          = as_sindex_create(ns, imd, true);
	if (ret != 0) {
		cf_warning(AS_SINDEX,"Index %s creation failed at the accept callback", imd->iname);
	}
	return 0;
}

/*
 * Description :
 *  	Checks the parameters passed to as_sindex_create function
 *
 * Parameters:
 * 		namespace, index metadata
 *
 * Returns:
 * 		AS_SINDEX_OK            - for valid parameters.
 * 		Appropriate error codes - otherwise
 *
 * Synchronization:
 * 		This function does not explicitly acquire any lock.
 */
int
as_sindex_create_check_params(as_namespace* ns, as_sindex_metadata* imd)
{
	SINDEX_GRLOCK();

	int ret     = AS_SINDEX_OK;
	int simatch = as_sindex__simatch_by_iname(ns, imd->iname);

	if (simatch != -1) {
		cf_info(AS_SINDEX,"Index %s already exists", imd->iname);
		ret = AS_SINDEX_ERR_FOUND;
	} else {
		int16_t binid = as_bin_get_id(ns, imd->bnames[0]);
		if (binid != -1)
		{
			int simatch = as_sindex__simatch_by_set_binid(ns, imd->set, binid);
			if (simatch != -1) {
				cf_info(AS_SINDEX," The bin %s is already indexed @ %d",imd->bnames[0], simatch);
				ret = AS_SINDEX_ERR_FOUND;
				goto END;
			}
		}

		for (int i = 0; i < AS_SINDEX_BINMAX; i++) {
			if (imd->bnames[i] && ( strlen(imd->bnames[i]) > ( AS_ID_INAME_SZ - 1 ) )) {
				cf_info(AS_SINDEX, "Index Name %s too long ", imd->bnames[i]);
				ret = AS_SINDEX_ERR_PARAM;
				goto END;
			}
		}
	}

END:
	SINDEX_GUNLOCK();
    return ret;
}

/*
 * Description: This cb function is called by paxos master, before doing the
 *              In the case of AS_SMD_SET_ACTION
 *              1) existence of index with the given index name.
 *              2) Whether the bin already has a index
 *              In case of AS_SMD_DELETE_ACTION
 *              1) the existence of index with the given name
 * Parameters:
 * 			   module -- module name (SINDEX_MODULE)
 * 			   item   -- action item that paxos master has received
 * 			   udata  -- user data for the callback
 *
 * Returns:
 * 	In case of AS_SMD_SET_ACTION:
 * 		AS_SIDNEX_ERR_INDEX_FOUND  - if index with the given name exists or bin
 * 									  already has an index
 * 		AS_SINDEX_OK			    - Otherwise
 * 	In case of AS_SMD_DELETE_ACTION
 * 		AS_SINDEX_ERR_NOTFOUND      - Index does not exist with the given index name
 * 		AS_SINDEX_OK				- Otherwise
 *
 * 	Synchronization
 * 		This function takes SINDEX GLOBAL WRITE LOCK and releasese it for checking deletion
 * 		operation.
 */
int
as_sindex_smd_can_accept_cb(char *module, as_smd_item_t *item, void *udata)
{
	as_sindex_metadata imd;
	memset((void *)&imd, 0, sizeof(imd));

	char         * params = NULL;
	as_namespace * ns     = NULL;
	int retval            = AS_SINDEX_ERR;

		switch (item->action) {
			case AS_SMD_ACTION_SET:
				{
					params = item->value;
					bool smd_op = false;
					if (as_info_parse_params_to_sindex_imd(params, &imd, NULL, true, &smd_op)){
						goto ERROR;
					}
					ns     = as_namespace_get_byname(imd.ns_name);
					retval = as_sindex_create_check_params(ns, &imd);

					if(retval != AS_SINDEX_OK){
						cf_info(AS_SINDEX, "Callback from paxos master for validation failed with error code %d", retval);
						goto ERROR;
					}
					break;
				}
			case AS_SMD_ACTION_DELETE:
				{
					char ns_name[100], ix_name[100];
					ns_name[0] = ix_name[0] = '\0';

					if (2 != sscanf(item->key, "%[^:]:%s", (char *) &ns_name, (char *) &ix_name)) {
						cf_warning(AS_SINDEX, "failed to extract namespace name and index name from SMD delete item value");
						retval = AS_SINDEX_ERR;
						goto ERROR;
					} else {
						imd.ns_name = cf_strdup(ns_name);
						imd.iname   = cf_strdup(ix_name);
						ns          = as_namespace_get_byname(imd.ns_name);
						if (as_sindex__lookup(ns, imd.iname, -1, imd.set,
										AS_SINDEX_LOOKUP_FLAG_NORESERVE
										| AS_SINDEX_LOOKUP_FLAG_ISACTIVE)) {
							retval = AS_SINDEX_OK;
						} else {
							retval = AS_SINDEX_ERR_NOTFOUND;
						}
					}
					break;
				}
	}				
	
ERROR:
	as_sindex_imd_free(&imd);
	return retval;
}

int
as_sindex_cfg_var_hash_reduce_fn(void *key, void *data, void *udata)
{
	// Parse through the entire si_cfg_array, do an shash_delete on all the valid entries
	// How do we know if its a valid-entry ? valid-entries get marked by the valid_flag in
	// the process of doing as_sindex_create() called by smd.
	// display a warning for those that are not valid and finally, free the entire structure

	as_sindex_config_var *si_cfg_var = (as_sindex_config_var *)data;

	if (! si_cfg_var->conf_valid_flag) {
		cf_warning(AS_SINDEX, "No secondary index %s found. Configuration stanza for %s ignored.", si_cfg_var->name, si_cfg_var->name);
	}

	return 0;
}

/*
 * This function is called when the SMD has resolved the correct state of
 * metadata. This function needs to, based on the value, looks at the current
 * state of the index and trigger requests to secondary index to do the
 * needful. At the start of time there is nothing in sindex and this code
 * comes and setup indexes
 *
 * Expectation. SMD is responsible for persisting data and communicating back
 *              to sindex layer to create in-memory structures
 *
 *
 * Description: To perform sindex operations(ADD,MODIFY,DELETE), through SMD
 * 				This function called on every node, after paxos master decides
 * 				the final version of the sindex to be created. This is the final
 *				version and the only allowed version in the sindex.Operations coming
 *				to this function are least expected to fail, ideally they should
 *				never fail.
 *
 * Parameters:
 * 		module:             SINDEX_MODULE
 * 		as_smd_item_list_t: list of action items, to be performed on sindex.
 * 		udata:              ??
 *
 * Returns:
 * 		always 0
 *
 * Synchronization:
 * 		underlying secondary index all needs to take corresponding lock and
 * 		SMD is today single threaded no sync needed there
 */
int
as_sindex_smd_accept_cb(char *module, as_smd_item_list_t *items, void *udata, uint32_t bitmap)
{
	as_sindex_metadata imd;
	memset((void *)&imd, 0, sizeof(imd));
	char         * params = NULL;
	as_namespace * ns     = NULL;
	imd.post_op = 0;
	
	for (int i = 0; i < items->num_items; i++) {
		params = items->item[i]->value;
		switch (items->item[i]->action) {
			// TODO: Better handling of failure of the action items list
			case AS_SMD_ACTION_SET:
			{
				bool smd_op = false;
				if (as_info_parse_params_to_sindex_imd(params, &imd, NULL, true, &smd_op)) {
					cf_info(AS_SINDEX,"Parsing the index metadata for index creation failed");
					break;
				}

				ns         = as_namespace_get_byname(imd.ns_name);
				if (as_sindex_exists_by_defn(ns, &imd)) {
					cf_detail(AS_SINDEX, "Index with the same index defn already exists.");
					// Fail quietly for duplicate sindex requests
					continue;
				}
				// Pessimistic --Checking again. This check was already done by the paxos master.
				int retval = as_sindex_create_check_params(ns, &imd);
				if (retval == AS_SINDEX_ERR_FOUND) {
						// Two possible cases for reaching here
						// 1. It is possible that secondary index is not active hence defn check
						//    fails but params check pick up sindex in destroy state as well.
						// 2. SMD thread is single threaded ... not sure how can above definition
						//    check fail but params check pass. But just in case it does bail out
						//    destroy and recreate (Accept the final version). think !!!!
						cf_detail(AS_SINDEX, "IndexName Already exists. Dropping the index due to cluster state change");
						imd.post_op = 1;
						as_sindex_destroy(ns, &imd);
				}
				else {
					retval = as_sindex_create(ns, &imd, true);
				}
				break;
			}
			case AS_SMD_ACTION_DELETE:
			{
				char ns_name[100], ix_name[100];
				ns_name[0] = ix_name[0] = '\0';

				if (2 != sscanf(items->item[i]->key, "%[^:]:%s", (char *) &ns_name, (char *) &ix_name)) {
					cf_warning(AS_SINDEX, "failed to extract namespace name and index name from SMD delete item value");
				} else {
					imd.ns_name = cf_strdup(ns_name);
					imd.iname = cf_strdup(ix_name);
					ns = as_namespace_get_byname(imd.ns_name);
					as_sindex_destroy(ns, &imd);
				}
				break;
			}
		}
	}



	// Check if the incoming operation is merge. If it's merge
	// After merge resolution of cluster, drop the local sindex definitions which are not part
	// of the paxos principal's sindex definition.
	if( bitmap & AS_SMD_INFO_MERGE ) {
		for (int k = 0; k < g_config.namespaces; k++) { // for each namespace
			as_namespace *local_ns = g_config.namespace[k];

			if (local_ns->sindex_cnt > 0) {
				as_sindex *del_list[AS_SINDEX_MAX];
				int        del_cnt = 0;
				SINDEX_GRLOCK();
				
				// Create List of Index to be Deleted
				for (int i = 0; i < AS_SINDEX_MAX; i++) {
					as_sindex *si = &local_ns->sindex[i];
					if (si && si->imd) {
						int found     = 0;
						SINDEX_RLOCK(&si->imd->slock);
						for (int j = 0; j < items->num_items; j++) {
							char key[256];
							sprintf(key, "%s:%s", si->imd->ns_name, si->imd->iname);
							cf_detail(AS_SINDEX,"Item key %s \n", items->item[j]->key);

							if (strcmp(key, items->item[j]->key) == 0) {
								found = 1;
								cf_detail(AS_SINDEX, "Item found in merge list %s \n", si->imd->iname);
								break;
							}
						}
						SINDEX_UNLOCK(&si->imd->slock);

						if (found == 0) { // Was not found in the merged list from paxos principal
							AS_SINDEX_RESERVE(si);
							del_list[del_cnt] = si;
							del_cnt++;
						}
					}
				}

				SINDEX_GUNLOCK();

				// Delete Index
				for (int i = 0 ; i < del_cnt; i++) {
					if (del_list[i]) {
						as_sindex_destroy(local_ns, del_list[i]->imd);
						AS_SINDEX_RELEASE(del_list[i]);
						del_list[i] = NULL;
					}
				}
			}
		}
	}

	as_sindex_imd_free(&imd);

	return(0);
}
