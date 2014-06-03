/*
 * thr_sindex.c
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
 * This file implements supporting threads for the secondary index implementation.
 * Currently following two main threads are implemented here
 *
 * -  Secondary index defrag thread which walks sweeps through secondary indexes
 *   and cleanup the stale entries by looking up digest in the primary index.
 *
 * -  Secondary index thread which cleans up secondary index entry for a particular
 *    partitions
 *
 */

#include <errno.h>

#include "base/secondary_index.h"
#include "base/thr_scan.h"

#include "ai_obj.h"
#include "ai_btree.h"

#define RELEASE_ITERATORS(icol) \
do {                                \
	init_ai_obj(&i_col);             \
	n_offset = 0;                   \
} while(0);

// All this is global because Aerospike Index is single threaded
pthread_rwlock_t g_sindex_rwlock = PTHREAD_RWLOCK_INITIALIZER;
pthread_rwlock_t g_ai_rwlock = PTHREAD_RWLOCK_INITIALIZER;
pthread_t g_sindex_populate_th;
pthread_t g_sindex_destroy_th;
pthread_t g_sindex_defrag_th;
cf_queue *g_sindex_populate_q;
cf_queue *g_sindex_destroy_q;
cf_queue *g_sindex_populateall_done_q;
bool      g_sindex_boot_done;

// Main thread which looks at the request of the populating index
void *
as_sindex__populate_fn(void *param)
{
	while(1) {
		as_sindex *si;
		cf_queue_pop(g_sindex_populate_q, &si, CF_QUEUE_FOREVER);
		if (si->flag & AS_SINDEX_FLAG_POPULATING) {
			// Earlier scan to populate index is still going on, push it back
			// into the queue to look at it later. this is problem only when
			// there are multiple populating threads currently there is only 1.
			cf_queue_push(g_sindex_populate_q, &si);
		} else {
			cf_debug(AS_SINDEX, "Populating index %s", si->imd->iname);
			si->flag |= AS_SINDEX_FLAG_POPULATING;
			as_tscan_sindex_populate(si);
		}
	}
	return NULL;
}


// Main thread which looks at the request of the destroy of index
void *
as_sindex__destroy_fn(void *param)
{
	while(1) {
		as_sindex *si;
		cf_queue_pop(g_sindex_destroy_q, &si, CF_QUEUE_FOREVER);

		SINDEX_GWLOCK();
		cf_assert((si->state == AS_SINDEX_DESTROY),
				AS_SINDEX, CF_CRITICAL, " Invalid state %d at cleanup expected %d for %p and %s", si->state, AS_SINDEX_DESTROY, si, (si) ? ((si->imd) ? si->imd->iname : NULL) : NULL);
		SINDEX_WLOCK(&si->imd->slock);
		ai_btree_destroy(si->imd);
		// Free entire usage counter for this index after the destroy
		// code... alc code does not do it.
		as_sindex_release_data_memory(si->imd, si->data_memory_used);
		as_sindex_destroy_pmetadata(si);
		si->state = AS_SINDEX_INACTIVE;
		si->flag  = 0;
		si->ns->sindex_cnt--;
		as_sindex_metadata *imd = si->imd;
		si->imd = NULL;

		char iname[AS_ID_INAME_SZ];
		memset(iname, 0, AS_ID_INAME_SZ);
		snprintf(iname, strlen(imd->iname) + 1, "%s", imd->iname);
		shash_delete(si->ns->sindex_iname_hash, (void *)iname);

		// add 14 for number of characters in any uint32
		char set_binid[AS_SET_NAME_MAX_SIZE + 14];
		memset(set_binid, 0, AS_SET_NAME_MAX_SIZE + 14);
		if (imd->set == NULL) {
			sprintf(set_binid, "_%d", imd->binid[0]);
		}
		else {
			sprintf(set_binid, "%s_%d", imd->set, imd->binid[0]);
		}

		shash_delete(si->ns->sindex_set_binid_hash, (void *)set_binid);
		si->ns      = NULL;
		si->simatch = -1;

		// remember this is going to release the write lock
		// of meta-data first. This is the only special case
		// where both GLOCK and LOCK is called together
		SINDEX_UNLOCK(&imd->slock);
		SINDEX_GUNLOCK();

		if (si->new_imd) {
			as_sindex_metadata *recreate_imd = si->new_imd;
			as_sindex_update(recreate_imd);
			si->new_imd = NULL;
		}

		as_sindex_imd_free(imd);
		cf_rc_free(imd);
	}
	return NULL;
}

void
as_sindex_update_defrag_stat(as_sindex *si, uint32_t r, uint64_t starttime)
{
	cf_atomic64_add(&si->stats.n_deletes,        r);
	cf_atomic64_add(&si->stats.n_objects,        -r);
	cf_atomic64_add(&si->stats.n_defrag_records, r);
	cf_atomic64_add(&si->stats.defrag_time, cf_getus() - starttime);
}

/*
 * Core of sindex defragic logic.
 * Determines which pimd needs to be defragged
 * Reserves, build si and build pimd
 * Returns :
 * 		 0  - Success
 * 		-1  - go to next si
 * 		-2  - go to next ns
 * Notes-
 * 		Caller needs to release the ref count of sindex(si)
*/
int
as_sindex_get_pimd_to_defrag(as_namespace *ns, int *si_index, int *p_index, as_sindex_pmetadata** pimd,
		as_sindex ** sindex, int *si_defraged)
{
	if (*p_index >= NUM_SINDEX_PARTITIONS) {
		// pimd reaches max limit. Switch to next si.
		*p_index = 0;
		(*si_index)++;
		(*si_defraged)++;
	}

	// This check may result in skipping of some indexes.
	// i.e When a sindex is created/dropped while the defrag is running on other sindexes of same namespace.
	// They will be covered in next iteration. Overall its a performance gain.
	if (*si_index >= AS_SINDEX_MAX || *si_defraged >= ns->sindex_cnt) {
		return -2;
	}

	SINDEX_GRLOCK();
	as_sindex * si = &ns->sindex[*si_index];

	if (!si) {
		SINDEX_GUNLOCK();
		cf_warning(AS_SINDEX, "Allocated sindex was found as null.");
		return -2;
	}
	if (si->state != AS_SINDEX_ACTIVE) {
		// Skip to next sindex in the same namespace
		SINDEX_GUNLOCK();
		*si_index = *si_index + 1;
		*p_index = 0;
		return -1;
	}

	AS_SINDEX_RESERVE(si);
	SINDEX_GUNLOCK();
	as_sindex_metadata *  imd = si->imd;
	*pimd = &imd->pimd[*p_index];
	*sindex = si;
	cf_detail(AS_SINDEX, "Defragging pimd %d of sindex %s on namespace %s and set %s",
			*p_index, si->imd->iname, si->imd->ns_name, si->imd->set);
	return 0;
}

/*
 * This thread/function continually runs over an secondary index to clean
 * up the unnecessary/expired digests.
 * If the data is on disk when record is deleted to avoid reading from the disk,
 * the delete from the secondary index is not done inline.
 *
 * Note: If the record comes back after being deleted on an on-disk namespace, there is a probability
 * that there was not enough time to defrag that. TODO -- FIX IT
 *
 * Flow :
 * 				GET PIMD ----> BUILD DEFRAG_LIST
 *				   /|\              |
 * 					|				|
 *					|			   \|/
 *				  DEFRAG THE DEFRAG_LIST
 *
 * Controlling parameters   : Modifiable through clinfo
 * 1. defrag_max_units      - Max units it will defrag in one iteration(i.e before sleeping) default -- 1000 units
 * 2. defrag_period(ms)     - Minimum time delay between two iteration of sindex defrag.     default -- 1    msec
 *
 * Takes a lock on pimd while defragging
 * TODO : Aerospike Index layer is probably doing a lot of mallocs and copy.
 *          It can be avoided.
 */
void *
as_sindex__defrag_fn(void *udata)
{
	GTRACE(CALLSTACK, debug, "Secondary index defrag thread started !!");
	while (!g_sindex_boot_done) {
		sleep(10);
		continue;
	}

	uint16_t ns_id = 0;
	while (true) {
		as_namespace *ns = g_config.namespace[ns_id];
		if (!ns || (ns->sindex_cnt == 0)) {
			goto next_ns;
		}

		uint64_t      last_time        = cf_get_seconds();
		int           si_index         = 0;
		int           p_index          = 0;
		ai_obj        i_col;                                      // Numeric type sindexes iterator
		init_ai_obj(&i_col);
		long          n_offset         = 0;
		int           sindex_defraged  = 0;
		long          defrag_period    = 0;
		long          limit            = 0;
		// loop through all sindexes and pimds to defrag
		while (1) {
			// Sleep for remainder of defrag period
			uint64_t        curr_time = cf_getms();
			if ((curr_time - last_time) < defrag_period) {
				usleep(1000 * (curr_time - last_time));
				continue;
			}
			last_time = curr_time;

			// Get pimd to defrag..
			as_sindex           * si;
			as_sindex_pmetadata * pimd;
			int retval     = as_sindex_get_pimd_to_defrag(ns, &si_index, &p_index, &pimd, &si, &sindex_defraged);
			if (retval != 0) {
				if (retval == -1) {
					// To avoid cases in which a sindex is dropped in middle of defragging
					RELEASE_ITERATORS(icol)
					continue;
				}
				if (retval == -2) {
					break;
				}
			}

			if (!pimd || !si) {
				break;
			}
			limit          = (long)si->config.defrag_max_units;
			defrag_period  = (long)si->config.defrag_period;

			// This can be use to control the defrag thread.
			// Setting defrag_max_units as 0 can allow a user
			// to stop defragging of a sindex
			if( limit <= 0 ) {
				si_index++;
				sindex_defraged++;
				p_index = 0;
				RELEASE_ITERATORS(icol)
				AS_SINDEX_RELEASE(si);
				continue;
			}

			// Create Defrag List
			cf_ll defrag_list;
			cf_ll_init(&defrag_list, &ll_ai_obj_dig_destroy_fn, false);
			defrag_list.sz      = 0;
			defrag_list.uselock = false;

			SINDEX_RLOCK(&pimd->slock)
			int  ret            = ai_btree_build_defrag_list(si->imd, pimd, &i_col, &n_offset, limit, &defrag_list);
			SINDEX_UNLOCK(&pimd->slock);
			int listsize        = defrag_list.sz;

			// Run Defrag..
			if ( (ret != AS_SINDEX_ERR ) && (listsize > 0) ) {
				ulong    wl_lim    = 10;
				uint64_t starttime = cf_getus();
				bool     more      = 1;
				ulong    deleted   = 0;
				while (more) {
					SINDEX_WLOCK(&pimd->slock);
					more = ai_btree_defrag_list(si->imd, pimd, &defrag_list, wl_lim, &deleted);
					SINDEX_UNLOCK(&pimd->slock);
				}
				cf_detail(AS_SINDEX, "Deleted %d units of attempted %d units from index %s", listsize, limit, si->imd->iname);
				as_sindex_update_defrag_stat(si, listsize, starttime);
			}

			// Release list
			cf_ll_reduce(&defrag_list, true /*forward*/, ll_ai_obj_dig_reduce_fn, NULL);
			if ((ret == AS_SINDEX_DONE) || (ret == AS_SINDEX_ERR)) {
				RELEASE_ITERATORS(icol)
				p_index++;
			}
			AS_SINDEX_RELEASE(si);
		}
next_ns:
		usleep(1);
		ns_id = (ns_id + 1) % g_config.namespaces;
	}
	return(0);
}


/*
 * Secondary index main defrag thread, it keeps watching out for request to
 * the defrag, Client API to set up aerospike facing meta data for the secondary index
 * and setting all the initial things
 *
 * Parameter:
 *		 sindex_metadata:  (in/out) Index meta-data structure
 *
 * Caller:
 *		aerospike
 * Return:
 *		0: On success
 *		-1: On failure
 * Synchronization:
 * 		Acquires the meta lock.
 */
void
as_sindex_thr_init()
{
	// Thread request read lock on this recursively could possibly cause deadlock. Caller
	// should be careful with that
	pthread_rwlockattr_t rwattr;

	if (0 != pthread_rwlockattr_init(&rwattr))
		cf_crash(AS_SINDEX, "pthread_rwlockattr_init: %s", cf_strerror(errno));
	if (0 != pthread_rwlockattr_setkind_np(&rwattr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP))
		cf_crash( AS_SINDEX, "pthread_rwlockattr_setkind_np: %s", cf_strerror(errno));

	// Aerospike Index Metadata lock
	if (0 != pthread_rwlock_init(&g_ai_rwlock, &rwattr)) {
		cf_crash(AS_SINDEX, " Could not create secondary index ddl mutex ");
	}

	// Sindex Metadata lock
	if (0 != pthread_rwlock_init(&g_sindex_rwlock, &rwattr)) {
		cf_crash(AS_SINDEX, " Could not create secondary index ddl mutex ");
	}

	g_sindex_populate_q = cf_queue_create(sizeof(as_sindex *), true);
	if (0 != pthread_create(&g_sindex_populate_th, 0, as_sindex__populate_fn, 0)) {
		cf_crash(AS_INDEX, " Could not create sindex populate thread ");
	}

	g_sindex_destroy_q = cf_queue_create(sizeof(as_sindex *), true);
	if (0 != pthread_create(&g_sindex_destroy_th, 0, as_sindex__destroy_fn, 0)) {
		cf_crash(AS_INDEX, " Could not create sindex destroy thread ");
	}

	if (0 != pthread_create(&g_sindex_defrag_th, 0, as_sindex__defrag_fn, 0)) {
		cf_crash(AS_INDEX, " Could not create sindex defrag thread ");
	}

	g_sindex_populateall_done_q = cf_queue_create(sizeof(int), true);
	// At the beginning it is false. It is set to true when all the sindex
	// are populated.
	g_sindex_boot_done = false;

	ai_btree_init();
}
