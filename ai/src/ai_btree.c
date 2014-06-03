/*
 * ai_btree.c
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

#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

#include "ai.h"
#include "ai_globals.h"
#include "ai_obj.h"
#include "ai_btree.h"
#include "bt_iterator.h"
#include "bt_output.h"
#include "find.h"

#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_ll.h>

#include "fault.h"
#include "queue.h"
#include "util.h"

#define DIG_ARRAY_QUEUE_HIGHWATER 512

#define AI_ARR_MAX_USED 32

/*
 *  Default file to use for printing a B-Tree by the "sindex-dump:" Info. command.
 */
#define DEFAULT_BTREE_DUMP_FILENAME "/tmp/BTREE.dump"

/*
 *  Global determining whether to use array rather than B-Tree.
 */
bool g_use_arr = true;

// AI_BTREE GLOBALS
static cf_queue *g_q_dig_arr = NULL;
extern pthread_rwlock_t g_ai_rwlock;

#define AI_GRLOCK()													\
	do {																\
		int ret = pthread_rwlock_rdlock(&g_ai_rwlock);					\
		if (ret) cf_warning(AS_SINDEX, "AI_RLOCK (%d) %s:%d", ret, __FILE__, __LINE__); \
	} while (0);

#define AI_GWLOCK()													\
	do {																\
		int ret = pthread_rwlock_wrlock(&g_ai_rwlock);					\
		if (ret) cf_warning(AS_SINDEX, "AI_WLOCK (%d) %s:%d",ret, __FILE__, __LINE__); \
	} while (0);

#define AI_UNLOCK()													\
	do {																\
		int ret = pthread_rwlock_unlock(&g_ai_rwlock);					\
		if (ret) cf_warning(AS_SINDEX, "AI_UNLOCK (%d) %s:%d",ret, __FILE__, __LINE__); \
	} while (0);

static void
cloneDigestFromai_obj(cf_digest *d, ai_obj *akey)
{
	memcpy(d, &akey->y, CF_DIGEST_KEY_SZ);
}

static void
init_ai_objFromDigest(ai_obj *akey, cf_digest *d)
{
	init_ai_objU160(akey, *(uint160 *)d);
}


int
ll_ai_obj_dig_reduce_fn(cf_ll_element *ele, void *udata)
{
	return CF_LL_REDUCE_DELETE;
}

void
ll_ai_obj_dig_destroy_fn(cf_ll_element *ele)
{
	ll_ai_obj_dig_element * node = (ll_ai_obj_dig_element *) ele;
	if (node) {
		if (node->a)
			cf_free(node->a);
		cf_free(node);
	}
}

void
ai_btree_init(void) {
	if (!g_q_dig_arr) {
		g_q_dig_arr = cf_queue_create(sizeof(void *), true);
	}
}

static dig_arr_t *
getDigestArray(void)
{
	dig_arr_t *dt;
	if (cf_queue_pop(g_q_dig_arr, &dt, CF_QUEUE_NOWAIT) == CF_QUEUE_EMPTY) {
		dt = cf_malloc(sizeof(dig_arr_t));
	}
	dt->num = 0;
	return dt;
}

void
releaseDigArrToQueue(void *v)
{
	dig_arr_t *dt = (dig_arr_t *)v;
	if (cf_queue_sz(g_q_dig_arr) < DIG_ARRAY_QUEUE_HIGHWATER) {
		cf_queue_push(g_q_dig_arr, &dt);
	} else cf_free(dt);
}

const byte INIT_CAPACITY = 1;

static ai_arr *
ai_arr_new()
{
	ai_arr *arr = cf_malloc(sizeof(ai_arr) + (INIT_CAPACITY * CF_DIGEST_KEY_SZ));
	if (!arr) return NULL;
	arr->capacity = INIT_CAPACITY;
	arr->used = 0;
	return arr;
}

static void
ai_arr_move_to_tree(ai_arr *arr, bt *nbtr)
{
	for (int i = 0; i < arr->used; i++) {
		ai_obj apk;
		init_ai_objFromDigest(&apk, (cf_digest *)&arr->data[i * CF_DIGEST_KEY_SZ]);
		if (!btIndNodeAdd(nbtr, &apk)) {
			// what to do ??
			continue;
		}
	}
}

/*
 * Side effect if success full *arr will be freed
 */
static void
ai_arr_destroy(ai_arr *arr)
{
	if (!arr) return;
	cf_free(arr);
}

static int
ai_arr_size(ai_arr *arr)
{
	if (!arr) return 0;
	return(sizeof(ai_arr) + (arr->capacity * CF_DIGEST_KEY_SZ));
}

/*
 * Finds the digest in the AI array.
 * Returns
 *      idx if found
 *      -1  if not found
 */
static int
ai_arr_find(ai_arr *arr, cf_digest *dig)
{
	for (int i = 0; i < arr->used; i++) {
		if (0 == cf_digest_compare(dig, (cf_digest *)&arr->data[i * CF_DIGEST_KEY_SZ])) {
			return i;
		}
	}
	return -1;
}

static ai_arr *
ai_arr_shrink(ai_arr *arr)
{
	int size = arr->capacity / 2;

	// Do not shrink if the capacity not greater than 4
	// or if the halving capacity is not a extra level
	// over currently used
	if ((arr->capacity <= 4) ||
			(size < arr->used * 2)) {
		return arr;
	}

	ai_arr * temp_arr = cf_realloc(arr, sizeof(ai_arr) + (size * CF_DIGEST_KEY_SZ));
	if (!temp_arr) {
		cf_warning(AS_SINDEX, "Shrink Failed ... ignoring...");
		return arr;
	}
	temp_arr->capacity = size;
	return temp_arr;
}

static ai_arr *
ai_arr_delete(ai_arr *arr, cf_digest *dig, bool *notfound)
{
	int idx = ai_arr_find(arr, dig);
	// Nothing to delete
	if (idx < 0) {
		*notfound = true;
		return arr;
	}
	if (idx != arr->used - 1) {
		int dest_offset = idx * CF_DIGEST_KEY_SZ;
		int src_offset = (arr->used - 1) * CF_DIGEST_KEY_SZ;
		// move last element
		memcpy(&arr->data[dest_offset], &arr->data[src_offset], CF_DIGEST_KEY_SZ);
	}
	arr->used--;
	return ai_arr_shrink(arr);
}

/*
 * Returns
 *      arr pointer in case of successful operation
 *      NULL in case of failure
 */
static ai_arr *
ai_arr_expand(ai_arr *arr)
{
	int size = arr->capacity * 2;

	if (size > AI_ARR_MAX_SIZE) {
		cf_crash(AS_SINDEX, "Refusing to expand ai_arr to %d (beyond limit of %d)", size, AI_ARR_MAX_SIZE);
	}

	arr = cf_realloc(arr, sizeof(ai_arr) + (size * CF_DIGEST_KEY_SZ));
	//cf_info(AS_SINDEX, "EXPAND REALLOC to %d", size);
	if (!arr) {
		return NULL;
	}
	arr->capacity = size;
	return arr;
}

/*
 * Returns
 *      arr in case of success
 *      NULL in case of failure
 */
static ai_arr *
ai_arr_insert(ai_arr *arr, cf_digest *dig, bool *found)
{
	int idx = ai_arr_find(arr, dig);
	// already found
	if (idx >= 0) {
		*found = true;
		return arr;
	}
	if (arr->used == arr->capacity) {
		arr = ai_arr_expand(arr);
	}
	if (!arr) {
		return NULL;
	}
	memcpy(&arr->data[arr->used * CF_DIGEST_KEY_SZ], dig, CF_DIGEST_KEY_SZ);
	arr->used++;
	return arr;
}

/*
 * Returns the size diff
 */
static int
anbtr_check_convert(ai_nbtr *anbtr, uchar pktyp)
{
	// Nothing to do
	if (anbtr->is_btree)
		return 0;

	ai_arr *arr = anbtr->u.arr;
	if (arr && (arr->used >= AI_ARR_MAX_USED)) {
		//cf_info(AS_SINDEX,"Flipped @ %d", arr->used);
		ulong ba = ai_arr_size(arr);
		// Allocate btree move digest from arr to btree
		bt *nbtr = createIndexNode(pktyp, COL_TYPE_NONE);
		if (!nbtr) {
			cf_warning(AS_SINDEX, "btree allocation failure");
			return 0;
		}

		ai_arr_move_to_tree(arr, nbtr);
		ai_arr_destroy(anbtr->u.arr);

		// Update anbtr
		anbtr->u.nbtr = nbtr;
		anbtr->is_btree = true;

		ulong aa = nbtr->msize;
		return (aa - ba);
	}
	return 0;
}

/*
 *  return -1    in case of failure
 *          size of allocation in case of success
 */
static int
anbtr_check_init(ai_nbtr *anbtr, uchar pktyp)
{
	bool create_arr = false;
	bool create_nbtr = false;

	if (anbtr->is_btree) {
		if (anbtr->u.nbtr) {
			create_nbtr = false;
		} else {
			create_nbtr = true;
		}
	} else {
		if (anbtr->u.arr) {
			create_arr = false;
		} else {
			if (g_use_arr) {
				create_arr = true;
			} else {
				create_nbtr = true;
			}
		}
	}

	// create array or btree
	if (create_arr) {
		anbtr->u.arr = ai_arr_new();
		if (!anbtr->u.arr) {
			return -1;
		}
		return ai_arr_size(anbtr->u.arr);
	} else if (create_nbtr) {
		anbtr->u.nbtr = createIndexNode(pktyp, COL_TYPE_NONE);
		if (!anbtr->u.nbtr) {
			return -1;
		}
		anbtr->is_btree = true;
		return anbtr->u.nbtr->msize;
	} else {
		if (!anbtr->u.arr && !anbtr->u.nbtr) {
			cf_warning(AS_SINDEX, "Something wrong!!!");
			return -1;
		}
	}
	return 0;
}

/*
 * Insert operation for the nbtr does the following
 * 1. Sets up anbtr if it is set up
 * 2. Inserts in the arr or nbtr depending number of elements.
 * 3. Cuts over from arr to btr at AI_ARR_MAX_USED
 *
 * Parameter:   ibtr  : Btree of key
 *              acol  : Secondary index key
 *              apk   : value (primary key to be inserted)
 *              pktyp : value type (U160 currently)
 *
 * Returns:
 *      AS_SINDEX_OK        : In case of success
 *      AS_SINDEX_ERR       : In case of failure
 *      AS_SINDEX_KEY_FOUND : If key already exists
 */
static int
reduced_iAdd(bt *ibtr, ai_obj *acol, ai_obj *apk, uchar pktyp)
{
	ai_nbtr *anbtr = (ai_nbtr *)btIndFind(ibtr, acol);
	ulong ba = 0, aa = 0;
	bool allocated_anbtr = false;
	if (!anbtr) {
		anbtr = cf_malloc(sizeof(ai_nbtr));
		aa += sizeof(ai_nbtr);
		if (!anbtr) {
			cf_warning(AS_SINDEX, "Allocation failure for anbtr");
			return AS_SINDEX_ERR;
		}
		memset(anbtr, 0, sizeof(ai_nbtr));
		allocated_anbtr = true;
	}

	// Init the array
	int ret = anbtr_check_init(anbtr, pktyp);
	if (ret < 0) {
		if (allocated_anbtr) {
			cf_free(anbtr);
		}
		return AS_SINDEX_ERR;
	} else if (ret) {
		ibtr->nsize += ret;
		btIndAdd(ibtr, acol, (bt *)anbtr);
	}

	// Convert from arr to nbtr if limit is hit
	ibtr->nsize += anbtr_check_convert(anbtr, pktyp);

	// If already a btree use it
	if (anbtr->is_btree) {
		bt *nbtr = anbtr->u.nbtr;
		if (!nbtr) {
			return AS_SINDEX_ERR;
		}

		if (btIndNodeExist(nbtr, apk)) {
			return AS_SINDEX_KEY_FOUND;
		}

		ba += nbtr->msize;
		if (!btIndNodeAdd(nbtr, apk)) {
			return AS_SINDEX_ERR;
		}
		aa += nbtr->msize;

	} else {
		ai_arr *arr = anbtr->u.arr;
		if (!arr) {
			return AS_SINDEX_ERR;
		}

		ba += ai_arr_size(anbtr->u.arr);
		bool found = false;
		ai_arr *t_arr = ai_arr_insert(arr, (cf_digest *)&apk->y, &found);
		if (!t_arr) {
			return AS_SINDEX_ERR;
		} else if (found) {
			return AS_SINDEX_KEY_FOUND;
		}
		anbtr->u.arr = t_arr;
		aa += ai_arr_size(anbtr->u.arr);
	}
	ibtr->nsize += (aa - ba);  // ibtr inherits nbtr

	return AS_SINDEX_OK;
}

/*
 * Delete operation for the nbtr does the following. Delete in the arr or nbtr
 * based on state of anbtr
 *
 * Parameter:   ibtr  : Btree of key
 *              acol  : Secondary index key
 *              apk   : value (primary key to be inserted)
 *
 * Returns:
 *      AS_SINDEX_OK           : In case of success
 *      AS_SINDEX_ERR          : In case of failure
 *      AS_SINDEX_KEY_NOTFOUND : If key does not exist
 */
static int
reduced_iRem(bt *ibtr, ai_obj *acol, ai_obj *apk)
{
	ai_nbtr *anbtr = (ai_nbtr *)btIndFind(ibtr, acol);
	ulong ba = 0, aa = 0;
	if (!anbtr) {
		return AS_SINDEX_ERR;
	}
	if (anbtr->is_btree) {
		if (!anbtr->u.nbtr) return AS_SINDEX_ERR;

		// Remove from nbtr if found
		bt *nbtr = anbtr->u.nbtr;
		if (!btIndNodeExist(nbtr, apk)) {
			return AS_SINDEX_KEY_NOTFOUND;
		}
		ba = nbtr->msize;
		int nkeys = btIndNodeDelete(nbtr, apk, NULL);
		aa = nbtr->msize;

		// remove from ibtr
		if (!nkeys) {
			btIndDelete(ibtr, acol);
			aa = 0;
			bt_destroy(nbtr);
			ba += sizeof(ai_nbtr);
			cf_free(anbtr);
		}
	} else {
		if (!anbtr->u.arr) return AS_SINDEX_ERR;

		// Remove from arr if found
		bool notfound = false;
		ba = ai_arr_size(anbtr->u.arr);
		anbtr->u.arr = ai_arr_delete(anbtr->u.arr, (cf_digest *)&apk->y, &notfound);
		if (notfound) return AS_SINDEX_KEY_NOTFOUND;
		aa = ai_arr_size(anbtr->u.arr);

		// Remove from ibtr
		if (anbtr->u.arr->used == 0) {
			btIndDelete(ibtr, acol);
			aa = 0;
			ai_arr_destroy(anbtr->u.arr);
			ba += sizeof(ai_nbtr);
			cf_free(anbtr);
		}
	}
	ibtr->nsize -= (ba - aa);

	return AS_SINDEX_OK;
}

static char *
str_concat(char *first, char separator, char *second)
{
	char *str;
	size_t str_len = strlen(first) + strlen(second) + 2;

	if (!(str = cf_malloc(str_len))) {
		return NULL;
	}

	if (0 > snprintf(str, str_len, "%s%c%s", first, separator, second)) {
		cf_free(str);
		return NULL;
	}

	return str;
}

static char *
create_tname(char *ns_name, char *set)
{
	return str_concat(ns_name, '.', (set ? set : ""));
}

static char *
create_tname_from_imd(const as_sindex_metadata *imd)
{
	return create_tname(imd->ns_name, imd->set);
}

static char *
create_cname(char *bin_name, int bin_type)
{
	char bin_type_str[NAME_STR_LEN];

	if (0 > snprintf(bin_type_str, sizeof(bin_type_str), "%d", bin_type)) {
		return NULL;
	}

	return str_concat(bin_name, '_', bin_type_str);
}

static char *
create_cname_from_imd(const as_sindex_metadata *imd) {
	return create_cname(imd->bnames[0], imd->btype[0]);
}

static char *
get_iname(char *ns_name, char *iname)
{
	return str_concat(ns_name, '.', iname);
}

static char *
get_iname_from_imd(const as_sindex_metadata *imd)
{
	return get_iname(imd->ns_name, imd->iname);
}

void
ai_set_simatch_by_name(char *ns, char *iname, int *imatch, int *simatch)
{
	char *ai_iname = get_iname(ns, iname);

	*simatch = -1;

	AI_GRLOCK();

	int im = match_index_name(ai_iname);
	if (im != -1) {
		*simatch = Index[im].simatch;
	}

	AI_UNLOCK();

	*imatch = im;
}

int
ai_btree_key_hash(as_sindex_metadata *imd, as_sindex_bin *b)
{
	uint64_t u;

	if (C_IS_Y(imd->dtype)) {
		char *x = (char *) &b->digest; // x += 4;
		u = ((* (uint128 *) x) % imd->nprts);
	} else {
		u = (((uint64_t) b->u.i64) % imd->nprts);
	}

	return (int) u;
}

int
ai_findandset_imatch(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, int idx)
{
	if (!Num_tbls) {
		return AS_SINDEX_ERR;
	}

	char *tname = NULL, *cname = NULL, *iname = NULL;
	int ret = AS_SINDEX_OK;

	if (!(tname = create_tname_from_imd(imd))) {
		return AS_SINDEX_ERR_NO_MEMORY;
	}

	ret = AS_SINDEX_ERR;

	AI_GRLOCK();

	int tmatch = find_table(tname);
	if (tmatch == -1) {
		goto END;
	}
	if (imd->iname) {
		if (!(iname = get_iname_from_imd(imd))) {
			ret = AS_SINDEX_ERR_NO_MEMORY;
			goto END;
		}
		char idx_str[NAME_STR_LEN];
		snprintf(idx_str, sizeof(idx_str), "%d", idx);
		char *piname = str_concat(iname, '.', idx_str);
		pimd->imatch = match_partial_index_name(piname);
		cf_free(piname);
	} else {
		if (!(cname = create_cname_from_imd(imd))) {
			ret = AS_SINDEX_ERR_NO_MEMORY;
			goto END;
		}
		icol_t *ic = find_column(tmatch, cname);
		if (!ic) {
			goto END;
		}
		pimd->imatch = find_partial_index(tmatch, ic);
	}
	if (pimd->imatch == -1) {
		SITRACE(imd->si, META, debug, "Index%s: %s not found", imd->iname ? "" : "column-name", imd->iname ? iname : cname);
		goto END;
	}

	ret = AS_SINDEX_OK;

END:

	AI_UNLOCK();

	cf_free(tname);
	cf_free(iname);
	cf_free(cname);

	return ret;
}

/*
 * Return 0  in case of success
 *        -1 in case of failure
 */
static int
btree_addsinglerec(as_sindex_metadata *imd, cf_digest *dig, cf_ll *recl, uint64_t *n_bdigs)
{
	if (!as_sindex_partition_isactive(imd->si->ns, dig)) {
		return 0;
	}
	cf_ll_element *ele = recl->tail;
	bool create = !ele;
	dig_arr_t *dt;
	if (!create) {
		dt = ((ll_recl_element*)ele)->dig_arr;
		if (dt->num == NUM_DIGS_PER_ARR) {
			create = 1;
		}
	}
	if (create) {
		dt = getDigestArray();
		if (!dt) {
			return -1;
		}
		ll_recl_element * node;
		node = cf_malloc(sizeof(ll_recl_element));
		node->dig_arr = dt;
		cf_ll_append(recl, (cf_ll_element *)node);
	}
	memcpy(&dt->digs[dt->num], dig, CF_DIGEST_KEY_SZ);
	dt->num++;
	*n_bdigs = *n_bdigs + 1;
	return 0;
}

/*
 * Return 0 in case of success
 *       -1 in case of failure
 */
static int
add_recs_from_nbtr(as_sindex_metadata *imd, ai_obj *ikey, bt *nbtr, as_sindex_qctx *qctx, bool fullrng)
{
	int ret = 0;
	ai_obj sfk, efk;
	init_ai_obj(&sfk);
	init_ai_obj(&efk);
	btSIter *nbi;
	btEntry *nbe;
	btSIter stack_nbi;

	if (fullrng) {
		nbi = btSetFullRangeIter(&stack_nbi, nbtr, 1, NULL);
	} else { // search from LAST batches end-point
		init_ai_objFromDigest(&sfk, &qctx->bdig);
		assignMaxKey(nbtr, &efk);
		nbi = btSetRangeIter(&stack_nbi, nbtr, &sfk, &efk, 1);
	}
	if (nbi) {
		while ((nbe = btRangeNext(nbi, 1))) {
			ai_obj *akey = nbe->key;
			// FIRST can be REPEAT (last batch)
			if (!fullrng && ai_objEQ(&sfk, akey)) {
				continue;
			}
			if (btree_addsinglerec(imd, (cf_digest *)&akey->y, qctx->recl, &qctx->n_bdigs)) {
				ret = -1;
				break;
			}
			if (qctx->n_bdigs == qctx->bsize) {
				if (ikey) {
					ai_objClone(qctx->bkey, ikey);
				}
				cloneDigestFromai_obj(&qctx->bdig, akey);
				break;
			}
		}
		btReleaseRangeIterator(nbi);
	} else {
		cf_warning(AS_QUERY, "Could not find nbtr iterator.. skipping !!");
	}
	return ret;
}

static int
add_recs_from_arr(as_sindex_metadata *imd, ai_obj *ikey, ai_arr *arr, as_sindex_qctx *qctx, bool fullrng)
{
	bool ret = 0;

	for (int i = 0; i < arr->used; i++) {
		if (btree_addsinglerec(imd, (cf_digest *)&arr->data[i * CF_DIGEST_KEY_SZ], qctx->recl, &qctx->n_bdigs)) {
			ret = -1;
			break;
		}
		// do not break on hitting batch limit, if the tree converts to
		// bt from arr, there is no way to know which digest were already
		// returned when attempting subsequent batch. Return the entire
		// thing.
	}
	// mark nbtr as finished and copy the offset
	qctx->last = true;
	if (ikey) {
		ai_objClone(qctx->bkey, ikey);
	}

	return ret;
}

/*
 * Return 0  in case of success
 *        -1 in case of failure
 */
static int
get_recl(as_sindex_metadata *imd, ai_obj *afk, as_sindex_qctx *qctx)
{
	as_sindex_pmetadata *pimd = &imd->pimd[qctx->pimd_idx];
	ai_nbtr *anbtr = (ai_nbtr *)btIndFind(pimd->ibtr, afk);

	if (!anbtr) {
		return 0;
	}

	if (anbtr->is_btree) {
		if (add_recs_from_nbtr(imd, NULL, anbtr->u.nbtr, qctx, qctx->first)) {
			return -1;
		}
	} else {
		// If already entire batch is returned
		if (qctx->last) {
			return 0;
		}
		if (add_recs_from_arr(imd, NULL, anbtr->u.arr, qctx, qctx->first)) {
			return -1;
		}
	}
	return 0;
}

/*
 * Return 0  in case of success
 *        -1 in case of failure
 */
static int
get_numeric_range_recl(as_sindex_metadata *imd, uint64_t begk, uint64_t endk, as_sindex_qctx *qctx)
{
	ai_obj sfk;
	init_ai_objLong(&sfk, qctx->first ? begk : qctx->bkey->l);
	ai_obj efk;
	init_ai_objLong(&efk, endk);
	as_sindex_pmetadata *pimd = &imd->pimd[qctx->pimd_idx];
	bool fullrng = qctx->first;
	int ret = 0;
	btSIter *bi = btGetRangeIter(pimd->ibtr, &sfk, &efk, 1);
	btEntry *be;

	if (bi) {
		while ((be = btRangeNext(bi, 1))) {
			ai_obj *ikey = be->key;
			ai_nbtr *anbtr = be->val;
			if (!anbtr) {
				ret = -1;
				break;
			}

			// figure out nbtr to deal with. If the key which was
			// used last time vanishes work with next key. If the
			// key exist but 'last' entry made to list in the last
			// iteration; Move to next nbtr
			if (!fullrng) {
				if (!ai_objEQ(&sfk, ikey)) {
					fullrng = 1; // bkey disappeared
				} else if (qctx->last) {
					qctx->last = false;
					continue;
				}
			}

			if (anbtr->is_btree) {
				if (add_recs_from_nbtr(imd, ikey, anbtr->u.nbtr, qctx, fullrng)) {
					ret = -1;
					break;
				}
			} else {
				if (add_recs_from_arr(imd, ikey, anbtr->u.arr, qctx, fullrng)) {
					ret = -1;
					break;
				}
			}
			if (qctx->n_bdigs >= qctx->bsize) {
				break;
			}
			fullrng = 1;
		}
		btReleaseRangeIterator(bi);
	}
	return ret;
}

int
ai_btree_query(as_sindex_metadata *imd, as_sindex_range *srange, as_sindex_qctx *qctx)
{
	bool err = 1;
	if (!srange->isrange) { // EQUALITY LOOKUP
		ai_obj afk;
		if (C_IS_Y(imd->dtype)) {
			init_ai_objFromDigest(&afk, &srange->start.digest);
		}
		else {
			init_ai_objLong(&afk, srange->start.u.i64);
		}
		err = get_recl(imd, &afk, qctx);
	} else {                // RANGE LOOKUP
		err = get_numeric_range_recl(imd, srange->start.u.i64, srange->end.u.i64, qctx);
	}
	return (err ? AS_SINDEX_ERR_NO_MEMORY :
			(qctx->n_bdigs >= qctx->bsize) ? AS_SINDEX_CONTINUE : AS_SINDEX_OK);
}

int
ai_btree_put(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, as_sindex_key *skey, void *value)
{
	int ret = AS_SINDEX_OK;
	uint64_t uk = *(uint64_t *)value;
	cf_digest *keyd = (cf_digest *)value;

	ai_obj ncol;
	if (C_IS_Y(imd->dtype)) {
		init_ai_objFromDigest(&ncol, &skey->b[0].digest);
	}
	else {
		init_ai_objLong(&ncol, skey->b[0].u.i64);
	}
	ai_obj apk;
	init_ai_objFromDigest(&apk, keyd);

	cf_detail(AS_SINDEX, "Insert: %ld %ld %ld", *(uint64_t *) &ncol.y, *(uint64_t *) &skey->b[0].digest, *((uint64_t *) &apk.y));

	ulong bb = pimd->ibtr->msize + pimd->ibtr->nsize;
	ret = reduced_iAdd(pimd->ibtr, &ncol, &apk, COL_TYPE_U160);
	if (ret == AS_SINDEX_KEY_FOUND) {
		goto END;
	} else if (ret != AS_SINDEX_OK) {
		cf_warning(AS_SINDEX, "Insert into the btree failed");
		ret = AS_SINDEX_ERR_NO_MEMORY;
		goto END;
	}
	ulong ab = pimd->ibtr->msize + pimd->ibtr->nsize;
	if (!as_sindex_reserve_data_memory(imd, (ab - bb))) {
		reduced_iRem(pimd->ibtr, &ncol, &apk);
		ret = AS_SINDEX_ERR_NO_MEMORY;
		goto END;
	}
	SITRACE(imd->si, DML, debug, "ai__btree_insert(N): %s key: %d val %lu", imd->iname, skey->b[0].u.i64, uk);

END:

	return ret;
}

int
ai_btree_delete(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, as_sindex_key *skey, void *val)
{
	int ret = AS_SINDEX_OK;
	uint64_t uk = * (uint64_t *) val;
	uint64_t bv = skey->b[0].u.i64;

	if (!pimd->ibtr) {
		SITRACE(imd->si, DML, debug, "AI_BTREE_FAIL: Delete failed no ibtr %d %lu", bv, uk);
		return AS_SINDEX_KEY_NOTFOUND;
	}
	ai_obj ncol;
	if (C_IS_Y(imd->dtype)) {
		init_ai_objFromDigest(&ncol, &skey->b[0].digest);
	}
	else {
		init_ai_objLong(&ncol, skey->b[0].u.i64);
	}

	ai_obj apk;
	init_ai_objFromDigest(&apk, (cf_digest *)val);
	ulong bb = pimd->ibtr->msize + pimd->ibtr->nsize;
	ret = reduced_iRem(pimd->ibtr, &ncol, &apk);
	ulong ab = pimd->ibtr->msize + pimd->ibtr->nsize;
	as_sindex_release_data_memory(imd, (bb - ab));
	SITRACE(imd->si, DML, debug, "ai__btree_delete(N): key: %d - %lu", bv, uk);
	return ret;
}

/*
 * Internal function which adds digests to the defrag_list
 * Mallocs the nodes of defrag_list
 * Returns :
 *      -1 : Error
 *      number of digests found : success
 *
 */
static long
build_defrag_list_from_nbtr(as_namespace *ns, ai_obj *acol, bt *nbtr, long nofst, long *limit, cf_ll *apk2d)
{
	int error = -1;
	btEntry *nbe;
	// STEP 1: go thru a portion of the nbtr and find to-be-deleted-PKs
	// TODO: a range query may be smarter then using the Xth Iterator
	btSIter *nbi = (nofst ? btGetFullXthIter(nbtr, nofst, 1, NULL, 0) :
					btGetFullRangeIter(nbtr, 1, NULL));
	if (!nbi) {
		return error;
	}

	uint64_t stime = cf_getus();
	long found = 0;
	long processed = 0;

	while ((nbe = btRangeNext(nbi, 1))) {
		ai_obj *akey = nbe->key;
		// STEP 2: if this PK is to be deleted then add it to PKtoDeleteList
		int ret = as_sindex_can_defrag_record(ns, (cf_digest *) (&akey->y));
		if (ret == AS_SINDEX_GC_SKIP_ITERATION) {
			*limit = 0;
			break;
		} else if (ret == AS_SINDEX_GC_OK){
			ai_obj_digest_t *a = cf_malloc(sizeof(ai_obj_digest_t));
			if (!a) {
				return error;
			}
			cloneDigestFromai_obj(&a->dig, akey);
			ai_objClone(&a->acol, acol);
			cf_detail(AS_SINDEX, "Built to Defrag [%lu %ld] from  [%lu %ld]",
					  acol->l, *((uint64_t *)&akey->y), a->acol.l, *((uint64_t *)&a->dig));
			// add this to the list
			ll_ai_obj_dig_element * node;
			node    = cf_malloc(sizeof(ll_ai_obj_dig_element));
			node->a = a;
			cf_ll_append(apk2d, (cf_ll_element *)node );
			found++;
		}
		processed++;
		(*limit)--;
		if (*limit == 0) break;
	}
	btReleaseRangeIterator(nbi);
	cf_detail(AS_SINDEX, "Total lookup %ld found %ld time %d microseconds", processed, found, cf_getus() - stime);

	return processed;
}

static long
build_defrag_list_from_arr(as_namespace *ns, ai_obj *acol, ai_arr *arr, long nofst, long *limit, cf_ll *apk2d)
{
	int error = -1;
	long found = 0;
	long processed = 0;
	uint64_t stime = cf_getus();

	for (int i = nofst; i < arr->used; i++) {
		int ret = as_sindex_can_defrag_record(ns, (cf_digest *) &arr->data[i * CF_DIGEST_KEY_SZ]);
		if (ret == AS_SINDEX_GC_SKIP_ITERATION) {
			*limit = 0;
			break;
		} else if (ret == AS_SINDEX_GC_OK) {
			ai_obj_digest_t *a = cf_malloc(sizeof(ai_obj_digest_t));
			if (!a) {
				return error;
			}
			memcpy(&a->dig, (cf_digest *) &arr->data[i * CF_DIGEST_KEY_SZ], CF_DIGEST_KEY_SZ);
			ai_objClone(&a->acol, acol);
			// add this to list
			ll_ai_obj_dig_element * node;
			node    = cf_malloc(sizeof(ll_ai_obj_dig_element));
			node->a = a;
			cf_ll_append(apk2d, (cf_ll_element *)node );
			found++;
		}
		processed++;
		(*limit)--;
		if (*limit == 0) {
			break;
		}
	}
	cf_detail(AS_SINDEX, "Total lookup %ld found %ld time %d microseconds", processed, found, cf_getus() - stime);

	return processed;
}

/*
 * Aerospike Index interface to build a defrag_list.
 *
 * Returns :
 *  AS_SINDEX_DONE     ---> The current pimd has been scanned completely for defragging
 *  AS_SINDEX_CONTINUE ---> Current pimd sill may have some candidate digest to be defragged
 *  AS_SINDEX_ERR      ---> Error. Abort this pimd.
 *
 *  Notes :  Caller has the responsibility to free the iterators.
 *           Requires a proper offset value from the caller.
 */
int
ai_btree_build_defrag_list(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, ai_obj *icol,
						   long *nofst, long limit, cf_ll *apk2d)
{
	int ret = AS_SINDEX_ERR;

	if (!pimd || !imd) {
		return ret;
	}

	int total = limit;
	uint64_t starttime = cf_getus();
	as_namespace *ns = imd->si->ns;
	if (!ns) {
		ns = as_namespace_get_byname((char *)imd->ns_name);
	}
	char *iname = get_iname_from_imd(imd);
	if (!iname) {
		ret = AS_SINDEX_ERR_NO_MEMORY;
		return ret;
	}
	if (!pimd || !pimd->ibtr || !pimd->ibtr->numkeys) {
		goto END;
	}
	//Entry is range query, FROM previous icol TO maxKey(ibtr)
	if (icol->empty) {
		assignMinKey(pimd->ibtr, icol); // init first call
	}
	ai_obj iH;
	assignMaxKey(pimd->ibtr, &iH);
	btEntry *be = NULL;
	btSIter *bi = btGetRangeIter(pimd->ibtr, icol, &iH, 1);
	if (!bi) {
		goto END;
	}

	while ( true ) {
		be = btRangeNext(bi, 1);
		if (!be) {
			ret = AS_SINDEX_DONE;
			break;
		}
		ai_obj *acol = be->key;
		ai_nbtr *anbtr = be->val;
		long processed = 0;
		if (!anbtr) {
			break;
		}
		if (anbtr->is_btree) {
			processed = build_defrag_list_from_nbtr(ns, acol, anbtr->u.nbtr, *nofst, &limit, apk2d);
		} else {
			processed = build_defrag_list_from_arr(ns, acol, anbtr->u.arr, *nofst, &limit, apk2d);
		}

		if (processed < 0) {    // error .. abort everything.
			cf_detail(AS_SINDEX, "build_defrag_list returns an error. Aborting defrag on current pimd");
			ret = AS_SINDEX_ERR;
			break;
		}
		// This tree may have some more digest to defrag
		if (limit == 0) {
			*nofst = *nofst + processed;
			ai_objClone(icol, acol);
			cf_detail(AS_SINDEX, "Current pimd may need more iteration of defragging.");
			ret = AS_SINDEX_CONTINUE;
			break;
		}

		// We have finished this tree. Yet we have not reached our limit to defrag.
		// Goes to next iteration
		*nofst = 0;
		ai_objClone(icol, acol);
	};
	btReleaseRangeIterator(bi);
END:
	cf_free(iname);
	cf_detail(AS_SINDEX, " List creation time %d microseconds for batch size %d", cf_getus() - starttime, total);

	return ret;
}

/*
 * Deletes the digest as in the passed in as apk2d, bound by n2del number of
 * elements per iteration, with *deleted successful deletes.
 */
bool
ai_btree_defrag_list(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, cf_ll *apk2d, ulong n2del, ulong *deleted)
{
	// If n2del is zero here, that means caller do not want to defrag
	if (n2del == 0 ) {
		return false;
	}
	ulong success = 0;
	as_namespace *ns = imd->si->ns;
	// STEP 3: go thru the PKtoDeleteList and delete the keys
	ulong bb = pimd->ibtr->msize + pimd->ibtr->nsize;
	while (apk2d->sz) {
		ll_ai_obj_dig_element * node = (ll_ai_obj_dig_element * )apk2d->head;
		ai_obj_digest_t *a           = node->a;

		// check before deleting. The digest may re-appear after the list
		// creation and before deletion from the secondary index
		int ret = as_sindex_can_defrag_record(ns, &a->dig);
		if (ret == AS_SINDEX_GC_SKIP_ITERATION) {
			break;
		} else if (ret == AS_SINDEX_GC_OK) {
			ai_obj           apk;
			init_ai_objFromDigest(&apk, &a->dig);
			ai_obj          *acol = &(a->acol);

			cf_detail(AS_SINDEX, "Defragged %lu %ld", acol->l, *((uint64_t *)&apk.y));

			if (reduced_iRem(pimd->ibtr, acol, &apk) == AS_SINDEX_OK) {
				success++;
			}
		}

		cf_ll_delete(apk2d, (cf_ll_element*)node);
		n2del--;
		if (n2del == 0) {
			break;
		}
	}
	ulong ab = pimd->ibtr->msize + pimd->ibtr->nsize;
	as_sindex_release_data_memory(imd, (bb - ab));
	*deleted = success;
	return apk2d->sz ? true : false;
}

/* NOTE: The creation of a secondary index is the following two commands
          0.) optional: CREATE TABLE namespace (pk U160, __dummy TEXT)
          1.) ALTER TABLE namespace ADD COLUMN binname columntype
          2.) CREATE [UNIQUE] INDEX indexname ON namespace (binname)
 */
int
ai_btree_create(as_sindex_metadata *imd, int simatch, int *bimatch, int nprts)
{
	char *iname = NULL, *cname = NULL, *tname = NULL;
	int ret = AS_SINDEX_ERR, rv;

	if (1 != imd->num_bins) {
		cf_warning(AS_SINDEX, "Multi-bin indexes not supported");
		return ret;
	}

	if (!(tname = create_tname_from_imd(imd))) {
		return AS_SINDEX_ERR_NO_MEMORY;
	}

	if (!(cname = create_cname_from_imd(imd))) {
		return AS_SINDEX_ERR_NO_MEMORY;
	}

	AI_GWLOCK();

	int tmatch = find_table(tname);
	if (tmatch == -1) {
		if (0 > (rv = ai_create_table(tname))) {
			cf_warning(AS_SINDEX, "Create table %s failed (rv %d)", tname, rv);
			goto END;
		}
		tmatch = find_table(tname);
	}
	r_tbl_t *rt = &Tbl[tmatch];

	// 1.) add entries in Aerospike Index's virtual TABLE
	int col_type = imd->btype[0];
	icol_t *ic = find_column(tmatch, cname);
	if (!ic) { // COLUMN does not exist
		if (0 > ai_add_column(tname, cname, col_type)) {
			goto END;
		}
		// Add (cmatch+1) always non-zero
		SITRACE(imd->si, META, debug, "Added Mapping [BINNAME=%s: BINID=%d: COLID%d] [IMATCH=%d: SIMATCH=%d: INAME=%s]",
				imd->bnames[0], imd->binid[0], rt->col_count, Num_indx - 1, simatch, imd->iname);
	}

	//NOTE: COMMAND: CREATE PARTITIONED INDEX iname ON tname (cname) NUM = nprts
	if (!(iname = get_iname_from_imd(imd))) {
		ret = AS_SINDEX_ERR_NO_MEMORY;
		goto END;
	}

	if (0 > (rv = ai_create_index(iname, tname, cname, col_type, nprts))) {
		cf_warning(AS_SINDEX, "Create index %s failed (rv %d)", iname, rv);
		goto END;
	}

	*bimatch = match_partial_index_name(iname);
	GTRACE(META, debug, "cr8SecIndex: iname: %s bname: %s type: %d ns: %s set: %s tmatch: %d bimatch: %d",
		   imd->iname, imd->bnames[0], imd->btype[0], imd->ns_name, imd->set, tmatch, *bimatch);

	ret = AS_SINDEX_OK;

END:

	AI_UNLOCK();

	cf_free(tname);
	cf_free(cname);
	cf_free(iname);

	return ret;
}

int
ai_post_index_creation_setup_pmetadata(as_sindex_metadata *imd, as_sindex_pmetadata *pimd, int simatch, int idx)
{
	if (idx == 0) {
		pimd->imatch = imd->bimatch;
	} else if (AS_SINDEX_OK != ai_findandset_imatch(imd, pimd, idx)) {
		return AS_SINDEX_ERR;
	}

	r_ind_t *ri = &Index[pimd->imatch];
	ri->simatch = simatch; //ref for simatch to enable search through Aerospike Index
	ri->done = true;
	if (idx == 0) { // idx of 0 means fill these in
		imd->dtype = ri->dtype;
		imd->btype[0] = ri->dtype;
	}
	pimd->tmatch = ri->tmatch;
	pimd->ibtr = ri->btr;

	return AS_SINDEX_OK;
}

// Iterate through the btree and cleanup local array
// if it is btree it will be cleaned up by Aerospike Index
// call for dropIndex
static int
ai_cleanup(bt *ibtr)
{
	if (!ibtr) {
		return 0;
	}

	btSIter stack_bi;
	btEntry *be;
	btSIter *bi = btSetFullRangeIter(&stack_bi, ibtr, 1, NULL);
	if (bi) {
		while ((be = btRangeNext(bi, 1))) {
			ai_nbtr *anbtr = be->val;
			if (anbtr) {
				if (!anbtr->is_btree) {
					ai_arr_destroy(anbtr->u.arr);
				}
			}
		}
		btReleaseRangeIterator(bi);
	}

	return 0;
}

int
ai_btree_destroy(as_sindex_metadata *imd)
{
	char *tname, *cname, *iname;

	AI_GWLOCK();

	for (int i = 0; i < imd->nprts; i++) {
		ai_cleanup(imd->pimd[i].ibtr);
	}

	if (!(tname = create_tname_from_imd(imd))) {
		return AS_SINDEX_ERR_NO_MEMORY;
	}

	if (!(cname = create_cname_from_imd(imd))) {
		return AS_SINDEX_ERR_NO_MEMORY;
	}

	if (0 > ai_drop_column(tname, cname)) {
		cf_warning(AS_SINDEX, "Failed to drop column %s from table %s", cname, tname);
	}

	cf_free(tname);
	cf_free(cname);

	if (!(iname = get_iname_from_imd(imd))) {
		return AS_SINDEX_ERR_NO_MEMORY;
	}

	if (0 > ai_drop_index(iname)) {
		cf_warning(AS_SINDEX, "Failed to drop index %s", iname);
	}

	cf_free(iname);

	AI_UNLOCK();

	return AS_SINDEX_OK;
}

int
ai_btree_dump(char *ns_name, char *setname, char *filename)
{
	char *tname;

	if (!(tname = create_tname(ns_name, setname))) {
		return -1;
	}

	AI_GRLOCK();

	int retval = dump_btree(tname, (filename ? filename : DEFAULT_BTREE_DUMP_FILENAME));

	AI_UNLOCK();

	cf_free(tname);

	return retval;
}

// Returns AS_SINDEX_ERR in case of failure
uint64_t
ai_btree_get_numkeys(as_sindex_metadata *imd)
{
	uint64_t val = 0;
	if ((!imd->ns_name)) {
		return AS_SINDEX_ERR;
	}

	for (int i = 0; i < imd->nprts; i++) {
		val += imd->pimd[i].ibtr->numkeys;
	}

	return val;
}

// Returns AS_SINDEX_ERR in case of failure
uint64_t
ai_btree_get_isize(as_sindex_metadata *imd)
{
	uint64_t size = 0;
	if ((!imd->ns_name)) {
		return AS_SINDEX_ERR;
	}

	for (int i = 0; i < imd->nprts; i++) {
		if (imd->pimd[i].ibtr->msize > 0) {
			size += imd->pimd[i].ibtr->msize;
		}
	}

	return size;
}

// Returns AS_SINDEX_ERR in case of failure
uint64_t
ai_btree_get_nsize(as_sindex_metadata *imd)
{
	uint64_t size = 0;
	if ((!imd->ns_name)) {
		return AS_SINDEX_ERR;
	}

	for (int i = 0; i < imd->nprts; i++) {
		if (imd->pimd[i].ibtr->nsize > 0) {
			size += imd->pimd[i].ibtr->nsize;
		}
	}

	return size;
}
