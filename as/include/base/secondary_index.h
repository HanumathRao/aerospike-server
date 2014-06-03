/*
 * secondary_index.h
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
 *  SYNOPSIS
 *  Abstraction to support secondary indexes with multiple implementations.
 */

#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_ll.h"

#include "dynbuf.h"
#include "hist.h"

#include "base/datamodel.h"
#include "base/monitor.h"
#include "base/proto.h"
#include "base/system_metadata.h"
#include "storage/storage.h"


#define AS_SINDEX_MAX_STRING_KSIZE 2048
#define SINDEX_SMD_KEY_SIZE        AS_ID_INAME_SZ + AS_ID_NAMESPACE_SZ 
#define SINDEX_SMD_VALUE_SIZE      (AS_SMD_MAJORITY_CONSENSUS_KEYSIZE)
#define LIST_ELEMENT_SIZE          24
#define DIGEST_LIST_EL_SIZE        (LIST_ELEMENT_SIZE + sizeof(cf_digest))
#define NUM_SINDEX_PARTITIONS      32
#define SINDEX_MODULE              "sindex_module"

/* 
 * Return status codes for index object functions.
 *
 * NB: When adding error code add the string in the as_sindex_err_str 
 * in secondary_index.c 
 *
 * Negative > 10 are the ones which show up and goes till client
 *
 * Positive are < 10 are something which are internal
 */
typedef enum {
	AS_SINDEX_ERR_SET_MISMATCH     = -15,
	AS_SINDEX_ERR_UNKNOWN_KEYTYPE  = -14,
	AS_SINDEX_ERR_BIN_NOTFOUND     = -13,
	AS_SINDEX_ERR_TYPE_MISMATCH    = -11,
	

	// Needed when attemping index create
	AS_SINDEX_ERR_FOUND            = -6,
	AS_SINDEX_ERR_NOTFOUND         = -5,
	AS_SINDEX_ERR_NO_MEMORY        = -4,
	AS_SINDEX_ERR_PARAM            = -3,
	AS_SINDEX_ERR_NOT_READABLE     = -2,
	AS_SINDEX_ERR                  = -1,
	AS_SINDEX_OK                   =  0,

	// Internal Not needed
	AS_SINDEX_CONTINUE             = 1,
	AS_SINDEX_DONE                 = 2,
	// Needed when inserting object in the btree.
	AS_SINDEX_KEY_FOUND            = 3,
	AS_SINDEX_KEY_NOTFOUND         = 4
} as_sindex_status;

typedef enum {
	AS_SINDEX_OP_UPDATE = 0,
	AS_SINDEX_OP_DELETE = 1,
	AS_SINDEX_OP_INSERT = 2,
	AS_SINDEX_OP_READ = 3
} as_sindex_op;

typedef enum {
	AS_SINDEX_GC_OK             = 0,
	AS_SINDEX_GC_ERROR          = 1,
	AS_SINDEX_GC_SKIP_ITERATION = 2
} as_sindex_gc_status;

/*
 * NB: DO NOT CHANGE NUMBERING, it matches COL_TYPE* to track down weird 
 * behavior if it does not match.
 */
typedef enum {
	AS_SINDEX_KTYPE_LONG   = 2, //Particle type INT
	AS_SINDEX_KTYPE_FLOAT  = 4, //Particle type INT
	AS_SINDEX_KTYPE_DIGEST = 10
} as_sindex_ktype;

as_particle_type as_sindex_pktype_from_sktype(as_sindex_ktype t);

typedef enum {
	AS_SINDEX_ITYPE_DEFAULT     = 0,
	AS_SINDEX_ITYPE_OBJECT      = 1
} as_sindex_type;

/*
 * TODO: Optimize it is a huge structure will cause cache invalidation
 * 320 bytes
 */
#define SINDEX_BINS_SETUP(skey_bin, size)         \
	as_sindex_bin skey_bin[(size)];                    \
	memset (&(skey_bin), 0, sizeof(as_sindex_bin) * (size)); \
	for (int id = 0; id < (size); id++) skey_bin[id].id = -1; 

/*
 * Used as structure to call into secondary indexes sindex_* interface
 * bin_id lists the bin id being touched. 
 */
#define SINDEX_FLAG_BIN_ISVALID    0x01
#define SINDEX_FLAG_BIN_DOFREE     0x02
#define SINDEX_STRONSTACK_VALSZ    256
typedef struct as_sindex_bin_s {
	uint32_t          id;
	as_particle_type  type; // this type is citrusleaf type
	uint32_t          valsz;
	union {
		char    *str; // sz is strlen
		char    *blob;
		int64_t  i64;
	} u;
	cf_digest         digest;
	byte              flag;
	char              stackstr[SINDEX_STRONSTACK_VALSZ];
} as_sindex_bin;

/* 
 * Configuration parameter and control variable for secondary indexes
 */
#define AS_SINDEX_CONFIG_IGNORE_ON_DESYNC     0x01

typedef struct as_sindex_config_s {
	uint64_t    defrag_period;
	uint32_t    defrag_max_units;
	uint64_t    data_max_memory;
	uint16_t    flag;
} as_sindex_config;

// First byte
#define IMD_FLAG_NO_RANGE_QUERY      0x0001
// Fourth byte
#define IMD_FLAG_LOCKSET             0x0200

struct btree;

typedef struct as_sindex_physical_metadata_s {
	pthread_rwlock_t    slock;
	int                 tmatch;
	int                 imatch;  // Aerospike Index Number
	struct btree       *ibtr;    // Aerospike Index pointer
} as_sindex_pmetadata;

typedef struct as_sindex_functional_metadata {
	char     * fname;
	char     * func;
	int        nfargs;
	char *   * fargs;
} as_sindex_fmetadata;

typedef struct as_sindex_metadata_s {
	// Run Time Data
	pthread_rwlock_t      slock;
	int                   bimatch;
	int                   tmatch;  // Aerospike Index to table(tmatch)
	int                   nprts;   // Aerospike Index Number of Index partitions
	struct as_sindex_s  * si;
	as_sindex_pmetadata * pimd;
	unsigned char         dtype;   // Aerospike Index type
	int                   binid[AS_SINDEX_BINMAX]; // Redundant info to aid search
	byte                  mfd_slot; // slot on the persistent file

	// Index Static Data (part persisted)
	char                * ns_name;
	char                * set;
	char                * iname;
	char                * bnames[AS_SINDEX_BINMAX];
	as_sindex_ktype       btype[AS_SINDEX_BINMAX]; // Same as Aerospike Index type
	as_sindex_type        itype;
	int                   num_bins;
	as_sindex_fmetadata   afi;
	uint8_t               oindx;
	uint32_t              flag;
	int 				  post_op;
} as_sindex_metadata;

/*
 * Stats are collected about memory utilization based on simple index
 * overhead. Any insert delete from the secondary index would update
 * this number and the memory management folks has to use this info.
 */
typedef struct as_sindex_stat_s {
	cf_atomic64        n_objects;
	int                n_keys;
	int                mem_used;

	cf_atomic64        n_reads;
	cf_atomic64        read_errs;

	cf_atomic64        n_writes;
	cf_atomic64        write_errs;
	histogram *        _write_hist;

	cf_atomic64        n_deletes;
	cf_atomic64        delete_errs;
	histogram *        _delete_hist;

	// Background thread stats
	cf_atomic64        loadtime;
	cf_atomic64        recs_pending;

	cf_atomic64        n_defrag_records;
	cf_atomic64        defrag_time;
	
	// Query Stats
	histogram *       _query_hist;    // histogram that tracks batch performance
	histogram *       _query_ai_hist; // histgram for getting list of FK
	histogram *       _query_cl_hist; // histogram for looking up records using FK
	//	--aggregation stats
	cf_atomic64        n_aggregation;
	cf_atomic64        agg_response_size;
	cf_atomic64        agg_num_records;
	cf_atomic64        agg_errs;
	//	--lookup stats
	cf_atomic64        n_lookup;
	cf_atomic64        lookup_response_size;
	cf_atomic64        lookup_num_records;
	cf_atomic64        lookup_errs;

	histogram *       _query_rcnt_hist;
	histogram *       _query_diff_hist;
} as_sindex_stat;

/*
 * This structure right now hangs from the namespace structure for the
 * Aerospike Index B-tree.
 */
typedef struct as_sindex_s {
	int                          simatch; //self, shash match by name
	byte                         state;
	uint64_t                     flag;
	struct as_sindex_metadata_s *imd;
	struct as_sindex_metadata_s *new_imd;
	as_namespace                *ns;
	as_sindex_stat               stats;
	// clean-up needed for sindex-config
	as_sindex_config             config; // Secondary index configuration
	// memory management
	cf_atomic_int                data_memory_used;
	uint16_t                     trace_flag;  // tracing flags
	bool                         enable_histogram; // default false;
	cf_atomic_int                desync_cnt;
} as_sindex;

void as_sindex__config_default(as_sindex *si);

typedef struct as_sindex_config_var_s {
	char 		name[AS_ID_INAME_SZ];
	uint64_t    defrag_period;
	uint32_t    defrag_max_units;
	uint64_t    data_max_memory;
	uint16_t    trace_flag;  // tracing flags
	bool        enable_histogram; // default false;
	uint16_t    ignore_not_sync_flag;
	bool 		conf_valid_flag;
}as_sindex_config_var;

/*
 * Hash function that takes a sindex-name and returns a uint64_t hash, 
 * meant for hashing name to a as_sindex_config_var structure. 
 */
static inline uint32_t
as_sindex_config_var_hash_fn(void* p_key)
{
	return (uint32_t)cf_hash_fnv(p_key, strlen((const char *)p_key));
}

void as_sindex_config_var_default(as_sindex_config_var *si_cfg);

int as_sindex_cfg_var_hash_reduce_fn(void *key, void *data, void *udata);

int  as_sindex_create_check_params(as_namespace* ns, as_sindex_metadata* imd);

bool as_sindex_delete_checker(as_namespace *ns, as_sindex_metadata *imd);

struct ai_obj;
typedef struct as_sindex_query_context_s {
	uint64_t         bsize;
	cf_ll            *recl;
	uint64_t         n_bdigs;
		
	// Physical Tree offset
	bool             first;		// If new tree
	int              pimd_idx;

	// IBTR offset
	bool             last;      // If nbtr was finished
								// next iteration starts
								// from key next to bkey
	struct ai_obj   *bkey;      // offset in ibtr

	// NBTR offset
	cf_digest        bdig;
} as_sindex_qctx;

// Tracing Infrastruture 
#define AS_SINDEX_TRACE_LOCK     0x01  // 1
#define AS_SINDEX_TRACE_RESERVE  0x02  // 2
#define AS_SINDEX_TRACE_META     0x04  // 4
#define AS_SINDEX_TRACE_DML      0x08  // 8
#define AS_SINDEX_TRACE_DEFRAG   0x10  // 16
#define AS_SINDEX_TRACE_POPULATE 0x20  // 32
#define AS_SINDEX_TRACE_DUMP     0x40  // 64
#define AS_SINDEX_TRACE_QUERY    0x80  // 128

#define AS_SINDEX_GTRACE_META      0x01  // 1  
#define AS_SINDEX_GTRACE_DUMP      0x02  // 2
#define AS_SINDEX_GTRACE_CALLSTACK 0x04  // 4
#define AS_SINDEX_GTRACE_CALLER    0x08  // 8
#define AS_SINDEX_GTRACE_QUERY     0x10  // 16 
#define AS_SINDEX_GTRACE_UNUSED4   0x20  // 32
#define AS_SINDEX_GTRACE_UNUSED5   0x40  // 64
#define AS_SINDEX_GTRACE_UNUSED6   0x80  // 128

#define GTRACE(mode, type, ...)   \
	cf_ ##type(AS_SINDEX, __VA_ARGS__);        

#define SITRACE(si, mode, type, ...)    \
	cf_ ##type(AS_SINDEX, __VA_ARGS__);  

/* Index iterator Abstraction */
typedef struct as_sindex_iter_s {
	void      *iter;
	as_sindex *si;
} as_sindex_iter;

/*
 * The range structure used to define the lower and upper limit
 * along with the key types. 
 *
 *  [0, endl]
 *  [startl, -1(inf)]
 *  [startl, endl]
 */
typedef struct as_sindex_range_s {
	byte           num_binval;
	bool           isrange;
	as_sindex_bin  start;
	as_sindex_bin  end;
} as_sindex_range;

// TODO: Generalize Where clause form
typedef bool (*as_sindex_cond_fn)(as_namespace *ns, cf_digest *digest);
typedef struct as_sindex_range_udata_s {
	void *values;
	ulong numval;
} as_sindex_range_udata;

typedef struct as_sindex_key_s {
	byte          num_binval;
	as_sindex_bin b[AS_SINDEX_BINMAX];
} as_sindex_key;

extern cf_atomic64 sindex_cntr_delete_failed;
extern cf_atomic64 sindex_cntr_insert_failed;
/* Type for a reduce function to be mapped over all elements in an index. */
typedef int (*as_sindex_reduce_fn)(void *key, void *value);

/* Index abstraction layer functions. */
/*
 * Initialize an instantiation of the index abstraction layer
 * using the array of index type-specific parameters passed in.
 *
 * All indexes created during this instantiation will use these type-specific
 * parameters (e.g., maximum data structure sizes, allocation policies, and any
 * other tuning parameters.)
 *
 * Call once before creating any type of index object.
 */
extern int as_sindex_init(as_namespace *ns);

/*
 * Terminate an instantiation of the index abstraction layer.
 *
 * Do not use any "sindex" functions after calling this function, so free your indexes beforehand.
 */
extern void as_sindex_shutdown(as_namespace *ns);

/* DDL and Metadata Query */
extern int as_sindex_create(as_namespace *ns, as_sindex_metadata *imd, bool user_create);
extern int as_sindex_destroy(as_namespace *ns, as_sindex_metadata *imd);
extern int as_sindex_update(as_sindex_metadata *imd);
extern void as_sindex_destroy_pmetadata(as_sindex *si);
extern int as_sindex_ns_has_sindex(as_namespace *ns);
extern int as_sindex_bin_has_sindex(as_namespace *ns, as_bin *b);

// Info functions
extern int as_sindex_list_str(as_namespace *ns, cf_dyn_buf *db);
extern int as_sindex_describe_str(as_namespace *ns, as_sindex_metadata *imd, cf_dyn_buf *db);
extern int as_sindex_stats_str(as_namespace *ns, as_sindex_metadata *imd, cf_dyn_buf *db);

/* DML */
extern int as_sindex_put(as_sindex *si, as_sindex_key *key, void *val);
extern int as_sindex_put_rd(as_sindex *si, as_storage_rd *rd);
extern int as_sindex_putall_rd(as_namespace *ns, as_storage_rd *rd);
extern int as_sindex_put_by_sbin(as_namespace *ns, const char *set, int numbins, as_sindex_bin *bins, as_storage_rd *rd);

extern int as_sindex_query(as_sindex *si, as_sindex_range *range, as_sindex_qctx *qctx);

extern int as_sindex_delete(as_sindex *si, as_sindex_key *key, void *val);
extern int as_sindex_delete_rd(as_sindex *si, as_storage_rd *rd);
extern int as_sindex_delete_by_sbin(as_namespace *ns, const char *set, int numbins, as_sindex_bin *bins, as_storage_rd *rd);

// Index Metadata Lookup
extern as_sindex *  as_sindex_from_msg(as_namespace *ns, as_msg *msgp); 
extern bool         as_sindex_partition_isactive(as_namespace *ns, cf_digest *digest);
extern as_sindex *  as_sindex_from_range(as_namespace *ns, char *set, as_sindex_range *srange);

/* Misc */
extern int as_sindex_reserve(as_sindex *si, char *fname, int lineno);
extern int as_sindex_release(as_sindex *si, char *fname, int lineno);
extern int as_sindex_imd_free(as_sindex_metadata *imd);
#define AS_SINDEX_RESERVE(si) \
	as_sindex_reserve((si), __FILE__, __LINE__);
#define AS_SINDEX_RELEASE(si) \
	as_sindex_release((si), __FILE__, __LINE__);


//TODO return values is actually enum. 
// Methods for creating secondary index bin array
extern int  as_sindex_sbin_from_op(as_msg_op *op, as_sindex_bin *skey_data, int binid);
extern int  as_sindex_sbin_from_bin(as_namespace *ns, const char *set, as_bin *bin, as_sindex_bin *skey_data);
extern int  as_sindex_sbin_from_rd(as_storage_rd *rd, uint16_t from_bin, uint16_t to_bin, 
										as_sindex_bin delbin[], uint16_t * del_success);
extern bool as_sindex_sbin_match(as_sindex_bin *b1, as_sindex_bin *b2);
extern int  as_sindex_sbin_free(as_sindex_bin *sbin);
extern int  as_sindex_sbin_freeall(as_sindex_bin *sbin, int numval);

extern int  as_sindex_range_free(as_sindex_range **srange);
extern int  as_sindex_rangep_from_msg(as_namespace *ns, as_msg *msgp, as_sindex_range **srange);
extern int  as_sindex_range_from_msg(as_namespace *ns, as_msg *msgp, as_sindex_range *srange);

extern int  as_sindex_populate_done(as_sindex *si);
extern int  as_sindex_boot_populateall_done(as_namespace *ns);
extern int  as_sindex_boot_populateall();
 
/* Some random number collect stats to come at this number */
extern int          as_sindex_remove_partition(as_namespace *ns, int partition_id, int batchsize);
extern const char * as_sindex_err_str(int err_code);
extern int          as_sindex_err_to_clienterr(int err, char *fname, int lineno);

extern as_sindex_gc_status  as_sindex_can_defrag_record(as_namespace *ns, cf_digest *keyd);
extern bool                 as_sindex_isactive(as_sindex *si);
extern uint64_t             as_sindex_memsize(as_namespace *ns, char *set, char *iname);
extern int                  as_sindex_assert_query(as_sindex *si, as_sindex_range *srange);
extern int          as_sindex_histogram_enable(as_namespace *ns, as_sindex_metadata *imd, bool enable);
extern int          as_sindex_trace_op(as_namespace *ns, as_sindex_metadata *imd, int trace);
extern int          as_sindex_reinit(char *name, char *params, cf_dyn_buf *db);
extern int          as_sindex_repair(as_namespace *ns, as_sindex_metadata *imd);
extern int          as_sindex_get_err(int op_code, char *filename, int lineno);


// SINDEX LOCK MACROS
extern pthread_rwlock_t g_sindex_rwlock;
#define SINDEX_GRLOCK()         \
do { \
	int ret = pthread_rwlock_rdlock(&g_sindex_rwlock); \
	if (ret) cf_warning(AS_SINDEX, "GRLOCK(%d) %s:%d",ret, __FILE__, __LINE__); \
} while (0);

#define SINDEX_GWLOCK()         \
do { \
	int ret = pthread_rwlock_wrlock(&g_sindex_rwlock); \
	if (ret) cf_warning(AS_SINDEX, "GWLOCK(%d) %s:%d", ret, __FILE__, __LINE__); \
} while (0);

#define SINDEX_GUNLOCK()        \
do { \
	int ret = pthread_rwlock_unlock(&g_sindex_rwlock); \
	if (ret) cf_warning(AS_SINDEX, "GUNLOCK (%d) %s:%d",ret,  __FILE__, __LINE__); \
} while (0);

#define SINDEX_RLOCK(l)          \
do {                                            \
	int ret = pthread_rwlock_rdlock((l));        \
	if (ret) cf_warning(AS_SINDEX, "RLOCK_ONLY (%d) %s:%d", ret, __FILE__, __LINE__); \
} while(0);

#define SINDEX_WLOCK(l)                       \
do {                                            \
	int ret = pthread_rwlock_wrlock((l));        \
	if (ret) cf_warning(AS_SINDEX, "WLOCK_ONLY (%d) %s:%d",ret, __FILE__, __LINE__); \
} while(0);
#define SINDEX_UNLOCK(l)                      \
do {                                            \
	int ret = pthread_rwlock_unlock((l));        \
	if (ret) cf_warning(AS_SINDEX, "UNLOCK_ONLY (%d) %s:%d",ret, __FILE__, __LINE__); \
} while(0);

#define SINDEX_HIST_INSERT_DATA_POINT(si, type, start_time)        \
do {                                                               \
	if (si->enable_histogram)                                      \
		if (si->stats._ ##type) histogram_insert_data_point(si->stats._ ##type, start_time); \
} while(0);

#define SINDEX_HIST_INSERT_DELTA(si, type, delta_time)        \
do {                                                               \
	if (si->enable_histogram)                                      \
		if (si->stats._ ##type) histogram_insert_delta(si->stats._ ##type, delta_time); \
} while(0);

// Opaque type definition.
struct as_config_s;

// QUERY QUERY QUERY
extern void  as_query_init();
extern int   as_query_reinit(int set_size, int *actual_size);
extern int   as_query_worker_reinit(int set_size, int *actual_size);
extern bool  as_sindex_reserve_data_memory(as_sindex_metadata *imd, uint64_t bytes);
extern bool  as_sindex_release_data_memory(as_sindex_metadata *imd, uint64_t bytes);
extern int   as_sindex_histogram_dumpall(as_namespace *ns);
extern int   as_sindex_set_config(as_namespace *ns, as_sindex_metadata *imd, char *params);
extern void  as_sindex_gconfig_default(struct as_config_s *c);
extern int   as_query_stat(char *name, cf_dyn_buf *db);
extern int   as_query_list(char *name, cf_dyn_buf *db);
extern int   as_query_kill(uint64_t trid);
extern void  as_query_gconfig_default(struct as_config_s *c);

#define AS_QUERY_OK        AS_SINDEX_OK
#define AS_QUERY_ERR       AS_SINDEX_ERR
#define AS_QUERY_CONTINUE  AS_SINDEX_CONTINUE
#define AS_QUERY_DONE      AS_SINDEX_DONE

extern as_mon_jobstat     * as_query_get_jobstat(uint64_t trid);
extern as_mon_jobstat     * as_query_get_jobstat_all(int * size);
extern int as_query_set_priority(uint64_t trid, uint32_t priority);

// Object Index API
extern int as_sindex__op_by_skey(as_sindex   *si, as_sindex_key *skey,
                                 as_storage_rd *rd, as_sindex_op op);

// SMD Integration related functions
extern int as_sindex_smd_accept_cb(char *module, as_smd_item_list_t *items, 
								   void *udata, uint32_t bitmap);
extern int as_sindex_smd_merge_cb(char *module, as_smd_item_list_t **item_list_out,
								  as_smd_item_list_t **item_lists_in, size_t num_lists,
								  void *udata);

extern int as_sindex_smd_can_accept_cb(char* module, as_smd_item_t *item, 
									   void *udata);
extern uint64_t as_sindex_get_ns_memory_used(as_namespace *ns);
