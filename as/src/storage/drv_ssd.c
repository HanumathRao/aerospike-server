/*
 * drv_ssd.c
 *
 * Copyright (C) 2009-2014 Aerospike, Inc.
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

/* SYNOPSIS
 * "file" based storage driver, which applies to both SSD namespaces and, in
 * some cases, to file-backed main-memory namespaces.
 */

#include "base/feature.h" // turn new AS Features on/off (must be first in line)

#include "storage/drv_ssd.h"

// TODO - We have a #include loop - datamodel.h and storage.h include each
// other. I'd love to untangle this mess, but can't right now. So this needs to
// be here to allow compilation for now:
#include "base/datamodel.h"

#include "storage/storage.h"

#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <linux/fs.h> // for BLKGETSIZE64
#include <sys/ioctl.h>
#include <sys/param.h> // for MAX()

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_random.h"

#include "clock.h"
#include "fault.h"
#include "hist.h"
#include "jem.h"
#include "queue.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/cfg.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/rec_props.h"
#include "base/secondary_index.h"


//==========================================================
// Forward declarations.
//

// Defined in thr_nsup.c, for historical reasons.
extern bool as_cold_start_evict_if_needed(as_namespace* ns);


//==========================================================
// Constants.
//

#define MAX_WRITE_BLOCK_SIZE	(1024 * 1024)
#define LOAD_BUF_SIZE			MAX_WRITE_BLOCK_SIZE // must be multiple of MAX_WRITE_BLOCK_SIZE

// We round usable device/file size down to SSD_DEFAULT_HEADER_LENGTH plus a
// multiple of LOAD_BUF_SIZE. If we ever change SSD_DEFAULT_HEADER_LENGTH we
// may break backward compatibility since an old header with different size
// could render the rounding ineffective. (There are backward compatibility
// issues anyway if we think we need to change SSD_DEFAULT_HEADER_LENGTH...)
#define SSD_DEFAULT_HEADER_LENGTH	(1024 * 1024)
#define SSD_DEFAULT_INFO_NUMBER		(1024 * 4)
#define SSD_DEFAULT_INFO_LENGTH		(128)

#define SSD_BLOCK_MAGIC		0x037AF200
#define SIGNATURE_OFFSET	offsetof(struct drv_ssd_block_s, keyd)

// Write-smoothing constants.
#define MIN_SECONDS_OF_WRITE_SMOOTHING_DATA		5
#define NUM_ELEMS_IN_LBW_CATCH_UP_CALC			5


//==========================================================
// Typedefs.
//

// Info slice in device header block.
typedef struct {
	uint32_t	len;
	uint8_t		data[];
} info_buf;


//------------------------------------------------
// Per-record metadata on device.
//
typedef struct drv_ssd_block_s {
	cf_signature	sig;			// digest of this entire block, 64 bits
	uint32_t		magic;
	uint32_t		length;			// total under signature - starts after this field - pointer + 16
	cf_digest		keyd;
	as_generation	generation;
	cf_clock		void_time;
	uint32_t		bins_offset;	// offset to bins from data
	uint32_t		n_bins;
	uint32_t		vinfo_offset;
	uint32_t		vinfo_length;
	uint8_t			data[];
} __attribute__ ((__packed__)) drv_ssd_block;


//------------------------------------------------
// Per-bin metadata on device.
//
typedef struct drv_ssd_bin_s {
	char		name[AS_ID_BIN_SZ];	// 15 aligns well
	uint8_t		version;
	uint32_t	offset;				// offset of bin data within block
	uint32_t	len;				// size of bin data
	uint32_t	next;				// location of next bin: block offset
} __attribute__ ((__packed__)) drv_ssd_bin;


//==========================================================
// Miscellaneous utility functions.
//

// Get an open file descriptor from the pool, or a fresh one if necessary.
int
ssd_fd_get(drv_ssd *ssd)
{
	int fd = -1;
	int rv = cf_queue_pop(ssd->fd_q, (void*)&fd, CF_QUEUE_NOWAIT);

	if (rv != CF_QUEUE_OK) {
		fd = open(ssd->name, ssd->open_flag, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_crash(AS_DRV_SSD, "unable to open file %s: %s",
					ssd->name, cf_strerror(errno));
		}
	}

	return fd;
}


// Save an open file descriptor in the pool
static inline void
ssd_fd_put(drv_ssd *ssd, int fd)
{
	cf_queue_push(ssd->fd_q, (void*)&fd);
}


// Decide which device a record belongs on.
static inline int
ssd_get_file_id(drv_ssds *ssds, cf_digest *keyd)
{
	return keyd->digest[DIGEST_STORAGE_BYTE] % ssds->n_ssds;
}


// Put a wblock on the free queue for reuse.
void
push_wblock_to_queue(ssd_alloc_table *at, uint32_t wblock_id, e_free_to free_to)
{
	// temp debugging:
	if (wblock_id >= at->n_wblocks) {
		cf_warning(AS_DRV_SSD, "pushing invalid wblock_id %d to free_wblock_q",
				(int32_t)wblock_id);
		return;
	}

	if (free_to == FREE_TO_HEAD) {
		cf_queue_push_head(at->free_wblock_q, &wblock_id);
	}
	else {
		cf_queue_push(at->free_wblock_q, &wblock_id);
	}
}


//------------------------------------------------
// ssd_write_buf "swb" methods.
//

static inline ssd_write_buf*
swb_create(drv_ssd *ssd)
{
	ssd_write_buf *swb = (ssd_write_buf*)cf_malloc(sizeof(ssd_write_buf));

	if (! swb) {
		cf_warning(AS_DRV_SSD, "device %s - swb malloc failed", ssd->name);
		return NULL;
	}

	swb->buf = cf_valloc(ssd->write_block_size);

	if (! swb->buf) {
		cf_warning(AS_DRV_SSD, "device %s - swb buf valloc failed", ssd->name);
		cf_free(swb);
		return NULL;
	}

	return swb;
}

static inline void
swb_destroy(ssd_write_buf *swb)
{
	cf_free(swb->buf);
	cf_free(swb);
}

static inline void
swb_reset(ssd_write_buf *swb)
{
	swb->wblock_id = STORAGE_INVALID_WBLOCK;
	swb->pos = 0;
}

#define swb_reserve(_swb) cf_atomic32_incr(&(_swb)->rc)

static inline void
swb_check_and_reserve(ssd_wblock_state *wblock_state, ssd_write_buf **p_swb)
{
	pthread_mutex_lock(&wblock_state->LOCK);

	if (wblock_state->swb) {
		*p_swb = wblock_state->swb;
		swb_reserve(*p_swb);
	}

	pthread_mutex_unlock(&wblock_state->LOCK);
}

static inline void
swb_release(ssd_write_buf *swb)
{
	if (0 == cf_atomic32_decr(&swb->rc)) {
		swb_reset(swb);

		// Put the swb back on the free queue for reuse.
		cf_queue_push(swb->ssd->swb_free_q, &swb);
	}
}

static inline void
swb_dereference_and_release(ssd_alloc_table *at, uint32_t wblock_id,
		ssd_write_buf *swb)
{
	ssd_wblock_state *wblock_state = &at->wblock_state[wblock_id];

	pthread_mutex_lock(&wblock_state->LOCK);

	if (swb != wblock_state->swb) {
		cf_warning(AS_DRV_SSD, "releasing wrong swb! %p (%d) != %p (%d), thread %d",
			swb, (int32_t)swb->wblock_id, wblock_state->swb,
			(int32_t)wblock_state->swb->wblock_id, pthread_self());
	}

	swb_release(wblock_state->swb);
	wblock_state->swb = 0;

	// Free wblock if all three gating conditions hold.
	if (cf_atomic32_get(wblock_state->inuse_sz) == 0 &&
			wblock_state->state != WBLOCK_STATE_DEFRAG) {
		push_wblock_to_queue(at, wblock_id, FREE_TO_HEAD);
	}

	pthread_mutex_unlock(&wblock_state->LOCK);
}

ssd_write_buf *
swb_get(drv_ssd *ssd)
{
	ssd_write_buf *swb;

	if (CF_QUEUE_OK != cf_queue_pop(ssd->swb_free_q, &swb, CF_QUEUE_NOWAIT)) {
		if (! (swb = swb_create(ssd))) {
			return NULL;
		}

		swb->ssd = ssd;
		swb->wblock_id = STORAGE_INVALID_WBLOCK;
		swb->pos = 0;
		swb->rc = 0;
	}

	// Find a device block to write to.
	if (CF_QUEUE_OK != cf_queue_pop(ssd->alloc_table->free_wblock_q,
			&swb->wblock_id, CF_QUEUE_NOWAIT)) {
		cf_queue_push(ssd->swb_free_q, &swb);
		return NULL;
	}

	ssd_wblock_state* p_wblock_state =
			&ssd->alloc_table->wblock_state[swb->wblock_id];

	// Sanity checks.
	if (cf_atomic32_get(p_wblock_state->inuse_sz) != 0) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u inuse-size %u off free-q",
				ssd->name, swb->wblock_id,
				cf_atomic32_get(p_wblock_state->inuse_sz));
	}
	if (p_wblock_state->swb) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u swb not null off free-q",
				ssd->name, swb->wblock_id);
	}
	if (p_wblock_state->state != WBLOCK_STATE_NONE) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u state not DEFRAG off free-q",
				ssd->name, swb->wblock_id);
	}

	pthread_mutex_lock(&p_wblock_state->LOCK);

	cf_atomic32_set(&p_wblock_state->inuse_sz, ssd->write_block_size);
	cf_atomic64_add(&ssd->inuse_size, ssd->write_block_size);

	swb_reserve(swb);
	p_wblock_state->swb = swb;

	pthread_mutex_unlock(&p_wblock_state->LOCK);

	return swb;
}

//
// END - ssd_write_buf "swb" methods.
//------------------------------------------------


// Reduce wblock's used size, if result is 0 put it in the "free" pool.
void
ssd_block_free(drv_ssd *ssd, uint64_t rblock_id, uint64_t n_rblocks,
		e_free_to free_to, char *msg)
{
	if (n_rblocks == 0) {
		return;
	}

	// Determine which wblock(s) we're reducing used size in.
	uint64_t start_byte = RBLOCKS_TO_BYTES(rblock_id);
	uint64_t sz_bytes = RBLOCKS_TO_BYTES(n_rblocks);
	uint32_t start_wblock_id = BYTES_TO_WBLOCK_ID(ssd, start_byte);
	uint32_t end_wblock_id = BYTES_TO_WBLOCK_ID(ssd, start_byte + sz_bytes - 1);

	// TODO - move to after the sanity checks?
	cf_atomic64_sub(&ssd->inuse_size, sz_bytes);

	ssd_alloc_table *at = ssd->alloc_table;

	// Sanity-check start and end.
	if (start_wblock_id >= at->n_wblocks) {
		cf_warning(AS_DRV_SSD, "ssd_block_free: %s: internal error: rblock_id %lu too large (start byte %"PRIu64,
			msg, rblock_id, start_byte);
		return;
	}
	if (end_wblock_id > at->n_wblocks) {
		cf_warning(AS_DRV_SSD, "ssd_block_free: %s: internal error: block length %lu too large (end byte %"PRIu64,
			msg, n_rblocks, start_byte + sz_bytes);
		return;
	}

	// All in one wblock, the very normal case.
	if (start_wblock_id == end_wblock_id) {
		ssd_wblock_state *p_wblock_state = &at->wblock_state[start_wblock_id];

		pthread_mutex_lock(&p_wblock_state->LOCK);

		int64_t resulting_inuse_sz = cf_atomic32_sub(
				&at->wblock_state[start_wblock_id].inuse_sz, (int32_t)sz_bytes);

		if (resulting_inuse_sz < 0) {
			cf_warning(AS_DRV_SSD, "%s %s ssd wblock1 %d over-freed, subtracted %d now %ld",
					ssd->name, msg, start_wblock_id, sz_bytes,
					resulting_inuse_sz);

			cf_atomic32_set(&at->wblock_state[start_wblock_id].inuse_sz,
					ssd->write_block_size); // is this the best thing to do ???
		}
		else if (resulting_inuse_sz == 0) {
			// Free wblock if all three gating conditions hold.
			if (! p_wblock_state->swb &&
					p_wblock_state->state != WBLOCK_STATE_DEFRAG) {
				push_wblock_to_queue(at, start_wblock_id, free_to);
			}
		}

		pthread_mutex_unlock(&p_wblock_state->LOCK);
	}
	// More than one wblock - only happens at startup.
	else {
		// Do the first partial.
		uint32_t f_partial_sz = ssd->write_block_size -
				(start_byte % ssd->write_block_size);
		uint32_t wblock_id = start_wblock_id;

		cf_atomic32_sub(&at->wblock_state[wblock_id].inuse_sz,
				(int32_t)f_partial_sz);

		if ((int32_t)cf_atomic32_get(at->wblock_state[wblock_id].inuse_sz) < 0) {
			cf_warning(AS_DRV_SSD, "%s %s: ssd wblock2 %d over-freed, subtracted %d now %d, %s",
					ssd->name, msg, wblock_id, sz_bytes,
					cf_atomic32_get(at->wblock_state[wblock_id].inuse_sz));

			cf_atomic32_set(&at->wblock_state[start_wblock_id].inuse_sz,
					ssd->write_block_size); // is this the best thing to do ???
		}

		if (cf_atomic32_get(at->wblock_state[wblock_id].inuse_sz) == 0) {
			push_wblock_to_queue(at, wblock_id, free_to);
		}

		start_byte += f_partial_sz;
		sz_bytes -= f_partial_sz;
		wblock_id++;

		// Do all the others - all but the last will be full.
		while (sz_bytes) {
			if (sz_bytes > ssd->write_block_size) {
				cf_atomic32_set(&at->wblock_state[wblock_id].inuse_sz, 0);
				push_wblock_to_queue(at, wblock_id, free_to);

				start_byte += ssd->write_block_size;
				sz_bytes -= ssd->write_block_size;
				wblock_id++;
			}
			else {
				cf_atomic32_sub(&at->wblock_state[wblock_id].inuse_sz,
						(int32_t)sz_bytes);

				if ((int32_t)cf_atomic32_get(at->wblock_state[wblock_id].inuse_sz) < 0) {
					cf_warning(AS_DRV_SSD, "%s %s: ssd wblock3 %d over-freed, subtracted %d, now %d",
							ssd->name, msg, wblock_id, sz_bytes,
							cf_atomic32_get(at->wblock_state[wblock_id].inuse_sz));

					cf_atomic32_set(&at->wblock_state[start_wblock_id].inuse_sz,
							ssd->write_block_size); // is this the best thing to do ???
				}

				if (cf_atomic32_get(at->wblock_state[wblock_id].inuse_sz) == 0) {
					push_wblock_to_queue(at, wblock_id, free_to);
				}

				start_byte += sz_bytes;
				sz_bytes = 0;
			}
		}
	}
}


static void
log_bad_record(const char* ns_name, uint32_t n_bins, uint32_t block_bins,
		const drv_ssd_bin* ssd_bin, const char* tag)
{
	cf_info(AS_DRV_SSD, "untrustworthy data from disk [%s], ignoring record", tag);
	cf_info(AS_DRV_SSD, "   ns->name = %s", ns_name);
	cf_info(AS_DRV_SSD, "   bin %" PRIu32 " [of %" PRIu32 "]", (block_bins - n_bins) + 1, block_bins);

	if (ssd_bin) {
		cf_info(AS_DRV_SSD, "   ssd_bin->version = %" PRIu32, (uint32_t)ssd_bin->version);
		cf_info(AS_DRV_SSD, "   ssd_bin->offset = %" PRIu32, ssd_bin->offset);
		cf_info(AS_DRV_SSD, "   ssd_bin->len = %" PRIu32, ssd_bin->len);
		cf_info(AS_DRV_SSD, "   ssd_bin->next = %" PRIu32, ssd_bin->next);
	}
}


static bool
is_valid_record(const drv_ssd_block* block, const char* ns_name)
{
	uint8_t* block_head = (uint8_t*)block;
	uint64_t size = (uint64_t)(block->length + SIGNATURE_OFFSET);
	drv_ssd_bin* ssd_bin_end = (drv_ssd_bin*)(block_head + size - sizeof(drv_ssd_bin));
	drv_ssd_bin* ssd_bin = (drv_ssd_bin*)(block->data + block->bins_offset);
	uint32_t n_bins = block->n_bins;

	if (n_bins == 0 || n_bins > BIN_NAMES_QUOTA) {
		log_bad_record(ns_name, n_bins, n_bins, NULL, "bins");
		return false;
	}

	while (n_bins > 0) {
		if (ssd_bin > ssd_bin_end) {
			log_bad_record(ns_name, n_bins, block->n_bins, NULL, "bin ptr");
			return false;
		}

		if (ssd_bin->version >= 16) {
			log_bad_record(ns_name, n_bins, block->n_bins, ssd_bin, "version");
			return false;
		}

		uint64_t data_offset = (uint64_t)((uint8_t*)(ssd_bin + 1) - block_head);

		if ((uint64_t)ssd_bin->offset != data_offset) {
			log_bad_record(ns_name, n_bins, block->n_bins, ssd_bin, "offset");
			return false;
		}

		uint64_t bin_end_offset = data_offset + (uint64_t)ssd_bin->len;

		if (bin_end_offset > size) {
			log_bad_record(ns_name, n_bins, block->n_bins, ssd_bin, "length");
			return false;
		}

		if (n_bins > 1) {
			if ((uint64_t)ssd_bin->next != bin_end_offset) {
				log_bad_record(ns_name, n_bins, block->n_bins, ssd_bin, "next ptr");
				return false;
			}

			ssd_bin = (drv_ssd_bin*)(block_head + ssd_bin->next);
		}

		n_bins--;
	}

	return true;
}


int
ssd_record_defrag(drv_ssds *ssds, drv_ssd *ssd, drv_ssd_block *block,
		uint64_t rblock_id, uint32_t n_rblocks, uint64_t filepos)
{
	as_partition_reservation rsv;
	as_partition_id pid = as_partition_getid(block->keyd);

	if (! is_valid_record(block, ssds->ns->name)) {
		return -3;
	}

	// Fix one bit of information: 0 generations coming off device.
	if (block->generation == 0) {
		block->generation = 1;
	}

	as_partition_reserve_migrate(ssds->ns, pid, &rsv, 0);
	cf_atomic_int_incr(&g_config.ssdr_tree_count);

	int rv;
	as_index_ref r_ref;
	r_ref.skip_lock = false;

	if ((0 == as_record_get(rsv.tree, &block->keyd, &r_ref, rsv.ns))
		|| (rsv.ns->ldt_enabled
				&& (0 == as_record_get(rsv.sub_tree, &block->keyd, &r_ref, rsv.ns)))) {
		as_index *r = r_ref.r;

		if (r->storage_key.ssd.file_id == ssd->file_id &&
				r->storage_key.ssd.rblock_id == rblock_id) {
			if (r->generation != block->generation) {
				cf_warning(AS_DRV_SSD, "defrag: block points here but generation different (%d:%d), surprising",
						r->generation, block->generation);
			}

			if (r->storage_key.ssd.n_rblocks != n_rblocks) {
				cf_warning(AS_DRV_SSD, "device %s defrag key %lx rblock-id %lu had mismatched n_blocks: %u, %u",
						ssd->name, rblock_id, *(uint64_t*)&r->key,
						r->storage_key.ssd.n_rblocks, n_rblocks);
			}

			as_storage_rd rd;
			as_storage_record_open(ssds->ns, r, &rd, &block->keyd);

			as_index_vinfo_mask_set(r,
					as_partition_vinfoset_mask_unpickle(rsv.p,
							block->data + block->vinfo_offset,
							block->vinfo_length),
					ssds->ns->allow_versions);

			rd.u.ssd.block = block;
			rd.have_device_block = true;

			rd.n_bins = as_bin_get_n_bins(r, &rd);
			as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];

			rd.bins = as_bin_get_all(r, &rd, stack_bins);

			as_storage_record_get_key(&rd);

			size_t rec_props_data_size = as_storage_record_rec_props_size(&rd);
			uint8_t rec_props_data[rec_props_data_size];

			if (rec_props_data_size > 0) {
				as_storage_record_set_rec_props(&rd, rec_props_data);
			}

			rd.write_to_device = true;

			uint64_t start_ms = 0;
			if (g_config.microbenchmarks) {
				start_ms = cf_getms();
			}

			as_storage_record_close(r, &rd);

			if (g_config.microbenchmarks && start_ms) {
				histogram_insert_data_point(g_config.defrag_storage_close_hist, start_ms);
			}

			rv = 0; // record was in index tree and current - moved it
		}
		else {
			rv = -1; // record was in index tree - presumably was overwritten
		}

		as_record_done(&r_ref, ssds->ns);
	}
	else {
		rv = -2; // record was not in index tree - presumably was deleted
	}

	as_partition_release(&rsv);
	cf_atomic_int_decr(&g_config.ssdr_tree_count);

	return rv;
}


int
ssd_defrag_wblock(drv_ssds *ssds, drv_ssd *ssd, uint32_t wblock_id)
{
	if (! as_storage_has_space_ssd(ssds->ns)) {
		cf_warning(AS_DRV_SSD, "{%s}: defrag: drives full", ssds->ns->name);
		return 0;
	}

	int record_count = 0;
	int num_old_records = 0;
	int num_deleted_records = 0;
	int record_err_count = 0;
	int fd = -1;
	uint8_t *read_buf = NULL;

	ssd_wblock_state* p_wblock_state = &ssd->alloc_table->wblock_state[wblock_id];

	if (cf_atomic32_get(p_wblock_state->inuse_sz) == 0) {
		goto Finished;
	}

	fd = ssd_fd_get(ssd);

	if (-1 == fd) {
		cf_warning(AS_DRV_SSD, "defrag: unable to get file descriptor for device %s errno %d",
				ssd->name, errno);
		goto Finished;
	}

	uint64_t start_time = 0;

	if (g_config.storage_benchmarks) {
		start_time = cf_getms();
	}

	off_t file_offset = lseek(fd, WBLOCK_ID_TO_BYTES(ssd, wblock_id), SEEK_SET);

	if (file_offset != WBLOCK_ID_TO_BYTES(ssd, wblock_id)) {
		cf_warning(AS_DRV_SSD, "DEVICE FAILED: device %s can't seek errno %d",
				ssd->name, errno);
		close(fd);
		fd = -1;
		goto Finished;
	}

	read_buf = cf_valloc(ssd->write_block_size);

	if (! read_buf) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u defrag valloc failed",
				ssd->name, wblock_id);
		goto Finished;
	}

	ssize_t rlen = read(fd, read_buf, ssd->write_block_size);

	if (rlen != ssd->write_block_size) {
		cf_info(AS_DRV_SSD, "defrag read failed: offset %"PRIu64" errno %d rv %zd",
				file_offset, errno, rlen);
		close(fd);
		fd = -1;
		goto Finished;
	}

	if (g_config.storage_benchmarks && start_time) {
		histogram_insert_data_point(ssd->hist_large_block_read, start_time);
	}

	size_t wblock_offset = 0; // current offset within the wblock, in bytes

	while (wblock_offset < ssd->write_block_size &&
			cf_atomic32_get(p_wblock_state->inuse_sz) != 0) {
		drv_ssd_block *block = (drv_ssd_block*)&read_buf[wblock_offset];

		if (block->magic != SSD_BLOCK_MAGIC) {
			// First block must have magic.
			if (wblock_offset == 0) {
				cf_warning(AS_DRV_SSD, "BLOCK CORRUPTED: device %s has bad data on wblock %d",
						ssd->name, wblock_id);
				break;
			}

			// Later blocks may have no magic, just skip to next block.
			wblock_offset += RBLOCK_SIZE;
			continue;
		}

		// Note - if block->length is sane, we don't need to round up to a
		// multiple of RBLOCK_SIZE, but let's do it anyway just to be safe.
		size_t next_wblock_offset = wblock_offset +
				BYTES_TO_RBLOCK_BYTES(block->length + SIGNATURE_OFFSET);

		if (next_wblock_offset > ssd->write_block_size) {
			cf_warning(AS_DRV_SSD, "error: block extends over read size: foff %"PRIu64" boff %"PRIu64" blen %"PRIu64,
				file_offset, wblock_offset, (uint64_t)block->length);
			break;
		}

		if (ssd->use_signature && block->sig) {
			cf_signature sig;

			cf_signature_compute(((uint8_t*)block) + SIGNATURE_OFFSET,
					block->length, &sig);

			if (sig != block->sig) {
				wblock_offset += RBLOCK_SIZE;
				ssd->record_add_sigfail_counter++;
				continue;
			}
		}

		// Found a good record, move it if it's current.
		int rv = ssd_record_defrag(ssds, ssd, block,
				BYTES_TO_RBLOCKS(file_offset + wblock_offset),
				(uint32_t)BYTES_TO_RBLOCKS(next_wblock_offset - wblock_offset),
				file_offset + wblock_offset);

		if (rv == 0) {
			record_count++;
		}
		else if (rv == -1) {
			num_old_records++;
		}
		else if (rv == -2) {
			num_deleted_records++;
		}
		else if (rv == -3) {
			cf_atomic_int_incr(&g_config.err_storage_defrag_corrupt_record);
			record_err_count++;
		}

		wblock_offset = next_wblock_offset;
	}

Finished:

	if (fd != -1) {
		ssd_fd_put(ssd, fd);
	}

	if (read_buf) {
		cf_free(read_buf);
	}

	// Note - usually wblock's inuse_sz is 0 here, but may legitimately be non-0
	// e.g. if a dropped partition's tree is not done purging. In this case, we
	// may have found deleted records in the wblock whose used-size contribution
	// has not yet been subtracted.

	if (record_err_count > 0) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u defragged, final in-use-sz %d records (%d:%d:%d:%d)",
				ssd->name, wblock_id, cf_atomic32_get(p_wblock_state->inuse_sz),
				record_count, num_old_records, num_deleted_records,
				record_err_count);
	}
	else {
		cf_detail(AS_DRV_SSD, "device %s: wblock-id %u defragged, final in-use-sz %d records (%d:%d:%d)",
				ssd->name, wblock_id, cf_atomic32_get(p_wblock_state->inuse_sz),
				record_count, num_old_records, num_deleted_records);
	}

	// Sanity checks.
	if (p_wblock_state->swb) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u swb not null while defragging",
				ssd->name, wblock_id);
	}
	if (p_wblock_state->state != WBLOCK_STATE_DEFRAG) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u state not DEFRAG while defragging",
				ssd->name, wblock_id);
	}

	pthread_mutex_lock(&p_wblock_state->LOCK);

	p_wblock_state->state = WBLOCK_STATE_NONE;

	// Free the wblock if it's empty.
	if (cf_atomic32_get(p_wblock_state->inuse_sz) == 0 &&
			! p_wblock_state->swb) {
		push_wblock_to_queue(ssd->alloc_table, wblock_id, FREE_TO_HEAD);
	}

	pthread_mutex_unlock(&p_wblock_state->LOCK);

	return record_count;
}


// TODO - really don't need ssds - deprecate this struct.
typedef struct {
	drv_ssds *ssds;
	drv_ssd *ssd;
} ssd_defrag_devices_data;

void *
ssd_defrag_worker_fn(void *udata)
{
	ssd_defrag_devices_data *ddd = (ssd_defrag_devices_data*)udata;
	drv_ssd *ssd = ddd->ssd;
	drv_ssds *ssds = ddd->ssds;

	cf_free(ddd);
	ddd = 0;

	uint64_t last_time = cf_get_seconds();
	uint64_t last_log_time = last_time;
	int curr_pos = -1;

	while (true) {
		// Wake up every 1 second to check if the period is complete.
		struct timespec delay = { 1, 0 };
		nanosleep(&delay, NULL);

		uint64_t curr_time = cf_get_seconds();

		if ((curr_time - last_time) < ssds->ns->storage_defrag_period) {
			// Period has not been reached.
			continue;
		}

		cf_detail(AS_DRV_SSD, "%s defrag start", ssd->name);

		last_time = curr_time;
		uint32_t defrag_max_wblocks = ssds->ns->storage_defrag_max_blocks;
		uint32_t defrag_lwm_pct = ssds->ns->storage_defrag_lwm_pct;
		uint32_t *free_wblocks = cf_malloc(sizeof(uint32_t)*defrag_max_wblocks);
		uint32_t found_wblocks = 0;
		uint32_t lwm_size = (ssd->write_block_size * defrag_lwm_pct) / 100;

		ssd_alloc_table *at = ssd->alloc_table;
		cf_clock start_time = cf_getms();

		// Loop over all blocks, starting from where we left off and wrapping
		// around if necessary, collecting blocks to defrag until either we do a
		// full lap or we collect the maximum allowed per lap (default 4000).
		for (int i = 0; i < at->n_wblocks; i++) {
			curr_pos = (curr_pos + 1) % at->n_wblocks; // clock algorithm

			ssd_wblock_state* p_wblock_state = &at->wblock_state[curr_pos];
			pthread_mutex_lock(&p_wblock_state->LOCK);

			uint32_t inuse_sz_curr = cf_atomic32_get(p_wblock_state->inuse_sz);

			// Look for partially used block with occupancy less than threshold.
			if (inuse_sz_curr <= lwm_size && (int32_t)inuse_sz_curr > 0 &&
					p_wblock_state->swb == 0) {
				// Sanity check:
				if (p_wblock_state->state != WBLOCK_STATE_NONE) {
					cf_warning(AS_DRV_SSD, "device %s: wblock-id %u state not NONE starting defrag",
							ssd->name, curr_pos);
				}

				p_wblock_state->state = WBLOCK_STATE_DEFRAG;
				free_wblocks[found_wblocks++] = curr_pos;

				if (found_wblocks == defrag_max_wblocks) {
					pthread_mutex_unlock(&p_wblock_state->LOCK);
					break;
				}
			}

			pthread_mutex_unlock(&p_wblock_state->LOCK);
		}

		uint64_t lock_time = cf_getms() - start_time;

		int n_waits = 0;
		int num_records = 0;

		// Defrag all the blocks we collected in the loop above, with various
		// configurable throttling applied.
		for (int i = 0; i < found_wblocks; i++) {
			if (g_config.defrag_queue_priority) {
				usleep(g_config.defrag_queue_priority * 1000);
			}

			int escape = 0;

			if (cf_queue_sz(ssd->swb_write_q) > g_config.defrag_queue_hwm) {
				while (cf_queue_sz(ssd->swb_write_q) > g_config.defrag_queue_lwm) {
					if (escape++ >= g_config.defrag_queue_escape) {
						cf_atomic_int_incr(&g_config.stat_storage_defrag_wait);
						break;
					}

					n_waits++;
					usleep(1000);
				}
			}

			num_records += ssd_defrag_wblock(ssds, ssd, free_wblocks[i]);
		}

		cf_free(free_wblocks);

		uint64_t elapsed_time = cf_getms() - start_time;

		// Only log defrag stats if we're maxed out or it's been a while.
		if (found_wblocks == defrag_max_wblocks ||
				curr_time - last_log_time > g_config.ticker_interval) {
			last_log_time = curr_time;

			cf_info(AS_DRV_SSD, "%s defrag curr_pos %d wblocks:%u recs:%d waits:%d lock-time:%lu ms total-time:%lu ms",
					ssd->name, curr_pos, found_wblocks, num_records, n_waits,
					lock_time, elapsed_time);
		}
	}

	return NULL;
}


void
ssd_start_defrag_threads(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "ns %s starting defrag threads", ssds->ns->name);

	for (uint i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];
		cf_info(AS_DRV_SSD, "%s defrag thread started", ssd->name);

		ssd_defrag_devices_data *ddd = cf_malloc(sizeof(ssd_defrag_devices_data));
		if (!ddd) {
			cf_crash(AS_DRV_SSD, "memory allocation in defrag thread dispatch");
		}

		ddd->ssds = ssds;
		ddd->ssd = ssd;
		pthread_create(&ssd->defrag_thread, 0, ssd_defrag_worker_fn, ddd);
	}
}


void
ssd_block_get_free_size(drv_ssd *ssd, off_t *total_r, off_t *contiguous_r)
{
	if (total_r) {
		// ssd->inuse_size does not include device header size.
		uint64_t full_inuse_size =
				cf_atomic64_get(ssd->inuse_size) + ssd->header_size;

		if (full_inuse_size > ssd->file_size) {
			// Should never happen!
			cf_warning(AS_DRV_SSD, "%s used size (%lu) > total size (%lu)",
					full_inuse_size, ssd->file_size);
			*total_r = 0;
		}
		else {
			*total_r = ssd->file_size - full_inuse_size;
		}
	}

	if (contiguous_r) {
		*contiguous_r = (uint64_t)cf_queue_sz(ssd->alloc_table->free_wblock_q) *
				(uint64_t)ssd->write_block_size;
	}
}


// Initial state (warm restart): inuse_sz starts at 0.
// Initial state (cold start): everything is allocated and taken, nothing free.
void
ssd_wblock_init(drv_ssd *ssd, uint32_t start_size)
{
	uint32_t n_wblocks = ssd->file_size / ssd->write_block_size;

	cf_info(AS_DRV_SSD, " number of wblocks in allocator: %d wblock %d",
			n_wblocks, ssd->write_block_size);

	ssd_alloc_table *at = cf_malloc(sizeof(ssd_alloc_table) + (n_wblocks * sizeof(ssd_wblock_state)));

	at->n_wblocks = n_wblocks;
	at->free_wblock_q = cf_queue_create(sizeof(uint32_t), true);

	for (uint32_t i = 0; i < n_wblocks; i++) {
		pthread_mutex_init(&at->wblock_state[i].LOCK, 0);
		at->wblock_state[i].state = WBLOCK_STATE_NONE;
		cf_atomic32_set(&at->wblock_state[i].inuse_sz, start_size);
		at->wblock_state[i].swb = 0;
	}

	ssd->alloc_table = at;
}


static int
ssd_populate_bin(as_bin *bin, drv_ssd_bin *ssd_bin, uint8_t *block_head,
		bool single_bin, bool allocate_memory)
{
	as_particle *p = (as_particle*)(block_head + ssd_bin->offset);

	if (p->metadata == AS_PARTICLE_TYPE_INTEGER) {
		if (ssd_bin->len != 10) {
			cf_warning(AS_DRV_SSD, "Possible memory corruption: integer value overflows: expected 10 bytes got %d bytes",
					ssd_bin->len);
		}

		as_particle_int_on_device *pi = (as_particle_int_on_device*)p;
		if (pi->len != 8) {
			return -1;
		}

		// Destroy old particle.
		as_particle_frombuf(bin, AS_PARTICLE_TYPE_NULL, 0, 0, 0, true);

		// Copy the integer particle in-place to the bin.
		bin->ivalue = pi->i;
		as_bin_state_set(bin, AS_BIN_STATE_INUSE_INTEGER);
	}
	else {
		if (allocate_memory) {
			uint32_t base_size = as_particle_get_base_size(p->metadata);

			as_particle_frombuf(bin, p->metadata, (uint8_t*)p + base_size,
					ssd_bin->len - base_size, 0, true);
		}
		else {
			bin->particle = p;

			if (as_particle_type_hidden(p->metadata)) {
				as_bin_state_set(bin, AS_BIN_STATE_INUSE_HIDDEN);
			}
			else {
				as_bin_state_set(bin, AS_BIN_STATE_INUSE_OTHER);
			}
		}
	}

	as_bin_set_version(bin, ssd_bin->version, single_bin);

	return 0;
}


//==========================================================
// Storage API implementation: reading records.
//

inline uint16_t
as_storage_record_get_n_bins_ssd(as_storage_rd *rd)
{
	return rd->u.ssd.block ? rd->u.ssd.block->n_bins : 0;
}


int
as_storage_record_read_ssd(as_storage_rd *rd)
{
	as_record *r = rd->r;

	if (STORAGE_RBLOCK_IS_INVALID(r->storage_key.ssd.rblock_id)) {
		cf_warning(AS_DRV_SSD, "**** ssd_read: record %"PRIx64" has no block associated, fail",
				*(uint64_t*)&rd->keyd);
		return-1;
	}

	uint64_t record_offset = RBLOCKS_TO_BYTES(r->storage_key.ssd.rblock_id);
	uint64_t record_size = RBLOCKS_TO_BYTES(r->storage_key.ssd.n_rblocks);

	uint8_t *read_buf = NULL;
	drv_ssd_block *block = NULL;

	drv_ssd *ssd = rd->u.ssd.ssd;
	ssd_write_buf *swb = 0;
	uint32_t wblock = RBLOCK_ID_TO_WBLOCK_ID(ssd, r->storage_key.ssd.rblock_id);

	swb_check_and_reserve(&ssd->alloc_table->wblock_state[wblock], &swb);

	if (swb) {
		// Data is in write buffer, so read it from there.
		cf_atomic32_incr(&rd->ns->n_reads_from_cache);

		read_buf = cf_malloc(record_size);

		if (! read_buf) {
			return -1;
		}

		block = (drv_ssd_block*)read_buf;

		int swb_offset = record_offset - WBLOCK_ID_TO_BYTES(ssd, wblock);
		memcpy(read_buf, swb->buf + swb_offset, record_size);
		swb_release(swb);
	}
	else {
		// Normal case - data is read from device.
		cf_atomic32_incr(&rd->ns->n_reads_from_device);

		uint64_t record_end_offset = record_offset + record_size;
		uint64_t read_offset = BYTES_DOWN_TO_SYS_RBLOCK_BYTES(record_offset);
		uint64_t read_end_offset = BYTES_UP_TO_SYS_RBLOCK_BYTES(record_end_offset);
		size_t read_size = read_end_offset - read_offset;
		uint64_t record_buf_indent = record_offset - read_offset;

		read_buf = cf_valloc(read_size);

		if (! read_buf) {
			return -1;
		}

		int fd = ssd_fd_get(ssd);

		uint64_t start_time = 0;

		// Measure the latency of device reads.
		if (g_config.storage_benchmarks) {
			start_time = cf_getms();
		}

		lseek(fd, read_offset, SEEK_SET);

		ssize_t rv = read(fd, read_buf, read_size);

		if (g_config.storage_benchmarks && start_time) {
			histogram_insert_data_point(ssd->hist_read, start_time);
		}

		if (rv != read_size) {
			cf_warning(AS_DRV_SSD,"read failed: expected %d got %d: fd %d data %p errno %d",
					read_size, rv, fd, read_buf, errno);
			cf_free(read_buf);
			close(fd);
			return -1;
		}

		ssd_fd_put(ssd, fd);

		block = (drv_ssd_block*)(read_buf + record_buf_indent);

		// Sanity checks.
		if (block->magic != SSD_BLOCK_MAGIC) {
			cf_warning(AS_DRV_SSD, "read: bad block magic offset %"PRIu64,
					read_offset);
			cf_free(read_buf);
			return -1;
		}
		if (0 != cf_digest_compare(&block->keyd, &rd->keyd)) {
			cf_warning(AS_DRV_SSD, "read: read wrong key: expecting %"PRIx64" got %"PRIx64,
				*(uint64_t*)&rd->keyd, *(uint64_t*)&block->keyd);
			cf_free(read_buf);
			return -1;
		}
	}

	rd->u.ssd.block = block;
	rd->u.ssd.must_free_block = read_buf;
	rd->have_device_block = true;

	return 0;
}


int
as_storage_particle_read_all_ssd(as_storage_rd *rd)
{
	// If the record hasn't been read, read it.
	if (rd->u.ssd.block == 0) {
		if (0 != as_storage_record_read_ssd(rd)) {
			cf_info(AS_DRV_SSD, "read_all: failed as_storage_record_read_ssd()");
			return -1;
		}
	}

	drv_ssd_block *block = rd->u.ssd.block;
	uint8_t *block_head = (uint8_t*)rd->u.ssd.block;

	drv_ssd_bin *ssd_bin = (drv_ssd_bin*)(block->data + block->bins_offset);

	for (uint16_t i = 0; i < block->n_bins; i++) {
		as_bin_set_version(&rd->bins[i], ssd_bin->version, rd->ns->single_bin);
		as_bin_set_id_from_name(rd->ns, &rd->bins[i], ssd_bin->name);

		int rv = ssd_populate_bin(&rd->bins[i], ssd_bin, block_head,
				rd->ns->single_bin, false);

		if (0 != rv) {
			return rv;
		}

		ssd_bin = (drv_ssd_bin*)(block_head + ssd_bin->next);
	}

	return 0;
}


// TODO: Eventually conflate this with above method.
int
as_storage_particle_read_and_size_all_ssd(as_storage_rd *rd)
{
	if (! rd->u.ssd.block) {
		if (0 != as_storage_record_read_ssd(rd)) {
			cf_warning(AS_DRV_SSD, "read_and_size_all: failed as_storage_record_read_ssd()");
			return -1;
		}
	}

	drv_ssd_block *block = rd->u.ssd.block;
	uint8_t *block_head = (uint8_t*)rd->u.ssd.block;

	uint8_t *bins_start = block->data + block->bins_offset;
	drv_ssd_bin *ssd_bin = (drv_ssd_bin*)bins_start;

	for (uint16_t i = 0; i < block->n_bins; i++) {
		as_bin_set_version(&rd->bins[i], ssd_bin->version, rd->ns->single_bin);
		as_bin_set_id_from_name(rd->ns, &rd->bins[i], ssd_bin->name);

		int rv = ssd_populate_bin(&rd->bins[i], ssd_bin, block_head,
				rd->ns->single_bin, false);

		if (0 != rv) {
			return rv;
		}

		ssd_bin = (drv_ssd_bin*)(block_head + ssd_bin->next);
	}

	rd->n_bins_to_write = block->n_bins;
	rd->particles_flat_size = ((uint8_t*)ssd_bin - bins_start) -
			(block->n_bins * sizeof(drv_ssd_bin));

	return 0;
}


bool
as_storage_record_get_key_ssd(as_storage_rd *rd)
{
	if (! rd->u.ssd.block) {
		if (0 != as_storage_record_read_ssd(rd)) {
			cf_warning(AS_DRV_SSD, "get_key: failed as_storage_record_read_ssd()");
			return false;
		}
	}

	drv_ssd_block *block = rd->u.ssd.block;
	uint32_t properties_offset = block->vinfo_offset + block->vinfo_length;
	as_rec_props props;

	props.size = block->bins_offset - properties_offset;

	if (props.size == 0) {
		return false;
	}

	props.p_data = block->data + properties_offset;

	return as_rec_props_get_value(&props, CL_REC_PROPS_FIELD_KEY,
			&rd->key_size, &rd->key) == 0;
}


//==========================================================
// Record writing utilities.
//

//------------------------------------------------
// Write-smoother.
//

// TODO - really don't need ssds - deprecate this struct.
typedef struct {
	drv_ssds *ssds;
	drv_ssd *ssd;
} write_worker_arg;

typedef struct as_write_smoothing_arg_s {
	drv_ssd *ssd;
	drv_ssds *ssds;
	as_namespace *ns;

	int budget_usecs_for_this_second;

	// lbw means large block writes
	cf_queue *lbw_to_process_per_second_q;
	int lbw_to_process_per_second_sum;
	uint64_t last_lbw_request_cnt; // last value of ssd_write_buf_counter

	int lbw_to_process_per_second_recent_max; // max from last 5 seconds
	int lbw_catchup_calculation_cnt; // counter used to help calculate recent max

	uint32_t smoothing_period;
	uint32_t last_smoothing_period;

	uint64_t last_time_ms;
	uint64_t last_second;
} as_write_smoothing_arg;

int
as_write_smoothing_arg_initialize(as_write_smoothing_arg *awsa,
		write_worker_arg *wwa)
{
	awsa->ssd = wwa->ssd;
	awsa->ssds = wwa->ssds;
	awsa->ns = awsa->ssds->ns;

	awsa->budget_usecs_for_this_second = 0;

	// lbw means large block writes
	awsa->lbw_to_process_per_second_q = cf_queue_create(sizeof(int), false);
	awsa->lbw_to_process_per_second_sum = 0;
	awsa->last_lbw_request_cnt = 0;

	awsa->lbw_to_process_per_second_recent_max = 0;
	awsa->lbw_catchup_calculation_cnt = 0;

	awsa->smoothing_period = awsa->ssds->ns->storage_write_smoothing_period;
	awsa->last_smoothing_period = awsa->smoothing_period;

	awsa->last_time_ms = cf_getms();
	awsa->last_second = awsa->last_time_ms / 1000;

	return 0;
}

void
as_write_smoothing_arg_destroy(as_write_smoothing_arg *awsa)
{
	cf_queue_destroy(awsa->lbw_to_process_per_second_q);
}

void
as_write_smoothing_arg_reset(as_write_smoothing_arg *awsa)
{
	cf_queue_delete_all(awsa->lbw_to_process_per_second_q);
	awsa->lbw_to_process_per_second_sum = 0;

	awsa->budget_usecs_for_this_second = 0;
}

// This function takes a queue (optionally with a max queue size), a pointer to
// a sum of the elements in the q, and the new data point to push on the q.
// This will "expire" the oldest q element if max_q_sz is hit, then will push
// the new element and update the sum.
//
// This is very useful for updating a rolling N second average.
void
as_write_smoothing_push_to_queue_and_update_queue_sum(cf_queue *q, int max_q_sz,
		int *p_sum, int new_data)
{
	if (max_q_sz != 0 && cf_queue_sz(q) == max_q_sz) {
		int old_data;

		if (0 != cf_queue_pop(q, &old_data, CF_QUEUE_NOWAIT)) {
			// this should never happen
			cf_assert(false, AS_DRV_SSD, CF_CRITICAL,
				"could not pop element off queue");
		}

		*p_sum -= old_data;
	}

	*p_sum += new_data;

	cf_queue_push(q, &new_data);
}

int
as_write_smoothing_get_lbw_to_process_per_second_recent_max_reduce_fn(void *buf,
		void *udata)
{
	as_write_smoothing_arg *awsa = (as_write_smoothing_arg*)udata;

	if (awsa->lbw_catchup_calculation_cnt == NUM_ELEMS_IN_LBW_CATCH_UP_CALC) {
		return -1;
	}

	int *p_lbw_to_process_per_second = (int*)buf;

	if (*p_lbw_to_process_per_second >
			awsa->lbw_to_process_per_second_recent_max) {
		awsa->lbw_to_process_per_second_recent_max =
			*p_lbw_to_process_per_second;
	}

	awsa->lbw_catchup_calculation_cnt++;

	return 0;
}

// This function dictates when to sleep after large block writes.
void
as_write_smoothing_fn(as_write_smoothing_arg *awsa)
{
	// Write throttling algorithm - look at smoothing_period seconds back of
	// data and slow writes accordingly.

	uint32_t smoothing_period = awsa->ssds->ns->storage_write_smoothing_period;

	if (smoothing_period != awsa->last_smoothing_period) {
		// We've changed the smoothing period, so shorten the
		// lbw_to_process_per_second_q if we need to.
		while (cf_queue_sz(awsa->lbw_to_process_per_second_q) >
				smoothing_period) {
			int old_lbw_to_process_per_second_data;

			if (0 != cf_queue_pop(awsa->lbw_to_process_per_second_q,
					&old_lbw_to_process_per_second_data, CF_QUEUE_NOWAIT)) {
				cf_assert(false, AS_DRV_SSD, CF_CRITICAL,
					"could not pop smoothing_data element off queue");
			}

			awsa->lbw_to_process_per_second_sum -=
				old_lbw_to_process_per_second_data;
		}

		if (awsa->last_smoothing_period == 0) {
			awsa->last_lbw_request_cnt =
				cf_atomic_int_get(awsa->ssd->ssd_write_buf_counter);
		}

		awsa->last_smoothing_period = smoothing_period;
	}

	if (smoothing_period == 0) {
		return;
	}

	uint64_t cur_time_ms = cf_getms();
	uint64_t cur_second = cur_time_ms / 1000;

	if ((cur_time_ms - awsa->last_time_ms) > 1000) {
		// It's been more than 1000 ms since our last write, so let's reset our
		// rolling averages.
		as_write_smoothing_arg_reset(awsa);
	}

	// Every *new* second, update the state of the world.
	if (cur_second != awsa->last_second) {
		uint64_t lbw_request_cnt =
			cf_atomic_int_get(awsa->ssd->ssd_write_buf_counter);

		int lbw_requests_this_second = (int)
				(lbw_request_cnt - awsa->last_lbw_request_cnt);

		awsa->last_lbw_request_cnt = lbw_request_cnt;

		if (lbw_requests_this_second == 0) {
			// We received no new lbw requests this second, so let's reset our
			// rolling averages.
			as_write_smoothing_arg_reset(awsa);
		}
		else {
			as_write_smoothing_push_to_queue_and_update_queue_sum(
					awsa->lbw_to_process_per_second_q, smoothing_period,
					&(awsa->lbw_to_process_per_second_sum),
					lbw_requests_this_second);

			// Calculate lbw process per second *recent* max (last 5s) used when
			// write q 80%+ full.
			awsa->lbw_to_process_per_second_recent_max = 0;
			awsa->lbw_catchup_calculation_cnt = 0;

			cf_queue_reduce_reverse(awsa->lbw_to_process_per_second_q,
					as_write_smoothing_get_lbw_to_process_per_second_recent_max_reduce_fn,
					awsa);

			int lbw_to_process_per_second_q_sz =
					cf_queue_sz(awsa->lbw_to_process_per_second_q);

			if (lbw_to_process_per_second_q_sz >=
					MIN_SECONDS_OF_WRITE_SMOOTHING_DATA) {
				int swb_write_q_sz = cf_queue_sz(awsa->ssd->swb_write_q);

				int est_lbw_to_do =
					((awsa->lbw_to_process_per_second_sum * smoothing_period) /
						lbw_to_process_per_second_q_sz) + swb_write_q_sz;

				if (est_lbw_to_do > 0) {
					awsa->budget_usecs_for_this_second =
						(smoothing_period * 1000 * 1000) *
							awsa->ns->storage_write_threads / est_lbw_to_do;
				}

				cf_detail(AS_DRV_SSD, "budget usecs = %d, lbw_to_process_per_second_q_sz = %d, lbw_to_process_per_second_sum = %d, write_q_depth = %d",
						awsa->budget_usecs_for_this_second,
						lbw_to_process_per_second_q_sz,
						awsa->lbw_to_process_per_second_sum, swb_write_q_sz);
			}
			else {
				// Not enough seconds of data to set a budget.
				awsa->budget_usecs_for_this_second = 0;
			}
		}
	}

	int swb_write_q_sz = cf_queue_sz(awsa->ssd->swb_write_q);
	int max_write_q_sz = awsa->ns->storage_max_write_q;

	// Set to 0 so I don't have to set to 0 in a bunch of special cases.
	int budget_usecs = 0;

	if (awsa->budget_usecs_for_this_second > 0) {
		// If write q is 90%+ full, don't smooth this transaction.
		if ((10 * swb_write_q_sz) < (9 * max_write_q_sz)) {
			// If write q is 80%+ full, calculate new budget, which tries to
			// offset the acceleration of write q growth.
			if ((10 * swb_write_q_sz) > (8 * max_write_q_sz)) {
				int est_lbw_to_do =
					(awsa->lbw_to_process_per_second_recent_max *
						smoothing_period) + swb_write_q_sz;

				if (est_lbw_to_do > 0) {
					budget_usecs = (smoothing_period * 1000 * 1000) *
						awsa->ns->storage_write_threads / est_lbw_to_do;
				}

				if (awsa->budget_usecs_for_this_second < budget_usecs) {
					budget_usecs = awsa->budget_usecs_for_this_second;
				}

				cf_detail(AS_DRV_SSD, "write_q > 80 pct | budget_usecs = %d, est_lbw_to_do = %d",
					budget_usecs, est_lbw_to_do);
			}
			else {
				// Standard case.
				budget_usecs = awsa->budget_usecs_for_this_second;
			}
		}
	}

	cf_detail(AS_DRV_SSD, "budget_usecs = %d, trans time = %"PRIu64", sleep_usecs = %d",
			budget_usecs, cur_time_ms - awsa->last_time_ms,
			budget_usecs - (((int)(cur_time_ms - awsa->last_time_ms)) * 1000));

	if (budget_usecs > 0) {
		// Don't budget for less than 1 lbw/sec.
		if (budget_usecs > 1000000) {
			budget_usecs = 1000000;
		}

		int sleep_usecs = budget_usecs -
			(((int)(cur_time_ms - awsa->last_time_ms)) * 1000);

		if (sleep_usecs > 0) {
			usleep(sleep_usecs);
			cur_time_ms += (sleep_usecs / 1000); // estimate new cur_time_ms
		}
	}

	awsa->last_time_ms = cur_time_ms;
	awsa->last_second = cur_second;
}

//
// END - Write-smoother.
//------------------------------------------------


// Thread "run" function that flushes write buffers to device.
void *
ssd_write_worker(void *arg)
{
	write_worker_arg *wwa = (write_worker_arg*)arg;
	drv_ssd *ssd = wwa->ssd;

	as_write_smoothing_arg awsa;
	as_write_smoothing_arg_initialize(&awsa, wwa);

	cf_free(wwa);

	if (ssd->readonly) {
		// Should never get here. TODO - may as well cf_crash.
		cf_warning(AS_DRV_SSD, "don't start write worker: readonly mode");
		return 0;
	}

	while (ssd->running) {
		ssd_write_buf *swb;

		if (CF_QUEUE_OK != cf_queue_pop(ssd->swb_write_q, &swb, 100)) {
			continue;
		}

		ssd_wblock_state* p_wblock_state =
				&ssd->alloc_table->wblock_state[swb->wblock_id];

		// Sanity checks.
		if (p_wblock_state->swb != swb) {
			cf_warning(AS_DRV_SSD, "device %s: wblock-id %u swb not consistent while writing",
					ssd->name, swb->wblock_id);
		}
		if (p_wblock_state->state != WBLOCK_STATE_NONE) {
			cf_warning(AS_DRV_SSD, "device %s: wblock-id %u state not NONE while writing",
					ssd->name, swb->wblock_id);
		}

		int fd = ssd_fd_get(ssd);

		uint64_t start_time = 0;

		if (g_config.storage_benchmarks) {
			start_time = cf_getms();
		}

		off_t rv_o = lseek(fd, WBLOCK_ID_TO_BYTES(ssd, swb->wblock_id), SEEK_SET);

		if (rv_o != WBLOCK_ID_TO_BYTES(ssd, swb->wblock_id)) {
			cf_warning(AS_DRV_SSD, "DEVICE FAILED: device %s can't seek errno %d",
					ssd->name, errno);

			if (g_config.storage_benchmarks && start_time) {
				histogram_insert_data_point(ssd->hist_write, start_time);
			}

			close(fd);
			return 0;
		}

		ssize_t rv_s = write(fd, swb->buf, ssd->write_block_size);

		if (g_config.storage_benchmarks && start_time) {
			histogram_insert_data_point(ssd->hist_write, start_time);
		}

		if (rv_s != ssd->write_block_size) {
			cf_crash(AS_DRV_SSD, "DEVICE FAILED: %s errno %d (%s)",
					ssd->name,errno, cf_strerror(errno));
			close(fd);
			return 0;
		}

		ssd_fd_put(ssd, fd);

		if (cf_atomic32_get(ssd->ns->storage_post_write_queue) == 0) {
			swb_dereference_and_release(ssd->alloc_table, swb->wblock_id, swb);
		}
		else {
			// Transfer swb to post-write queue.
			cf_queue_push(ssd->post_write_q, &swb);
		}

		if (ssd->post_write_q) {
			// Release post-write queue swbs if we're over the limit.
			while ((uint32_t)cf_queue_sz(ssd->post_write_q) >
					cf_atomic32_get(ssd->ns->storage_post_write_queue)) {
				ssd_write_buf* cached_swb;

				if (CF_QUEUE_OK != cf_queue_pop(ssd->post_write_q, &cached_swb,
						CF_QUEUE_NOWAIT)) {
					// Should never happen.
					cf_warning(AS_DRV_SSD, "device %s: post-write queue pop failed",
							ssd->name);
					break;
				}

				swb_dereference_and_release(ssd->alloc_table,
						cached_swb->wblock_id, cached_swb);
			}
		}

		as_write_smoothing_fn(&awsa);
	} // infinite event loop waiting for block to write

	as_write_smoothing_arg_destroy(&awsa);

	return NULL;
}


void
ssd_start_write_worker_threads(drv_ssds *ssds)
{
	if (ssds->ns->storage_readonly) {
		return;
	}

	if (ssds->ns->storage_write_threads > MAX_SSD_THREADS) {
		cf_warning(AS_DRV_SSD, "configured number of write threads %s greater than max, using %d instead",
				ssds->ns->storage_write_threads, MAX_SSD_THREADS);
		ssds->ns->storage_write_threads = MAX_SSD_THREADS;
	}

	cf_info(AS_DRV_SSD, "ns %s starting write worker threads", ssds->ns->name);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		for (uint32_t j = 0; j < ssds->ns->storage_write_threads; j++) {
			write_worker_arg *wwa = cf_malloc(sizeof(write_worker_arg));
			wwa->ssd = ssd;
			wwa->ssds = ssds;
			pthread_create(&ssd->write_worker_thread[j], 0, ssd_write_worker, wwa);
		}
	}
}


uint32_t
as_storage_record_overhead_size(as_storage_rd *rd)
{
	size_t size = 0;

	// Start with pickled size of vinfo, if any.
	if (rd->ns->allow_versions &&
			0 != as_partition_vinfoset_mask_pickle_getsz(
					as_index_vinfo_mask_get(rd->r, true), &size)) {
		cf_crash(AS_DRV_SSD, "unable to pickle vinfoset");
	}

	// Add size of record header struct.
	size += sizeof(drv_ssd_block);

	// Add size of any record properties.
	if (rd->rec_props.p_data) {
		size += rd->rec_props.size;
	}

	return (uint32_t)size;
}


uint32_t
ssd_write_calculate_size(as_record *r, as_storage_rd *rd)
{
	// Note - this function is the only place where rounding size (up to a
	// multiple of RBLOCK_SIZE) is really necessary.

	// TODO - use when we're confident duplicate bins were the only issue:
#ifdef HANDLE_DUPLICATE_BINS
	// If we already know the flat size, just need to round it.
	if (rd->flat_size != 0) {
		return BYTES_TO_RBLOCK_BYTES(rd->flat_size);
	}
#endif

	// Start with the record storage overhead, including vinfo and rec-props.
	uint32_t write_size = as_storage_record_overhead_size(rd);

	if (! rd->bins) {
		// Should never get here.
		cf_warning(AS_DRV_SSD, "cannot calculate write size, no bins pointer");
		return 0;
	}

	// Add the bins' sizes, including bin overhead.
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *bin = &rd->bins[i];

		if (! as_bin_inuse(bin)) {
			break;
		}

		size_t particle_flat_sz;

		if (0 != as_particle_get_flat_size(bin, &particle_flat_sz)) {
			// Should never get here.
			cf_warning(AS_DRV_SSD, "on write, can't get particle flat size");
			return 0;
		}

		// TODO: could factor out sizeof(drv_ssd_bin) and multiply by i, but
		// for now let's favor the low bin-count case and leave it this way.
		write_size += sizeof(drv_ssd_bin) + (uint32_t)particle_flat_sz;
	}

	// TODO - remove when we're confident duplicate bins were the only issue:
	if (rd->flat_size != 0 && rd->flat_size != write_size) {
		cf_warning(AS_DRV_SSD, "flat_size %u != write_size %u - duplicate bins?",
				rd->flat_size, write_size);
	}

	return BYTES_TO_RBLOCK_BYTES(write_size);
}


int
ssd_write_bins(as_record *r, as_storage_rd *rd)
{
	uint32_t write_size = ssd_write_calculate_size(r, rd);

	if (write_size == 0) {
		return -1;
	}

	// Pickle vinfo, if any.
	uint8_t vinfo_buf[AS_PARTITION_VINFOSET_PICKLE_MAX];
	size_t vinfo_buf_length = 0;

	if (rd->ns->allow_versions) {
		vinfo_buf_length = AS_PARTITION_VINFOSET_PICKLE_MAX;

		as_partition_id pid = as_partition_getid(rd->keyd);
		as_partition_vinfoset *vinfoset = &rd->ns->partitions[pid].vinfoset;

		if (0 != as_partition_vinfoset_mask_pickle(vinfoset,
				as_index_vinfo_mask_get(r, true), vinfo_buf,
				&vinfo_buf_length)) {
			cf_crash(AS_DRV_SSD, "unable to pickle vinfoset");
		}
	}

	drv_ssd *ssd = rd->u.ssd.ssd;

	if (! ssd->current_swb) {
		ssd->current_swb = swb_get(ssd);

		if (! ssd->current_swb) {
			return -1;
		}
	}

	ssd_write_buf *swb = ssd->current_swb;

	// Check if there's enough space in current buffer -if not, free and zero
	// any remaining unused space, enqueue it to be flushed to device, and grab
	// a new buffer.
	if (write_size > ssd->write_block_size - swb->pos) {
		if (write_size > ssd->write_block_size) {
			cf_warning(AS_DRV_SSD, "write: rejecting %"PRIx64" write size: %d",
					*(uint64_t*)&rd->keyd, write_size);
			return -1;
		}

		if (ssd->write_block_size != swb->pos) {
			// Clean the end of the buffer before pushing to write queue.
			memset(&swb->buf[swb->pos], 0, ssd->write_block_size - swb->pos);

			// Free unused size at the end of the wblock.
			ssd_block_free(ssd,
					BYTES_TO_RBLOCKS(WBLOCK_ID_TO_BYTES(ssd, swb->wblock_id) +
							swb->pos),
					BYTES_TO_RBLOCKS(ssd->write_block_size - swb->pos),
					FREE_TO_HEAD, "write-bins");
		}

		// Enqueue the buffer, to be flushed to device.
		cf_queue_push(ssd->swb_write_q, &swb);
		cf_atomic_int_incr(&ssd->ssd_write_buf_counter); // for write smoothing

		// Get the new buffer.
		swb = swb_get(ssd);
		ssd->current_swb = swb;
	}

	if (! ssd->current_swb) {
		return -1;
	}

	// Flatten data into the block.

	uint8_t *buf = &swb->buf[swb->pos];
	uint8_t *buf_start = buf;

	drv_ssd_block *block = (drv_ssd_block*)buf;

	buf += sizeof(drv_ssd_block);

	// Write pickled vinfo, if any.
	if (vinfo_buf_length != 0) {
		memcpy(buf, vinfo_buf, vinfo_buf_length);
		buf += vinfo_buf_length;
	}

	// Properties list goes just before bins.
	if (rd->rec_props.p_data) {
		memcpy(buf, rd->rec_props.p_data, rd->rec_props.size);
		buf += rd->rec_props.size;
	}

	drv_ssd_bin *ssd_bin = 0;
	uint32_t write_nbins = 0;

	if (0 == rd->bins) {
		cf_warning(AS_DRV_SSD, "write bins: no bin array to write from, aborting.");
		return -1;
	}

	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *bin = &rd->bins[i];

		if (as_bin_inuse(bin)) {
			ssd_bin = (drv_ssd_bin*)buf;
			buf += sizeof(drv_ssd_bin);

			ssd_bin->version = as_bin_get_version(bin, rd->ns->single_bin);

			if (! rd->ns->single_bin) {
				strcpy(ssd_bin->name, as_bin_get_name_from_id(rd->ns, bin->id));
			}
			else {
				ssd_bin->name[0] = 0;
			}

			ssd_bin->offset = buf - buf_start;

			size_t particle_flat_size;

			if (0 == as_particle_get_flat_size(bin, &particle_flat_size)) {
				if (as_bin_is_integer(bin)) {
					as_particle_int_on_device *p =
							(as_particle_int_on_device*)buf;
					p->type = AS_PARTICLE_TYPE_INTEGER;
					p->len = sizeof(uint64_t);
					p->i = bin->ivalue;
				}
				else {
					memcpy(buf, as_bin_get_particle(bin), particle_flat_size);
				}
			}
			else {
				cf_warning(AS_DRV_SSD, "can't get flat particle size - writing empty bin");
				particle_flat_size = 0;
			}

			buf += particle_flat_size;
			ssd_bin->len = particle_flat_size;
			ssd_bin->next = buf - buf_start;

			write_nbins++;
		}
	}

	block->length = write_size - SIGNATURE_OFFSET;
	block->magic = SSD_BLOCK_MAGIC;
	block->sig = 0;
	block->keyd = rd->keyd;
	block->generation = r->generation;
	block->void_time = r->void_time;
	block->bins_offset = vinfo_buf_length + (rd->rec_props.p_data ? rd->rec_props.size : 0);
	block->n_bins = write_nbins;
	block->vinfo_offset = 0;
	block->vinfo_length = vinfo_buf_length;

	r->storage_key.ssd.file_id = ssd->file_id;
	r->storage_key.ssd.rblock_id = BYTES_TO_RBLOCKS(WBLOCK_ID_TO_BYTES(ssd, swb->wblock_id) + swb->pos);
	r->storage_key.ssd.n_rblocks = BYTES_TO_RBLOCKS(write_size);

	swb->pos += write_size;

	// It would be nicer to do this outside the lock.
	if (ssd->use_signature) {
		cf_signature_compute(((uint8_t*)block) + SIGNATURE_OFFSET,
				block->length, &block->sig);
	}
	else {
		block->sig = 0;
	}

	return 0;
}


int
ssd_write(as_record *r, as_storage_rd *rd)
{
	drv_ssd *old_ssd = NULL;
	uint64_t old_rblock_id = 0;
	uint16_t old_n_rblocks = 0;

	if (STORAGE_RBLOCK_IS_VALID(r->storage_key.ssd.rblock_id)) {
		// Replacing an old record.

		// temp "available percent" bug hunting:
		if (r->storage_key.ssd.n_rblocks == 0) {
			cf_warning(AS_DRV_SSD, "accounting: replacing 0-size record %lx",
					*(uint64_t*)&r->key);
		}

		old_ssd = rd->u.ssd.ssd;
		old_rblock_id = r->storage_key.ssd.rblock_id;
		old_n_rblocks = r->storage_key.ssd.n_rblocks;
	}

	drv_ssds *ssds = (drv_ssds*)rd->ns->storage_private;

	// Figure out which device to write to. When replacing an old record, it's
	// possible this is different from the old device (e.g. if we've added a
	// fresh device), so derive it from the digest each time.
	rd->u.ssd.ssd = &ssds->ssds[ssd_get_file_id(ssds, &rd->keyd)];

	drv_ssd *ssd = rd->u.ssd.ssd;

	if (! ssd) {
		return -1;
	}

	pthread_mutex_lock(&ssd->LOCK);

	int rv = ssd_write_bins(r, rd);

	pthread_mutex_unlock(&ssd->LOCK);

	if (rv == 0 && old_ssd) {
		ssd_block_free(old_ssd, old_rblock_id, old_n_rblocks, FREE_TO_HEAD,
				"ssd-write");
	}

	return rv;
}


//==========================================================
// Storage statistics utilities.
//

void
as_storage_show_wblock_stats(as_namespace *ns)
{
	if (AS_STORAGE_ENGINE_SSD != ns->storage_type) {
		cf_info(AS_DRV_SSD, "Storage engine type must be SSD (%d), not %d.",
				AS_STORAGE_ENGINE_SSD, ns->storage_type);
		return;
	}

	if (ns->storage_private) {
		drv_ssds *ssds = ns->storage_private;

		for (int d = 0; d < ssds->n_ssds; d++) {
			int num_free_blocks = 0;
			int num_full_blocks = 0;
			int num_full_swb = 0;
			int num_above_wm = 0;
			int num_defraggable = 0;

			drv_ssd *ssd = &ssds->ssds[d];
			ssd_alloc_table *at = ssd->alloc_table;
			uint32_t lwm_size =
					(ssd->write_block_size * ns->storage_defrag_lwm_pct) / 100;

			for (uint32_t i = 0; i < at->n_wblocks; i++) {
				ssd_wblock_state *wblock_state = &at->wblock_state[i];
				uint32_t inuse_sz = cf_atomic32_get(wblock_state->inuse_sz);

				if (inuse_sz == 0) {
					num_free_blocks++;
				}
				else if (inuse_sz == ssd->write_block_size) {
					if (wblock_state->swb) {
						num_full_swb++;
					}
					else {
						num_full_blocks++;
					}
				}
				else {
					if (inuse_sz > ssd->write_block_size || inuse_sz <= lwm_size) {
						cf_info(AS_DRV_SSD, "dev %d, wblock %"PRIu32", inuse_sz %"PRIu32", %s swb",
								d, i, inuse_sz, wblock_state->swb ? "has" : "no");

						num_defraggable++;
					}
					else {
						num_above_wm++;
					}
				}
			}

			cf_info(AS_DRV_SSD, "device %s free %d full %d fullswb %d pfull %d defrag %d freeq %d",
				ssd->name, num_free_blocks, num_full_blocks, num_full_swb,
				num_above_wm, num_defraggable, cf_queue_sz(at->free_wblock_q));
		}
	}
	else {
		cf_info(AS_DRV_SSD, "no devices");
	}
}


void
as_storage_summarize_wblock_stats(as_namespace *ns)
{
	int num_free_blocks = 0;
	int num_full_blocks = 0;
	int num_full_swb = 0;
	int num_above_wm = 0;
	int num_defraggable = 0;
	uint64_t defraggable_sz = 0;
	uint64_t non_defraggable_sz = 0;

	if (AS_STORAGE_ENGINE_SSD != ns->storage_type) {
		cf_info(AS_DRV_SSD, "Storage engine type must be SSD (%d), not %d.",
				AS_STORAGE_ENGINE_SSD, ns->storage_type);
		return;
	}

	if (ns->storage_private) {
		drv_ssds *ssds = ns->storage_private;

		// Note: This is a sparse array that could be more efficiently stored.
		// (In addition, ranges of block sizes could be binned together to
		// compress the histogram, rather than using one bin per block size.)
		int wb_hist[MAX_WRITE_BLOCK_SIZE + 1];

		memset(wb_hist, 0, MAX_WRITE_BLOCK_SIZE * sizeof(int));

		for (int d=0; d <ssds->n_ssds; d++) {
			drv_ssd *ssd = &ssds->ssds[d];
			ssd_alloc_table *at = ssd->alloc_table;
			uint32_t lwm_size =
					(ssd->write_block_size * ns->storage_defrag_lwm_pct) / 100;

			for (uint32_t i = 0; i < at->n_wblocks; i++) {
				ssd_wblock_state *wblock_state = &at->wblock_state[i];
				uint32_t inuse_sz = cf_atomic32_get(wblock_state->inuse_sz);

				if (inuse_sz >= MAX_WRITE_BLOCK_SIZE) {
					cf_warning(AS_DRV_SSD, "wblock size (%d >= %d) too large ~~ not counting in histogram",
							inuse_sz, MAX_WRITE_BLOCK_SIZE);
				}
				else {
					wb_hist[inuse_sz]++;
				}

				if (inuse_sz == 0) {
					num_free_blocks++;
				}
				else if (inuse_sz == ssd->write_block_size) {
					if (wblock_state->swb) {
						num_full_swb++;
					}
					else {
						num_full_blocks++;
					}
				}
				else {
					if (inuse_sz > ssd->write_block_size || inuse_sz <= lwm_size) {
						defraggable_sz += inuse_sz;
						num_defraggable++;
					}
					else {
						non_defraggable_sz += inuse_sz;
						num_above_wm++;
					}
				}
			}

			cf_info(AS_DRV_SSD, "device %s free %d full %d fullswb %d pfull %d defrag %d freeq %d",
				ssd->name, num_free_blocks, num_full_blocks, num_full_swb,
				num_above_wm, num_defraggable, cf_queue_sz(at->free_wblock_q));
		}

		// Dump histogram.
		cf_info(AS_DRV_SSD, "WBH: Storage histogram for namespace \"%s\":", ns->name);
		cf_info(AS_DRV_SSD, "WBH: Average wblock size of: defraggable blocks: %lu bytes; nondefraggable blocks: %lu bytes; all blocks: %lu bytes",
				(defraggable_sz / MAX(1, num_defraggable)),
				(non_defraggable_sz / MAX(1, num_above_wm)),
				((defraggable_sz + non_defraggable_sz) / MAX(1, (num_defraggable + num_above_wm))));

		for (int i = 0; i < MAX_WRITE_BLOCK_SIZE; i++) {
			if (wb_hist[i] > 0) {
				cf_info(AS_DRV_SSD, "WBH: %d block%s of size %lu bytes",
						wb_hist[i], (wb_hist[i] != 1 ? "s" : ""), i);
			}
		}
	}
	else {
		cf_info(AS_DRV_SSD,"no devices");
	}
}


// TODO - do something more useful with this info command.
int
as_storage_analyze_wblock(as_namespace* ns, int device_index,
		uint32_t wblock_id)
{
	if (AS_STORAGE_ENGINE_SSD != ns->storage_type) {
		cf_info(AS_DRV_SSD, "Storage engine type must be SSD (%d), not %d.",
				AS_STORAGE_ENGINE_SSD, ns->storage_type);
		return -1;
	}

	cf_info(AS_DRV_SSD, "analyze wblock: ns %s, device-index %d, wblock-id %u",
			ns->name, device_index, wblock_id);

	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	if (! ssds) {
		cf_warning(AS_DRV_SSD, "analyze wblock ERROR: no devices");
		return -1;
	}

	if (device_index < 0 || device_index >= ssds->n_ssds) {
		cf_warning(AS_DRV_SSD, "analyze wblock ERROR: bad device-index");
		return -1;
	}

	drv_ssd* ssd = &ssds->ssds[device_index];

	int fd = ssd_fd_get(ssd);

	if (-1 == fd) {
		cf_warning(AS_DRV_SSD, "analyze wblock ERROR: can't get fd");
		return -1;
	}

	off_t file_offset = lseek(fd, WBLOCK_ID_TO_BYTES(ssd, wblock_id), SEEK_SET);

	if (file_offset != (off_t)WBLOCK_ID_TO_BYTES(ssd, wblock_id)) {
		cf_warning(AS_DRV_SSD, "analyze wblock ERROR: fail fd seek");
		close(fd);
		return -1;
	}

	uint8_t* read_buf = cf_valloc(ssd->write_block_size);

	if (! read_buf) {
		cf_warning(AS_DRV_SSD, "analyze wblock ERROR: fail valloc");
		close(fd);
		return -1;
	}

	ssize_t rlen = read(fd, read_buf, ssd->write_block_size);

	close(fd);

	if (rlen != (ssize_t)ssd->write_block_size) {
		cf_warning(AS_DRV_SSD, "analyze wblock ERROR: fail fd read");
		cf_free(read_buf);
		return -1;
	}

	uint32_t living_populations[AS_PARTITIONS];
	uint32_t zombie_populations[AS_PARTITIONS];

	memset(living_populations, 0, sizeof(living_populations));
	memset(zombie_populations, 0, sizeof(zombie_populations));

	uint32_t inuse_sz_start =
			cf_atomic32_get(ssd->alloc_table->wblock_state[wblock_id].inuse_sz);
	uint32_t offset = 0;

	while (offset < ssd->write_block_size) {
		drv_ssd_block* p_block = (drv_ssd_block*)&read_buf[offset];

		if (p_block->magic != SSD_BLOCK_MAGIC) {
			if (offset == 0) {
				// First block must have magic.
				cf_warning(AS_DRV_SSD, "analyze wblock ERROR: 1st block has no magic");
				cf_free(read_buf);
				return -1;
			}

			// Later blocks may have no magic, just skip to next block.
			offset += RBLOCK_SIZE;
			continue;
		}

		// Note - if block->length is sane, we don't need to round up to a
		// multiple of RBLOCK_SIZE, but let's do it anyway just to be safe.
		uint32_t next_offset = offset +
				BYTES_TO_RBLOCK_BYTES(p_block->length + SIGNATURE_OFFSET);

		if (next_offset > ssd->write_block_size) {
			cf_warning(AS_DRV_SSD, "analyze wblock ERROR: record overflows wblock");
			cf_free(read_buf);
			return -1;
		}

		// Check signature.
		if (ssd->use_signature && p_block->sig) {
			cf_signature sig;

			cf_signature_compute(((uint8_t*)p_block) + SIGNATURE_OFFSET,
					p_block->length, &sig);

			if (sig != p_block->sig) {
				// Look for next block with magic.
				offset += RBLOCK_SIZE;
				continue;
			}
		}

		uint64_t rblock_id = BYTES_TO_RBLOCKS(file_offset + offset);
		uint32_t n_rblocks = (uint32_t)BYTES_TO_RBLOCKS(next_offset - offset);

		bool living = false;
		as_partition_id pid = as_partition_getid(p_block->keyd);
		as_partition_reservation rsv;

		as_partition_reserve_migrate(ns, pid, &rsv, 0);
		cf_atomic_int_incr(&g_config.ssdr_tree_count);

		as_index_ref r_ref;
		r_ref.skip_lock = false;

		if (0 == as_record_get(rsv.tree, &p_block->keyd, &r_ref, ns)) {
			as_index* r = r_ref.r;

			if (r->storage_key.ssd.rblock_id == rblock_id &&
					r->storage_key.ssd.n_rblocks == n_rblocks) {
				living = true;
			}

			as_record_done(&r_ref, ns);
		}
		else if (ns->ldt_enabled &&
				(0 == as_record_get(rsv.sub_tree, &p_block->keyd, &r_ref, ns))) {
			as_index* r = r_ref.r;

			if (r->storage_key.ssd.rblock_id == rblock_id &&
					r->storage_key.ssd.n_rblocks == n_rblocks) {
				living = true;
			}

			as_record_done(&r_ref, ns);
		}
		// else it was deleted (?) so call it a zombie...

		as_partition_release(&rsv);
		cf_atomic_int_decr(&g_config.ssdr_tree_count);

		if (living) {
			living_populations[pid]++;
		}
		else {
			zombie_populations[pid]++;
		}

		offset = next_offset;
	}

	cf_free(read_buf);

	uint32_t inuse_sz_end =
			cf_atomic32_get(ssd->alloc_table->wblock_state[wblock_id].inuse_sz);

	cf_info(AS_DRV_SSD, "analyze wblock: inuse_sz %u (before) -> %u (after)",
			inuse_sz_start, inuse_sz_end);

	for (int i = 0; i < AS_PARTITIONS; i++) {
		if (living_populations[i] > 0 || zombie_populations[i] > 0) {
			cf_info(AS_DRV_SSD, "analyze wblock: pid %4d - live %u, dead %u",
					i, living_populations[i], zombie_populations[i]);
		}
	}

	return 0;
}


void *
ssd_track_free_thread(void *udata)
{
	drv_ssd *ssd = (drv_ssd*)udata;

	while (true) {
		sleep(20);

		off_t free;
		off_t contig;

		ssd_block_get_free_size(ssd, &free, &contig);
		free /= (1000 * 1000);
		contig /= (1000 * 1000);

		cf_info(AS_DRV_SSD, "device %s: free %zuM contig %zuM %s w-q %d w-free %d swb-free %d w-tot %"PRIu64,
				ssd->name, free, contig, ssd->readonly ? "READONLY" : "",
				cf_queue_sz(ssd->swb_write_q),
				cf_queue_sz(ssd->alloc_table->free_wblock_q),
				cf_queue_sz(ssd->swb_free_q),
				cf_atomic_int_get(ssd->ssd_write_buf_counter));

		if (cf_queue_sz(ssd->alloc_table->free_wblock_q) == 0) {
			cf_warning(AS_DRV_SSD, "Device %s out of storage space", ssd->name);
		}

		// Try to recover swbs, 16 at a time, down to 16.
		for (int i = 0; i < 16 && cf_queue_sz(ssd->swb_free_q) > 16; i++) {
			ssd_write_buf* swb;

			if (CF_QUEUE_OK !=
					cf_queue_pop(ssd->swb_free_q, &swb, CF_QUEUE_NOWAIT)) {
				break;
			}

			cf_info(AS_DRV_SSD, "device %s: freeing swb", ssd->name);
			swb_destroy(swb);
		}
	}

	return NULL;
}


static void
ssd_start_track_free_threads(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "ns %s starting device tracker threads", ssds->ns->name);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd* ssd = &ssds->ssds[i];

		pthread_create(&ssd->free_tracker_thread, 0, ssd_track_free_thread, ssd);
	}
}


//==========================================================
// Device header utilities.
//

// -1 means unrecoverable error
// -2 means not formatted, please overwrite me
int
as_storage_read_header(drv_ssd *ssd, as_namespace *ns,
		ssd_device_header **header_r)
{
	*header_r = 0;

	int rv = -1;

	int fd = open(ssd->name, ssd->open_flag, S_IRUSR | S_IWUSR);

	if (fd <= 0) {
		cf_info(AS_DRV_SSD, "read_header: can't open dev %s error %s",
				ssd->name, cf_strerror(errno));
		return -1;
	}

	size_t peek_size = BYTES_UP_TO_SYS_RBLOCK_BYTES(sizeof(ssd_device_header));
	ssd_device_header *header = cf_valloc(peek_size);

	if (! header) {
		goto Fail;
	}

	off_t off = lseek(fd, 0, SEEK_SET);

	if (off != 0) {
		cf_info(AS_DRV_SSD, "read_header: dev %s: unable to seek: rv %zd error %s",
				ssd->name, off, cf_strerror(errno));
		goto Fail;
	}

	ssize_t sz = read(fd, (void*)header, peek_size);

	if (sz != peek_size) {
		cf_info(AS_DRV_SSD, "read_header: dev %s: unable to read: rv %zd error %s",
				ssd->name, sz, cf_strerror(errno));
		goto Fail;
	}

	// Make sure all following checks that return -1 or -2 are also done in
	// as_storage_namespace_peek_ssd().

	if (header->magic != SSD_HEADER_MAGIC) { // normal path for a fresh drive
		cf_detail(AS_DRV_SSD, "read_header: device %s no magic, not a Citrusleaf drive",
				ssd->name);
		rv = -2;
		goto Fail;
	}

	if (header->version != SSD_VERSION) {
		if (can_convert_storage_version(header->version)) {
			cf_info(AS_DRV_SSD, "read_header: device %s converting storage version %u to %u",
					ssd->name, header->version, SSD_VERSION);
		}
		else {
			cf_warning(AS_DRV_SSD, "read_header: device %s bad version %u, not a current Citrusleaf drive",
					ssd->name, header->version);
			goto Fail;
		}
	}

	if (header->write_block_size != 0 &&
			ns->storage_write_block_size % header->write_block_size != 0) {
		cf_warning(AS_DRV_SSD, "read header: device %s can't change write-block-size from %u to %u",
				ssd->name, header->write_block_size,
				ns->storage_write_block_size);
		goto Fail;
	}

	if (header->devices_n > 100) {
		cf_warning(AS_DRV_SSD, "read header: device %s don't support %u devices, corrupt read",
				ssd->name, header->devices_n);
		goto Fail;
	}

	if (strcmp(header->namespace, ns->name) != 0) {
		cf_warning(AS_DRV_SSD, "read header: device %s previous namespace %s now %s, check config or erase device",
				ssd->name, header->namespace, ns->name);
		goto Fail;
	}

	size_t h_len = header->header_length;

	cf_free(header);

	header = cf_valloc(h_len);

	if (! header) {
		goto Fail;
	}

	lseek(fd, 0, SEEK_SET);
	sz = read(fd, (void*)header, h_len);

	if (sz != header->header_length) {
		goto Fail;
	}

	cf_detail(AS_DRV_SSD, "device %s: header read success: version %d devices %d random %"PRIu64,
		ssd->name, header->version, header->devices_n, header->random);

	// In case we're bumping the version - ensure the new version gets written.
	header->version = SSD_VERSION;

	// In case we're increasing write-block-size - ensure new value is recorded.
	header->write_block_size = ns->storage_write_block_size;

	*header_r = header;
	close(fd);

	return 0;

Fail:

	if (header) {
		cf_free(header);
	}

	if (fd != -1) {
		close(fd);
	}

	return rv;
}


ssd_device_header *
as_storage_init_header(as_namespace *ns)
{
	ssd_device_header *h = cf_valloc(SSD_DEFAULT_HEADER_LENGTH);

	if (! h) {
		return 0;
	}

	memset(h, 0, SSD_DEFAULT_HEADER_LENGTH);

	h->magic = SSD_HEADER_MAGIC;
	h->random = 0;
	h->write_block_size = ns->storage_write_block_size;
	h->last_evict_void_time = 0;
	h->version = SSD_VERSION;
	h->devices_n = 0;
	h->header_length = SSD_DEFAULT_HEADER_LENGTH;
	memset(h->namespace, 0, sizeof(h->namespace));
	strcpy(h->namespace, ns->name);
	h->info_n = 4096;
	h->info_stride = 128;

	return h;
}


int
as_storage_write_header(drv_ssd *ssd, ssd_device_header *header)
{
	if (ssd->readonly) {
		return -1;
	}

	cf_detail(AS_DRV_SSD, "storage write header: device %s", ssd->name);

	int fd = open(ssd->name, ssd->open_flag, S_IRUSR | S_IWUSR);

	if (fd <= 0) {
		cf_warning(AS_DRV_SSD, "unable to open file %s: %s", ssd->name, cf_strerror(errno));
		return -1;
	}

	lseek(fd, 0, SEEK_SET);

	ssize_t sz = write(fd, (void*)header, header->header_length);

	if (sz != header->header_length) {
		cf_info(AS_DRV_SSD, "storage write header: failure: wrote %"PRIu64" expected %"PRIu64" %s",
			sz, header->header_length, ssd->name);
		close(fd);
		return -1;
	}

	fsync(fd);
	close(fd);
	return 0;
}


//==========================================================
// Cold start utilities.
//

void
ssd_record_get_ldt_property(as_rec_props *props, bool *is_ldt_parent,
		bool *is_ldt_sub)
{
	uint16_t * ldt_rectype_bits;

	*is_ldt_sub	= false;
	*is_ldt_parent = false;

	if (props->size != 0 &&
			(as_rec_props_get_value(props, CL_REC_PROPS_FIELD_LDT_TYPE, NULL,
					(uint8_t**)&ldt_rectype_bits) == 0)) {
		if (as_ldt_flag_has_sub(*ldt_rectype_bits)) {
			*is_ldt_sub = true;
		}
		else if (as_ldt_flag_has_parent(*ldt_rectype_bits)) {
			*is_ldt_parent = true;
		}
	}

	cf_detail(AS_LDT, "LDT_LOAD ssd_record_get_ldt_property: Parent=%d Subrec=%d",
			*is_ldt_parent, *is_ldt_sub);
}


// Add a record just read from drive to the index, if all is well.
// Return values:
//  0 - success, record added or updated
// -1 - skipped or deleted this record for a "normal" reason
// -2 - serious limit encountered, caller won't continue
// -3 - couldn't parse this record, but caller will continue
int
ssd_record_add(drv_ssds* ssds, drv_ssd* ssd, drv_ssd_block* block,
		uint64_t rblock_id, uint32_t n_rblocks)
{
	cf_atomic_int_incr(&g_config.stat_storage_startup_load);

	as_partition_id pid = as_partition_getid(block->keyd);

	// If this isn't a partition we're interested in, skip this record.
	if (! ssds->get_state_from_storage[pid]) {
		return -1;
	}

	as_namespace* ns = ssds->ns;

	// If eviction is necessary, evict previously added records closest to
	// expiration. (If evicting, this call will block for a long time.) This
	// call also updates the cold-start threshold void-time.
	if (! as_cold_start_evict_if_needed(ns)) {
		cf_warning(AS_DRV_SSD, "device %s: record-add halting read", ssd->name);
		return -2;
	}

	// Sanity-check the record.
	if (! is_valid_record(block, ns->name)) {
		return -3;
	}

	// Don't bother with reservations - partition trees aren't going anywhere.
	as_partition* p_partition = &ns->partitions[pid];

	// Get or create the record.
	as_index_ref r_ref;
	r_ref.skip_lock = false;

	// Read LDT rec-prop.
	uint32_t properties_offset = block->vinfo_offset + block->vinfo_length;
	as_rec_props props;

	props.p_data = block->data + properties_offset;
	props.size = block->bins_offset - properties_offset;

	bool is_ldt_sub;
	bool is_ldt_parent;

	ssd_record_get_ldt_property(&props, &is_ldt_parent, &is_ldt_sub);

	if (ssd->sub_sweep) {
		if (! is_ldt_sub) {
			cf_detail(AS_DRV_SSD, "LDT_LOAD Skipping parent records in the subrecord sweep %d %d",
					is_ldt_parent, is_ldt_sub);
			return 0;
		}
	}
	else {
		if (is_ldt_sub) {
			cf_detail(AS_DRV_SSD, "LDT_LOAD Skipping subrecord records in the non subrecord sweep %d %d",
					is_ldt_parent, is_ldt_sub);
			return 0;
		}
	}

	// Get/create the record from/in the appropriate index tree.
	int rv = as_record_get_create(
			is_ldt_sub ? p_partition->sub_vp : p_partition->vp,
					&block->keyd, &r_ref, ns);

	if (rv < 0) {
		cf_warning(AS_DRV_SSD, "record-add as_record_get_create() failed");
		return -1;
	}

	// Fix 0 generations coming off device.
	if (block->generation == 0) {
		block->generation = 1;
	}

	// Set 0 void-time to default, if there is one.
	if (block->void_time == 0 && ns->default_ttl != 0) {
		cf_debug(AS_DRV_SSD, "record-add changing 0 void-time to default");
		block->void_time = as_record_void_time_get() + ns->default_ttl;
	}

	as_index* r = r_ref.r;

	if (rv == 0) {
		// Record already existed. Perform generation check first, and ignore
		// this record if existing record is newer. Use void-times as tie-break
		// if generations are equal.
		if (block->generation < r->generation ||
				(block->generation == r->generation &&
						block->void_time <= r->void_time)) {
			cf_detail(AS_DRV_SSD, "record-add skipping generation %u <= existing %u",
					block->generation, r->generation);

			as_record_done(&r_ref, ns);
			ssd->record_add_generation_counter++;
			return -1;
		}
	}
	// The record we're now reading is the latest version (so far) ...

	// Set/reset the record's void-time and generation.
	r->void_time = block->void_time;
	r->generation = block->generation;

	if (r->void_time != 0) {
		// The threshold may be ~ now, or it may be in the future if eviction
		// has been happening.
		uint32_t threshold_void_time =
				cf_atomic32_get(ns->cold_start_threshold_void_time);

		// If the record is set to expire before the threshold, delete it.
		// (Note that if a record is skipped here, then later we encounter a
		// version with older generation but bigger (not expired) void-time,
		// that older version gets resurrected.)
		if (r->void_time < threshold_void_time) {
			cf_detail(AS_DRV_SSD, "record-add deleting void-time %u < threshold %u",
					r->void_time, threshold_void_time);

			as_index_delete(p_partition->vp, &block->keyd);
			as_record_done(&r_ref, ns);
			ssd->record_add_expired_counter++;
			return -1;
		}

		// If the record is beyond max-ttl, either it's rogue data (from
		// improperly coded clients) or it's data the users don't want anymore
		// (user decreased the max-ttl setting). No such check is needed for
		// the subrecords ...
		if (ns->max_ttl != 0 && ! is_ldt_sub) {
			if (r->void_time > ns->cold_start_max_void_time) {
				cf_debug(AS_DRV_SSD, "record-add deleting void-time %u > max %u",
						r->void_time, ns->cold_start_max_void_time);

				as_index_delete(p_partition->vp, &block->keyd);
				as_record_done(&r_ref, ns);
				ssd->record_add_max_ttl_counter++;
				return -1;
			}
		}
	}

	// We'll keep the record we're now reading ...

	// Update maximum void-times.
	cf_atomic_int_setmax(&p_partition->max_void_time, r->void_time);
	cf_atomic_int_setmax(&ns->max_void_time, r->void_time);

	if (props.size != 0) {
		// Do this early since set-id is needed for the secondary index update.
		as_record_apply_properties(r, ns, &props);
	}

	as_ldt_record_set_rectype_bits(r, &props);

	cf_detail(AS_RW, "TO INDEX FROM DISK	Digest=%"PRIx64" bits %d",
			*(uint64_t*)&block->keyd.digest[8],
			as_ldt_record_get_rectype_bits(r));

	// If data is in memory, load bins and particles.
	if (ns->storage_data_in_memory) {
		as_index_vinfo_mask_set(r,
				as_partition_vinfoset_mask_unpickle(p_partition,
						block->data + block->vinfo_offset, block->vinfo_length),
				ns->allow_versions);

		uint8_t* block_head = (uint8_t*)block;
		drv_ssd_bin* ssd_bin = (drv_ssd_bin*)(block->data + block->bins_offset);
		as_storage_rd rd;

		if (rv == 1) {
			as_storage_record_create(ns, r, &rd, &block->keyd);
		}
		else {
			as_storage_record_open(ns, r, &rd, &block->keyd);
		}

		rd.u.ssd.block = block;
		rd.have_device_block = true;
		rd.n_bins = as_bin_get_n_bins(r, &rd);
		rd.bins = as_bin_get_all(r, &rd, 0);

		uint64_t bytes_memory = as_storage_record_get_n_bytes_memory(&rd);
		uint16_t old_n_bins = rd.n_bins;

		bool has_sindex = as_sindex_ns_has_sindex(ns);
		SINDEX_BINS_SETUP(oldbin, rd.n_bins);
		SINDEX_BINS_SETUP(newbin, block->n_bins);
		int sindex_ret = AS_SINDEX_OK;
		int oldbin_cnt = 0;
		int newbin_cnt = 0;
		bool check_update = false;

		if (has_sindex) {
			SINDEX_GRLOCK();
		}

		if (! rd.ns->single_bin) {
			int32_t delta_bins = (int32_t)block->n_bins - (int32_t)rd.n_bins;

			if (delta_bins) {
				uint16_t new_size = (uint16_t)block->n_bins;
				uint16_t del_success = 0;

				if ((delta_bins < 0) && has_sindex) {
					sindex_ret = as_sindex_sbin_from_rd(&rd, new_size,
							old_n_bins, oldbin, &del_success);

					if (sindex_ret == AS_SINDEX_OK) {
						cf_detail(AS_DRV_SSD, "Expected sbin deletes : %d Actual sbin deletes: %d",
								-1 * delta_bins, del_success);
					}
					else {
						cf_warning(AS_DRV_SSD, "sbin delete failed: %s",
								as_sindex_err_str(sindex_ret));
					}
				}

				if (del_success) {
					check_update = true;
				}

				oldbin_cnt += del_success;
				as_bin_allocate_bin_space(r, &rd, delta_bins);
			}
		}

		for (uint16_t i = 0; i < block->n_bins; i++) {
			as_bin* b;

			if (i < old_n_bins) {
				b = &rd.bins[i];
				as_bin_set_version(b, ssd_bin->version, ns->single_bin);
				as_bin_set_id_from_name(ns, b, ssd_bin->name);
			}
			else {
				b = as_bin_create(r, &rd, (uint8_t*)ssd_bin->name,
						strlen(ssd_bin->name), ssd_bin->version);
			}

			if (has_sindex) {
				sindex_ret = as_sindex_sbin_from_bin(ns,
						as_index_get_set_name(r, ns), &rd.bins[i],
						&oldbin[oldbin_cnt]);

				if (sindex_ret == AS_SINDEX_OK) {
					oldbin_cnt++;
					check_update = true;
				}
				else if (sindex_ret != AS_SINDEX_ERR_NOTFOUND) {
					GTRACE(CALLER, debug, "Failed to get sbin");
				}
			}

			ssd_populate_bin(b, ssd_bin, block_head, ns->single_bin, true);
			ssd_bin = (drv_ssd_bin*)(block_head + ssd_bin->next);

			if (has_sindex) {
				sindex_ret = as_sindex_sbin_from_bin(ns,
						as_index_get_set_name(r, ns), &rd.bins[i],
						&newbin[newbin_cnt]);

				if (sindex_ret == AS_SINDEX_OK) {
					newbin_cnt++;
				}
				else if (sindex_ret != AS_SINDEX_ERR_NOTFOUND) {
					GTRACE(CALLER, debug, "Failed to get sbin");
					check_update = false;
				}

				// If values are updated, then check if both the values are the
				// same. If so, make it a no-op.
				if (check_update) {
					if (as_sindex_sbin_match(&newbin[newbin_cnt - 1], &oldbin[oldbin_cnt - 1])) {
						as_sindex_sbin_free(&newbin[newbin_cnt - 1]);
						as_sindex_sbin_free(&oldbin[oldbin_cnt - 1]);
						oldbin_cnt--;
						newbin_cnt--;
					}
				}
			}
		}

		if (has_sindex) {
			SINDEX_GUNLOCK();
			// Delete should precede insert.
			as_sindex_delete_by_sbin(ns, as_index_get_set_name(r, ns), oldbin_cnt, oldbin, &rd);
			as_sindex_put_by_sbin(ns, as_index_get_set_name(r, ns), newbin_cnt, newbin, &rd);
			as_sindex_sbin_freeall(oldbin, oldbin_cnt);
			as_sindex_sbin_freeall(newbin, newbin_cnt);
		}

		uint64_t end_bytes_memory = as_storage_record_get_n_bytes_memory(&rd);
		int64_t delta_bytes = end_bytes_memory - bytes_memory;

		if (delta_bytes) {
			cf_atomic_int_add(&ns->n_bytes_memory, delta_bytes);
			cf_atomic_int_add(&p_partition->n_bytes_memory, delta_bytes);
		}

		as_storage_record_close(r, &rd);
	}

	// If replacing an existing record, undo its previous storage accounting.
	if (STORAGE_RBLOCK_IS_VALID(r->storage_key.ssd.rblock_id)) {
		ssd_block_free(&ssds->ssds[r->storage_key.ssd.file_id],
				r->storage_key.ssd.rblock_id, r->storage_key.ssd.n_rblocks,
				FREE_TO_HEAD, "record-add");
	}

	// Set/reset the record's storage information.
	r->storage_key.ssd.file_id = ssd->file_id;
	r->storage_key.ssd.rblock_id = rblock_id;
	r->storage_key.ssd.n_rblocks = n_rblocks;

	// Make sure subrecord sweep happens.
	if (is_ldt_parent) {
		ssd->has_ldt = true;
	}

	as_record_done(&r_ref, ns);
	ssd->record_add_success_counter++;
	return 0;
}


typedef enum {
	ST_FREE,
	ST_ALLOC
} free_state_e;

// Sweep through storage devices and rebuild the index.
//
// If there are LDT records the sweep is done twice, once for LDT parent records
// and then again for LDT subrecords.
int
ssd_load_device_sweep(drv_ssds *ssds, drv_ssd *ssd)
{
	uint8_t *buf = cf_valloc(LOAD_BUF_SIZE);

	int fd = open(ssd->name, ssd->open_flag, S_IRUSR | S_IWUSR);

	if (-1 == fd) {
		cf_crash(AS_DRV_SSD, "unable to open device %s: %s",
				ssd->name, cf_strerror(errno));
	}

	// Seek past the header.
	off_t file_offset = ssds->header->header_length;

	lseek(fd, file_offset, SEEK_SET);

	free_state_e free_state = ST_ALLOC;
	uint64_t free_range_start = 0;
	int error_count = 0;

	// Loop over all blocks in device.
	while (true) {
		ssize_t rlen = read(fd, buf, LOAD_BUF_SIZE);

		if (rlen != LOAD_BUF_SIZE) {
			cf_warning(AS_DRV_SSD, "startup read failed: offset %"PRIu64" errno %d rv %zd",
					file_offset, errno, rlen);
			goto Finished;
		}

		size_t block_offset = 0; // current offset within the 1M block, in bytes

		while (block_offset < LOAD_BUF_SIZE) {
			drv_ssd_block *block = (drv_ssd_block*)&buf[block_offset];

			// Look for record magic.
			if (block->magic != SSD_BLOCK_MAGIC) {
				// No record found here.
				// (Includes normal case of nothing ever written here).

				if (free_state == ST_ALLOC) {
					free_state = ST_FREE;
					free_range_start = file_offset + block_offset;
				}

				block_offset += RBLOCK_SIZE;

				// We always write some at the start of a 1M block.
				if (block_offset == RBLOCK_SIZE) {
					error_count++;
					goto NextBlock;
				}
				else {
					// Otherwise check the next rblock, looking for magic.
					continue;
				}
			}

			// Note - if block->length is sane, we don't need to round up to a
			// multiple of RBLOCK_SIZE, but let's do it anyway just to be safe.
			size_t next_block_offset = block_offset +
					BYTES_TO_RBLOCK_BYTES(block->length + SIGNATURE_OFFSET);

			// Sanity-check for 1M block overruns.
			// TODO - check write_block_size boundaries!
			if (next_block_offset > LOAD_BUF_SIZE) {
				cf_warning(AS_DRV_SSD, "error: block extends over read size: foff %"PRIu64" boff %"PRIu64" blen %"PRIu64,
					file_offset,block_offset, (uint64_t)block->length);

				if (free_state == ST_ALLOC) {
					free_range_start = file_offset + block_offset;
					free_state = ST_FREE;
				}

				error_count++;
				goto NextBlock;

			}

			// Check signature.
			if (ssd->use_signature && block->sig) {
				cf_signature sig;

				cf_signature_compute(((uint8_t*)block) + SIGNATURE_OFFSET,
						block->length, &sig);

				if (sig != block->sig) {
					if (free_state == ST_ALLOC) {
						free_state = ST_FREE;
						free_range_start = file_offset + block_offset;
					}

					block_offset += RBLOCK_SIZE;
					ssd->record_add_sigfail_counter++;

					// Check the next rblock, looking for magic.
					continue;
				}
			}

			// Found a record - try to add it to the index.
			int add_rv = ssd_record_add(ssds, ssd, block,
					BYTES_TO_RBLOCKS(file_offset + block_offset),
					(uint32_t)BYTES_TO_RBLOCKS(next_block_offset - block_offset));

			if (add_rv == -2) {
				cf_warning(AS_DRV_SSD, "disk restore: hit high water limit before disk entirely loaded.");

				if (free_state == ST_ALLOC) {
					free_range_start = file_offset + block_offset;
					free_state = ST_FREE;
				}

				goto Finished;
			}

			if (add_rv == -3) {
				if (free_state == ST_ALLOC) {
					free_range_start = file_offset + block_offset;
					free_state = ST_FREE;
				}

				error_count++;
				goto NextBlock;
			}

			// Success - transition to ST_ALLOC state.
			if (add_rv == 0) {
				// If we were in free state, free previous free range found.
				if (free_state == ST_FREE) {
					uint64_t free_size =
							file_offset + block_offset - free_range_start;

					if (free_size == 0) {
						cf_warning(AS_DRV_SSD, "should never free 0 blocks, check logic");
					}

					ssd_block_free(ssd, BYTES_TO_RBLOCKS(free_range_start),
							BYTES_TO_RBLOCKS(free_size), FREE_TO_TAIL,
							"load devices");

					free_state = ST_ALLOC;
					free_range_start = 0;
				}
			}
			// Failure - transition to ST_FREE state.
			else {
				if (free_state == ST_ALLOC) {
					free_state = ST_FREE;
					free_range_start = file_offset + block_offset;
				}
			}

			error_count = 0;
			block_offset = next_block_offset;
		}

NextBlock:

		// If we encounter enough 1M blocks that have no records, assume we've
		// read all our data and we're done.
		if (error_count > 10) {
			goto Finished;
		}

		file_offset += LOAD_BUF_SIZE;
	}

Finished:

	if (fd != -1) {
		close(fd);
		fd = -1;
	}

	if (free_state == ST_FREE && (ssd->sub_sweep || ! ssd->has_ldt)) {
		// Handle everything at the end of the device.
		if (free_range_start < ssd->file_size) {
			cf_info(AS_DRV_SSD, "finished: marking blocks free: block %"PRIu64" nblocks %"PRIu64,
				BYTES_TO_RBLOCKS(free_range_start),
				BYTES_TO_RBLOCKS(ssd->file_size - free_range_start));

			// Use special "free to tail" mode.
			ssd_block_free(ssd, BYTES_TO_RBLOCKS(free_range_start),
				BYTES_TO_RBLOCKS(ssd->file_size - free_range_start),
				FREE_TO_TAIL, "load-devices2");
		}
	}

	cf_free(buf);

	return 0;
}


#ifdef USE_JEM
#include <sys/syscall.h>
#include <unistd.h>
#endif

typedef struct {
	drv_ssds *ssds;
	drv_ssd *ssd;
	cf_queue *complete_q;
	void *complete_udata;
	void *complete_rc;
} ssd_load_devices_data;

// Thread "run" function to read a device and rebuild the index.
void *
ssd_load_devices_fn(void *udata)
{
	ssd_load_devices_data *ldd = (ssd_load_devices_data*)udata;
	drv_ssd *ssd = ldd->ssd;
	drv_ssds *ssds = ldd->ssds;
	cf_queue *complete_q = ldd->complete_q;
	void *complete_udata = ldd->complete_udata;
	void *complete_rc = ldd->complete_rc;

	cf_free(ldd);
	ldd = 0;

	cf_info(AS_DRV_SSD, "load device start: device %s", ssd->name);

#ifdef USE_JEM
	int tid = syscall(SYS_gettid);
	cf_info(AS_DRV_SSD, "In TID %d: Using arena #%d for loading data for namespace \"%s\"",
			tid, ssds->ns->jem_arena, ssds->ns->name);

	// Allocate long-term storage in this namespace's JEMalloc arena.
	jem_set_arena(ssds->ns->jem_arena);
#endif

	ssd->sub_sweep	= false;
	ssd->has_ldt	= false;

	cf_detail(AS_DRV_SSD, "LDT_LOAD Performing Primary Sweep");
	ssd_load_device_sweep(ssds, ssd);

	if (ssds->ns->ldt_enabled && ssd->has_ldt) {
		cf_detail(AS_DRV_SSD, "LDT_LOAD Performing Secondary Sweep");
		ssd->sub_sweep = true;
		ssd_load_device_sweep(ssds, ssd);
	}

	cf_info(AS_DRV_SSD, "device %s: read complete: READ %"PRIu64" (GEN %"PRIu64") (EXPIRED %"PRIu64") (MAX-TTL %"PRIu64") records",
		ssd->name, ssd->record_add_success_counter,
		ssd->record_add_generation_counter, ssd->record_add_expired_counter,
		ssd->record_add_max_ttl_counter);

	off_t free, contig;
	ssd_block_get_free_size(ssd, &free, &contig);

	cf_info(AS_DRV_SSD, "device %s: read complete: free %"PRIu64"M contig %"PRIu64"M",
			ssd->name,free / (1024 * 1024), contig / (1024 * 1024));

	if (ssd->record_add_sigfail_counter) {
		cf_warning(AS_DRV_SSD, "devices %s: WARNING: %"PRIu64" elements could not be read due to signature failure. Possible hardware errors.",
			ssd->record_add_sigfail_counter);
	}

	if (0 == cf_rc_release(complete_rc)) {
		// All drives are done reading.

		pthread_mutex_destroy(&ssds->ns->cold_start_evict_lock);

		cf_queue_push(complete_q, &complete_udata);
		cf_rc_free(complete_rc);

		ssd_start_track_free_threads(ssds);
		ssd_start_write_worker_threads(ssds);
		ssd_start_defrag_threads(ssds);
	}

	return 0;
}


void
ssd_load_devices_load(drv_ssds *ssds, cf_queue *complete_q, void *udata)
{
	drv_ssd *ssd;

	void *p = cf_rc_alloc(1);

	for (int i = 1; i < ssds->n_ssds; i++) {
		cf_rc_reserve(p);
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		ssd = &ssds->ssds[i];

		ssd_load_devices_data *ldd = cf_malloc(sizeof(ssd_load_devices_data));

		if (!ldd) {
			cf_crash(AS_DRV_SSD, "memory allocation in device load");
		}

		ldd->ssds = ssds;
		ldd->ssd = ssd;
		ldd->complete_q = complete_q;
		ldd->complete_udata = udata;
		ldd->complete_rc = p;
		pthread_create(& ssd->load_device_thread, 0, ssd_load_devices_fn, ldd);
	}
}


void
ssd_load_devices_init_free(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "load devices init free: %d devices", ssds->n_ssds);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		cf_info(AS_DRV_SSD, "load devices init free: %s hlen %d freesize %"PRIu64,
				ssd->name, ssds->header->header_length,
				ssd->file_size - ssds->header->header_length);

		ssd_block_free(ssd, BYTES_TO_RBLOCKS(ssds->header->header_length),
				BYTES_TO_RBLOCKS(ssd->file_size - ssds->header->header_length),
				FREE_TO_TAIL, "init-free");
	}
}


void
ssd_load_devices_init_header_length(drv_ssds *ssds)
{
	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->header_size = ssds->header->header_length;
	}
}


void
ssd_load_devices_init_inuse_size(drv_ssds *ssds)
{
	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->inuse_size = ssd->file_size - ssds->header->header_length;
	}
}


//==========================================================
// Generic startup utilities.
//

static int
first_used_device(ssd_device_header *headers[], int n_ssds)
{
	for (int i = 0; i < n_ssds; i++) {
		if (headers[i]->random != 0) {
			return i;
		}
	}

	return -1;
}

bool
ssd_load_devices(drv_ssds *ssds, cf_queue *complete_q, void *udata)
{
	uint64_t random = cf_get_rand64();

	int n_ssds = ssds->n_ssds;
	as_namespace *ns = ssds->ns;

	ssd_device_header *headers[n_ssds];

	// Check all the headers. Pick one as the representative.
	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];
		int rvh = as_storage_read_header(ssd, ns, &headers[i]);

		if (rvh == -1) {
			cf_crash(AS_DRV_SSD, "unable to read disk header %s: %s",
					ssd->name, cf_strerror(errno));
		}

		if (rvh == -2) {
			headers[i] = as_storage_init_header(ns);
		}
	}

	int first_used = first_used_device(headers, n_ssds);

	if (first_used == -1) {
		// Shouldn't find all fresh headers here during warm restart.
		if (! ns->cold_start) {
			// There's no going back to cold start now - do so the harsh way.
			cf_crash(AS_DRV_SSD, "ns %s: found all %d devices fresh during warm restart",
					ns->name, n_ssds);
		}

		cf_info(AS_DRV_SSD, "namespace %s: found all %d devices fresh, initializing to random %"PRIu64,
				ns->name, n_ssds, random);

		ssds->header = headers[0];

		for (int i = 1; i < n_ssds; i++) {
			cf_free(headers[i]);
		}

		ssds->header->random = random;
		ssds->header->devices_n = n_ssds;
		as_storage_info_flush_ssd(ns);

		ssd_load_devices_init_header_length(ssds);
		ssd_load_devices_init_inuse_size(ssds);
		ssd_load_devices_init_free(ssds);

		return true;
	}

	// At least one device is not fresh. Check that all non-fresh devices match.

	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		// Skip fresh devices.
		if (headers[i]->random == 0) {
			ssd->started_fresh = true; // warm restart needs to know
			continue;
		}

		if (headers[first_used]->random != headers[i]->random) {
			cf_crash(AS_DRV_SSD, "namespace %s: drive set with unmatched headers - devices %s & %s have different signatures",
					ns->name, ssds->ssds[first_used].name, ssd->name);
		}

		if (headers[first_used]->devices_n != headers[i]->devices_n) {
			cf_crash(AS_DRV_SSD, "namespace %s: drive set with unmatched headers - devices %s & %s have different device counts",
					ns->name, ssds->ssds[first_used].name, ssd->name);
		}

		if (headers[first_used]->last_evict_void_time !=
				headers[i]->last_evict_void_time) {
			cf_warning(AS_DRV_SSD, "namespace %s: devices have inconsistent evict-void-times - ignoring",
					ns->name);
			headers[first_used]->last_evict_void_time = 0;
		}
	}

	// Drive set OK - fix up header set.
	ssds->header = headers[first_used];
	headers[first_used] = 0;

	for (int i = 0; i < n_ssds; i++) {
		if (headers[i]) {
			cf_free(headers[i]);
			headers[i] = 0;
		}
	}

	ssds->header->random = random;
	ssds->header->devices_n = n_ssds; // may have added fresh drives
	as_storage_info_flush_ssd(ns);

	as_partition_get_state_from_storage(ssds->ns, ssds->get_state_from_storage);

	ssd_load_devices_init_header_length(ssds);

	// Warm restart - imitate device loading by reducing resumed index.
	if (! ns->cold_start) {
		ssd_resume_devices(ssds);

		return true;
	}

	ssd_load_devices_init_inuse_size(ssds);
	// TODO - equivalent of ssd_load_devices_init_free() for fresh drives to
	// avoid reading them? Or, if we find valid data on drive we think is fresh,
	// then crash out?

	// Initialize the cold-start eviction machinery.

	if (0 != pthread_mutex_init(&ns->cold_start_evict_lock, NULL)) {
		cf_crash(AS_DRV_SSD, "failed cold-start eviction mutex init");
	}

	uint32_t now = as_record_void_time_get();

	if (ns->cold_start_evict_ttl == 0xFFFFffff) {
		// Config file did NOT specify cold-start-evict-ttl.
		ns->cold_start_threshold_void_time = ssds->header->last_evict_void_time;

		// Check that it's not already in the past. (Note - includes 0.)
		if (ns->cold_start_threshold_void_time < now) {
			ns->cold_start_threshold_void_time = now;
		}
		else {
			cf_info(AS_DRV_SSD, "namespace %s: using saved cold start evict-ttl %u",
					ns->name, ns->cold_start_threshold_void_time - now);
		}
	}
	else {
		// Config file specified cold-start-evict-ttl. (0 is a valid value.)
		ns->cold_start_threshold_void_time = now + ns->cold_start_evict_ttl;

		cf_info(AS_DRV_SSD, "namespace %s: using config-specified cold start evict-ttl %u",
				ns->name, ns->cold_start_evict_ttl);
	}

	ns->cold_start_max_void_time = now + (uint32_t)ns->max_ttl;

	// Fire off threads to load in data - will signal completion when threads
	// are all done.
	ssd_load_devices_load(ssds, complete_q, udata);

	// Make sure caller doesn't signal completion.
	return false;
}


// Set a device's system block scheduler mode.
static int
ssd_set_scheduler_mode(const char* device_name, const char* mode)
{
	if (strncmp(device_name, "/dev/", 5)) {
		cf_warning(AS_DRV_SSD, "storage: invalid device name %s, did not set scheduler mode",
				device_name);
		return -1;
	}

	char device_tag[(strlen(device_name) - 5) + 1];

	strcpy(device_tag, device_name + 5);

	// Replace any slashes in the device tag with '!' - this is the naming
	// convention in /sys/block.
	char* p_char = device_tag;

	while (*p_char) {
		if (*p_char == '/') {
			*p_char = '!';
		}

		p_char++;
	}

	// If the device name ends with a number, assume it's a partition name and
	// removing the number gives the raw device name.
	if (p_char != device_tag) {
		p_char--;

		if (*p_char >= '0' && *p_char <= '9') {
			*p_char = 0;
		}
	}

	char scheduler_file_name[11 + strlen(device_tag) + 16 + 1];

	strcpy(scheduler_file_name, "/sys/block/");
	strcat(scheduler_file_name, device_tag);
	strcat(scheduler_file_name, "/queue/scheduler");

	FILE* scheduler_file = fopen(scheduler_file_name, "w");

	if (! scheduler_file) {
		cf_warning(AS_DRV_SSD, "storage: couldn't open %s, did not set scheduler mode: %s",
				scheduler_file_name, cf_strerror(errno));
		return -1;
	}

	if (fwrite(mode, strlen(mode), 1, scheduler_file) != 1) {
		fclose(scheduler_file);

		cf_warning(AS_DRV_SSD, "storage: couldn't write %s to %s, did not set scheduler mode",
				mode, scheduler_file_name);
		return -1;
	}

	fclose(scheduler_file);

	// TODO - shouldn't this be promoted to info?
	cf_debug(AS_DRV_SSD, "storage: set %s scheduler mode to %s",
			device_name, mode);

	return 0;
}


static uint32_t
check_write_block_size(uint32_t write_block_size)
{
	if (write_block_size > MAX_WRITE_BLOCK_SIZE) {
		cf_crash(AS_DRV_SSD, "attempted to configure write block size in excess of %u",
				MAX_WRITE_BLOCK_SIZE);
	}

	if (LOAD_BUF_SIZE % write_block_size != 0 ||
			SSD_DEFAULT_HEADER_LENGTH % write_block_size != 0) {
		cf_crash(AS_DRV_SSD, "attempted to configure non-round write block size %u",
				write_block_size);
	}

	return write_block_size;
}


static off_t
check_file_size(off_t file_size, const char *tag)
{
	if (sizeof(off_t) <= 4) {
		cf_warning(AS_DRV_SSD, "this OS supports only 32-bit (4g) files - compile with 64 bit offsets");
	}

	if (file_size > SSD_DEFAULT_HEADER_LENGTH) {
		off_t unusable_size =
				(file_size - SSD_DEFAULT_HEADER_LENGTH) % LOAD_BUF_SIZE;

		if (unusable_size != 0) {
			cf_info(AS_DRV_SSD, "%s size must be header size %u + multiple of %u, rounding down",
					tag, SSD_DEFAULT_HEADER_LENGTH, LOAD_BUF_SIZE);
			file_size -= unusable_size;
		}
	}

	if (file_size <= SSD_DEFAULT_HEADER_LENGTH) {
		cf_crash(AS_DRV_SSD, "%s size %"PRIu64" must be greater than header size %d",
				tag, file_size, SSD_DEFAULT_HEADER_LENGTH);
	}

	return file_size;
}


int
ssd_init_devices(as_namespace *ns, drv_ssds **ssds_p)
{
	int n_ssds;

	for (n_ssds = 0; n_ssds < AS_STORAGE_MAX_DEVICES; n_ssds++) {
		if (! ns->storage_devices[n_ssds]) {
			break;
		}
	}

	if (n_ssds == 0) {
		cf_warning(AS_DRV_SSD, "storage: no devices, bad config");
		return -1;
	}

	drv_ssds *ssds = cf_malloc(sizeof(drv_ssds) + (n_ssds * sizeof(drv_ssd)));

	if (! ssds) {
		return -1;
	}

	memset(ssds, 0, sizeof(drv_ssds) + (n_ssds * sizeof(drv_ssd)));
	ssds->n_ssds = n_ssds;
	ssds->ns = ns;
	ssds->header = 0; // created in load function

	ns->ssd_size = 0;

	// Initialize the devices.
	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		strcpy(ssd->name, ns->storage_devices[i]);

		ssd->open_flag = O_DIRECT | O_RDWR;

		if (ns->storage_disable_odirect) {
			ssd->open_flag = O_RDWR;
		}

		ssd->use_signature = ns->storage_signature;
		ssd->data_in_memory = ns->storage_data_in_memory;
		ssd->readonly = ns->storage_readonly;
		ssd->write_block_size =
				check_write_block_size(ns->storage_write_block_size);
		ssd->file_id = i;

		int fd = open(ssd->name, ssd->open_flag, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_warning(AS_DRV_SSD, "unable to open device %s: %s", ssd->name,
					cf_strerror(errno));
			return -1;
		}

		uint64_t size = 0;

		ioctl(fd, BLKGETSIZE64, &size); // gets the number of bytes
		close(fd);

		ssd->file_size = check_file_size((off_t)size, "usable device");

		ns->ssd_size += ssd->file_size; // increment total storage size

		cf_info(AS_DRV_SSD, "Opened device %s bytes %"PRIu64, ssd->name,
				ssd->file_size);

		if (ns->storage_scheduler_mode) {
			// Set scheduler mode specified in config file.
			ssd_set_scheduler_mode(ssd->name, ns->storage_scheduler_mode);
		}
	}

	*ssds_p = ssds;

	return 0;
}


int
ssd_init_files(as_namespace *ns, drv_ssds **ssds_p)
{
	int n_ssds;

	for (n_ssds = 0; n_ssds < AS_STORAGE_MAX_FILES; n_ssds++) {
		if (! ns->storage_files[n_ssds]) {
			break;
		}
	}

	if (n_ssds == 0) {
		cf_warning(AS_DRV_SSD, "storage: no files, bad config");
		return -1;
	}

	drv_ssds *ssds = cf_malloc(sizeof(drv_ssds) + (n_ssds * sizeof(drv_ssd)));

	if (! ssds) {
		return -1;
	}

	memset(ssds, 0, sizeof(drv_ssds) + (n_ssds * sizeof(drv_ssd)));
	ssds->n_ssds = n_ssds;
	ssds->ns = ns;

	ns->ssd_size = 0;

	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		strcpy(ssd->name, ns->storage_files[i]);

		ssd->open_flag = O_RDWR;
		ssd->use_signature = ns->storage_signature;
		ssd->data_in_memory = ns->storage_data_in_memory;
		ssd->readonly = ns->storage_readonly;
		ssd->write_block_size =
				check_write_block_size(ns->storage_write_block_size);
		ssd->file_id = i;
		ssd->file_size = check_file_size(ns->storage_filesize, "file");

		// Validate that this file can be opened.
		int fd = open(ssd->name, ssd->open_flag | O_CREAT, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_warning(AS_DRV_SSD, "unable to open file %s: %s", ssd->name,
					cf_strerror(errno));
			return -1;
		}

		// Truncate will grow or shrink the file to the correct size.
		if (0 != ftruncate(fd, ssd->file_size)) {
			cf_info(AS_DRV_SSD, "unable to truncate file: errno %d", errno);
			return -1;
		}

		close(fd);

		ns->ssd_size += ssd->file_size; // increment total storage size

		cf_info(AS_DRV_SSD, "Opened file %s bytes %"PRIu64, ssd->name,
				ssd->file_size);
	}

	*ssds_p = ssds;

	return 0;
}


//==========================================================
// Storage API implementation: startup, shutdown, etc.
//

int
as_storage_namespace_init_ssd(as_namespace *ns, cf_queue *complete_q,
		void *udata)
{
	drv_ssds *ssds;

	if (ns->storage_devices[0] != 0) {
		if (0 != ssd_init_devices(ns, &ssds)) {
			cf_warning(AS_DRV_SSD, "ns %s can't initialize devices", ns->name);
			return -1;
		}
	}
	else if (ns->storage_files[0] != 0) {
		if (0 != ssd_init_files(ns, &ssds)) {
			cf_warning(AS_DRV_SSD, "ns %s can't initialize files", ns->name);
			return -1;
		}
	}
	else {
		cf_warning(AS_DRV_SSD, "namespace %s: neither device nor files configured, fatal error",
				ns->name);
		return -1;
	}

	// Allow defrag to go full speed during startup - restore the configured
	// settings when startup is done.
	ns->saved_defrag_period = ns->storage_defrag_period;
	ns->saved_write_smoothing_period = ns->storage_write_smoothing_period;
	ns->storage_defrag_period = 1;
	ns->storage_write_smoothing_period = 0;

	// The queue limit is more efficient to work with.
	ns->storage_max_write_q = (int)
			(ns->storage_max_write_cache / ns->storage_write_block_size);

	ns->storage_private = (void*)ssds;

	// Create files, open file descriptors, etc.
	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->ns = ns;

		pthread_mutex_init(&ssd->LOCK, 0);

		ssd->running = true;
		ssd->current_swb = 0;

		// Initialize ssd_alloc_table.
		ssd_wblock_init(ssd, ns->cold_start ? ssd->write_block_size : 0);

		ssd->fd_q = cf_queue_create(sizeof(int), true);

		ssd->file_id = i;
		ssd->inuse_size = 0;

		ssd->swb_write_q = 0;
		ssd->swb_free_q = 0;
		ssd->post_write_q = 0;

		if (! ns->storage_readonly) {
			ssd->swb_write_q = cf_queue_create(sizeof(void*), true);
			ssd->swb_free_q = cf_queue_create(sizeof(void*), true);

			if (ssd->swb_write_q == 0 || ssd->swb_free_q == 0) {
				cf_crash(AS_DRV_SSD, "can't create queue");
			}

			if (! ns->storage_data_in_memory) {
				ssd->post_write_q = cf_queue_create(sizeof(void*), false);

				if (! ssd->post_write_q) {
					cf_crash(AS_DRV_SSD, "can't create post-write queue");
				}
			}
		}

		ssd->ssd_write_buf_counter = 0;

		char histname[HISTOGRAM_NAME_SIZE];
		snprintf(histname, sizeof(histname), "SSD_READ_%d %s", i, ssd->name);
		ssd->hist_read = histogram_create(histname);
		if (! ssd->hist_read) {
			cf_warning(AS_DRV_SSD, "cannot create histogram %s", histname);
		}
		snprintf(histname, sizeof(histname), "SSD_LARGE_BLOCK_READ_%d %s", i, ssd->name);
		ssd->hist_large_block_read = histogram_create(histname);
		if (! ssd->hist_large_block_read) {
			cf_warning(AS_DRV_SSD,"cannot create histogram %s", histname);
		}
		snprintf(histname, sizeof(histname), "SSD_WRITE_%d %s", i, ssd->name);
		ssd->hist_write = histogram_create(histname);
		if (! ssd->hist_write) {
			cf_warning(AS_DRV_SSD, "cannot create histogram %s", histname);
		}
	}

	// Attempt to load the data.
	//
	// Return value 'false' means it's going to take a long time and will later
	// asynchronously signal completion via the complete_q, 'true' means it's
	// finished, signal here.

	if (ssd_load_devices(ssds, complete_q, udata)) {
		cf_queue_push(complete_q, &udata);

		ssd_start_track_free_threads(ssds);
		ssd_start_write_worker_threads(ssds);
		ssd_start_defrag_threads(ssds);
	}

	return 0;
}


int
as_storage_namespace_destroy_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		while (true) {
			int fd = ssd_fd_get(ssd);

			if (fd == -1) {
				break;
			}

			close(fd);
		}

		pthread_mutex_destroy(&ssd->LOCK);
	}

	cf_free(ssds);

	return 0;
}


int
as_storage_namespace_attributes_get_ssd(as_namespace *ns,
		as_storage_attributes *attr)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	attr->n_devices = ssds->n_ssds;

	return 0;
}


// Note that this is *NOT* the counterpart to as_storage_record_create_ssd()!
// That would be as_storage_record_close_ssd(). This is what gets called when a
// record is destroyed, to dereference storage.
int
as_storage_record_destroy_ssd(as_namespace *ns, as_record *r)
{
	if (STORAGE_RBLOCK_IS_VALID(r->storage_key.ssd.rblock_id) &&
			r->storage_key.ssd.n_rblocks != 0) {
		drv_ssds *ssds = (drv_ssds*)ns->storage_private;
		drv_ssd *ssd = &ssds->ssds[r->storage_key.ssd.file_id];

		ssd_block_free(ssd, r->storage_key.ssd.rblock_id,
				r->storage_key.ssd.n_rblocks, FREE_TO_HEAD, "destroy");

		r->storage_key.ssd.rblock_id = STORAGE_INVALID_RBLOCK;
		r->storage_key.ssd.n_rblocks = 0;
	}

	return 0;
}


//==========================================================
// Storage API implementation: as_storage_rd cycle.
//

int
as_storage_record_create_ssd(as_namespace *ns, as_record *r, as_storage_rd *rd,
		cf_digest *keyd)
{
	rd->u.ssd.block = 0;
	rd->u.ssd.must_free_block = NULL;
	rd->u.ssd.ssd = 0;

	r->storage_key.ssd.file_id = STORAGE_INVALID_FILE_ID;
	r->storage_key.ssd.rblock_id = STORAGE_INVALID_RBLOCK;
	r->storage_key.ssd.n_rblocks = 0;

	return 0;
}


int
as_storage_record_open_ssd(as_namespace *ns, as_record *r, as_storage_rd *rd,
		cf_digest *keyd)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	if (r->storage_key.ssd.file_id == STORAGE_INVALID_FILE_ID) {
		r->storage_key.ssd.file_id = ssd_get_file_id(ssds, keyd);
	}

	rd->u.ssd.block = 0;
	rd->u.ssd.must_free_block = NULL;
	rd->u.ssd.ssd = &ssds->ssds[r->storage_key.ssd.file_id];

	return 0;
}


void
as_storage_record_close_ssd(as_record *r, as_storage_rd *rd)
{
	// All record writes come through here!
	if (rd->write_to_device && as_bin_inuse_has(rd)) {
		ssd_write(r, rd);
	}

	if (rd->u.ssd.must_free_block) {
		cf_free(rd->u.ssd.must_free_block);
	}
}


// These are near the top of this file:
//		as_storage_record_get_n_bins_ssd()
//		as_storage_record_read_ssd()
//		as_storage_particle_read_all_ssd()
//		as_storage_particle_read_and_size_all_ssd()


bool
as_storage_record_can_fit_ssd(as_storage_rd *rd)
{
	rd->flat_size =
			as_storage_record_overhead_size(rd) +
			(rd->n_bins_to_write * sizeof(drv_ssd_bin)) +
			rd->particles_flat_size;

	return rd->ns->storage_write_block_size >= rd->flat_size;
}


// Currently record overhead is: size of drv_ssd_block (64 bytes) + max set name
// size (64 bytes) + size of its as_rec_prop_field (8 bytes?) = 136 bytes. But
// let's leave some room for more rec-props... Like key and LDT info...
#define RECORD_STORAGE_OVERHEAD 512

// TODO - pointless since we really need to look at all the bins - deprecate?
bool
as_storage_bin_can_fit_ssd(as_namespace *ns, uint32_t bin_data_size)
{
	uint32_t overhead = RECORD_STORAGE_OVERHEAD + sizeof(drv_ssd_bin);

	return ns->storage_write_block_size >= overhead &&
			bin_data_size <= ns->storage_write_block_size - overhead;
}


//==========================================================
// Storage API implementation: storage capacity monitoring.
//

void
as_storage_wait_for_defrag_ssd(as_namespace *ns)
{
	if (ns->storage_defrag_startup_minimum > 0) {
		while (true) {
			int avail_pct;

			if (0 != as_storage_stats_ssd(ns, &avail_pct, 0)) {
				cf_crash(AS_DRV_SSD, "namespace %s storage stats failed",
						ns->name);
			}

			if (avail_pct >= ns->storage_defrag_startup_minimum) {
				break;
			}

			cf_info(AS_DRV_SSD, "namespace %s waiting for defrag: %d pct available, waiting for %d ...",
					ns->name, avail_pct, ns->storage_defrag_startup_minimum);

			sleep(2);
		}
	}

	// Restore configured defrag throttling values.
	ns->storage_defrag_period = ns->saved_defrag_period;
	ns->storage_write_smoothing_period = ns->saved_write_smoothing_period;

	// Set the "floor" for wblock usage. Must come after startup defrag so it
	// doesn't prevent defrag from resurrecting a drive that hit the floor.

	int n_transaction_threads = g_config.use_queue_per_device ?
			g_config.n_transaction_threads_per_queue :
			g_config.n_transaction_queues * g_config.n_transaction_threads_per_queue;

	ns->storage_min_free_wblocks =
			n_transaction_threads +		// client writes
			g_config.n_fabric_workers +	// migration and prole writes
			1 +							// always 1 defrag thread
			8;							// reserve for defrag at startup
	// TODO - what about UDFs?

	cf_info(AS_DRV_SSD, "{%s} floor set at %u wblocks per device", ns->name,
			ns->storage_min_free_wblocks);
}

bool
as_storage_overloaded_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	int max_write_q = ns->storage_max_write_q;

	// TODO - would be nice to not do this loop every single write transaction!
	for (int i = 0; i < ssds->n_ssds; i++) {
		int qsz = cf_queue_sz(ssds->ssds[i].swb_write_q);

		if (qsz > max_write_q) {
			cf_atomic_int_incr(&g_config.err_storage_queue_full);
			cf_warning(AS_DRV_SSD, "{%s} write fail: queue too deep: q %d, max %d",
					ns->name, qsz, max_write_q);
			return true;
		}
	}

	return false;
}


bool
as_storage_has_space_ssd(as_namespace *ns)
{
	// Shortcut - assume we can't go from 5% to 0% in 1 ticker interval.
	if (ns->storage_last_avail_pct > 5) {
		return true;
	}
	// else - running low on available percent, check rigorously...

	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		if (cf_queue_sz(ssds->ssds[i].swb_free_q) <
				ns->storage_min_free_wblocks) {
			return false;
		}
	}

	return true;
}


//==========================================================
// Storage API implementation: data in device headers.
//

int
as_storage_info_set_ssd(as_namespace *ns, uint idx, uint8_t *buf, size_t len)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	if (ssds == 0 || ssds->header == 0 || ssds->header->info_data == 0) {
		cf_info(AS_DRV_SSD, "illegal ssd header in namespace %s", ns->name);
		return -1;
	}

	if (idx > ssds->header->info_n)	{
		cf_info(AS_DRV_SSD, "storage info set failed: idx %d", idx);
		return -1;
	}
	if (len > ssds->header->info_stride - sizeof(info_buf)) {
		cf_info(AS_DRV_SSD, "storage info set failed: bad length %d", len);
		return -1;
	}

	info_buf *b = (info_buf*)
			(ssds->header->info_data + (ssds->header->info_stride * idx));

	b->len = len;
	memcpy(b->data, buf, len);

	return 0;
}


int
as_storage_info_get_ssd(as_namespace *ns, uint idx, uint8_t *buf, size_t *len)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	if (ssds == 0 || ssds->header == 0 || ssds->header->info_data == 0) {
		cf_info(AS_DRV_SSD, "illegal ssd header in namespace %s", ns->name);
		return -1;
	}

	if (idx > ssds->header->info_n)	{
		cf_info(AS_DRV_SSD, "storage info get ssd: failed: idx %d too large",
				idx);
		return -1;
	}

	info_buf *b = (info_buf*)
			(ssds->header->info_data + (ssds->header->info_stride * idx));

	if (b->len > *len || b->len > ssds->header->info_stride) {
		cf_info(AS_DRV_SSD, "storage info get ssd: bad length: from disk %d input %d stride %d",
			b->len, *len, ssds->header->info_stride);
		return -1;
	}

	*len = b->len;

	if (b->len) {
		memcpy(buf, b->data, b->len);
	}

	return 0;
}


int
as_storage_info_flush_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		as_storage_write_header(ssd, ssds->header);
	}

	return 0;
}


void
as_storage_save_evict_void_time_ssd(as_namespace *ns, uint32_t evict_void_time)
{
	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	ssds->header->last_evict_void_time = evict_void_time;

	// Customized write instead of using as_storage_info_flush_ssd() so we can
	// write 512b instead of 1Mb (and not interfere with potentially concurrent
	// writes for partition info), and so we can avoid fsync() which is slow.

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd* ssd = &ssds->ssds[i];

		if (ssd->readonly) {
			continue;
		}

		int fd = open(ssd->name, ssd->open_flag, S_IRUSR | S_IWUSR);

		if (fd <= 0) {
			cf_warning(AS_DRV_SSD, "device %s: unable to open - %s",
					ssd->name, cf_strerror(errno));
			continue;
		}

		lseek(fd, 0, SEEK_SET);

		size_t peek_size =
				BYTES_UP_TO_SYS_RBLOCK_BYTES(sizeof(ssd_device_header));
		ssize_t sz = write(fd, (void*)ssds->header, peek_size);

		if (sz != peek_size) {
			cf_warning(AS_DRV_SSD, "device %s: failed write to tip of header",
					ssd->name);
			close(fd);
			continue;
		}

		close(fd);
	}
}


//==========================================================
// Storage API implementation: statistics.
//

int
as_storage_stats_ssd(as_namespace *ns, int *available_pct,
		uint64_t *used_disk_bytes)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	// Find the device with the lowest available percent.
	if (available_pct) {
		*available_pct = 100;

		for (int i = 0; i < ssds->n_ssds; i++) {
			drv_ssd *ssd = &ssds->ssds[i];
			off_t contig;

			ssd_block_get_free_size(ssd, NULL, &contig);
			contig = (contig * 100) / ssd->file_size;

			if (contig < *available_pct) {
				*available_pct = contig;
			}
		}

		// Used for shortcut in as_storage_has_space_ssd(), which is done on a
		// per-transaction basis:
		ns->storage_last_avail_pct = *available_pct;
	}

	if (used_disk_bytes) {
		uint64_t sz = 0;

		for (int i = 0; i < ssds->n_ssds; i++) {
			sz += ssds->ssds[i].inuse_size;
		}

		*used_disk_bytes = sz;
	}

	return 0;
}


int
as_storage_ticker_stats_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		if (ssd->hist_read) {
			histogram_dump(ssd->hist_read);
		}

		if (ssd->hist_large_block_read) {
			histogram_dump(ssd->hist_large_block_read);
		}

		if (ssd->hist_write) {
			histogram_dump(ssd->hist_write);
		}
	}

	return 0;
}


int
as_storage_histogram_clear_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		histogram_clear(ssd->hist_read);
		histogram_clear(ssd->hist_large_block_read);
		histogram_clear(ssd->hist_write);
	}

	return 0;
}


//==========================================================
// Shutdown.
//

void
as_storage_shutdown_ssd(as_namespace *ns)
{
	ns->storage_write_smoothing_period = 0;

	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		// Flush current swb by pushing it to write-q.
		if (ssd->current_swb) {
			// Clean the end of the buffer before pushing to write-q.
			if (ssd->write_block_size > ssd->current_swb->pos) {
				memset(&ssd->current_swb->buf[ssd->current_swb->pos], 0,
						ssd->write_block_size - ssd->current_swb->pos);
			}

			cf_queue_push(ssd->swb_write_q, &ssd->current_swb);
			ssd->current_swb = NULL;
		}
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		while (cf_queue_sz(ssd->swb_write_q)) {
			usleep(1000);
		}

		ssd->running = false;
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		for (uint32_t j = 0; j < ssds->ns->storage_write_threads; j++) {
			void *p_void;

			pthread_join(ssd->write_worker_thread[j], &p_void);
		}
	}
}
