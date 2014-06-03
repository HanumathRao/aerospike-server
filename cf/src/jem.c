/*
 * jem.c
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
 * JEMalloc Interface.
 */

#include "jem.h"

#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <jemalloc/jemalloc.h>
#include <sys/syscall.h>

#include "fault.h"

/* SYNOPSIS
 *  This is the implementation of a simple interface to JEMalloc.
 *  It provides a higher-level interface to working with JEMalloc arenas
 *  and the thread caching feature.
 *
 *  To enable these functions, first call "jem_init(true)".  Otherwise,
 *  and by default, the use of these JEMalloc features is disabled, and
 *  all of these functions do nothing but return a failure status code (-1),
 *  or in the case of "jem_allocate_in_arena()", will simply use "malloc(3)",
 *  which may be bound to JEMalloc's "malloc(3)", but will disregard the
 *  arguments other than size.
 *
 *  These functions use JEMalloc "MIB"s internally instead of strings for
 *  efficiency.
 */

/*
 *  Set the default JEMalloc configuration options.
 *
 *  Ideally, the nuber of arenas should be >= # threads so that every thread
 *   can have its own arena, and then we can use this JEM API to share arenas
 *   across threads as needed for guaranteeing data locality.
 *
 *  N.B.:  These default options can be overriden at run time via setting
 *          the "MALLOC_CONF" environment variable and/or the "name" of
 *          the "/etc/malloc.conf" symbolic link.
 */
const char *malloc_conf = "narenas:150";

/*
 *  Is the JEMalloc interface enabled?  (By default, no.)
 */
static bool jem_enabled = false;

/*
 *  JEMalloc MIB values for the "arenas.extend" control.
 */
static size_t arenas_extend_mib[2], arenas_extend_miblen = sizeof(arenas_extend_mib);

/*
 *  JEMalloc MIB values for the "thread.arena" control.
 */
static size_t thread_arena_mib[2], thread_arena_miblen = sizeof(thread_arena_mib);

/*
 *  JEMalloc MIB values for the "thread.tcache.enabled" control.
 */
static size_t thread_tcache_enabled_mib[3], thread_tcache_enabled_miblen = sizeof(thread_tcache_enabled_mib);

/*
 *  Initialize the interface to JEMalloc.
 *  If enable is true, the JEMalloc features will be enabled, otherwise they will be disabled.
 *  Returns 0 if successful, -1 otherwise.
 */
int jem_init(bool enable)
{
	if (enable) {
		// Initialize JEMalloc MIBs.
		char *mib = "arenas.extend";
		if (mallctlnametomib(mib, arenas_extend_mib, &arenas_extend_miblen)) {
			cf_crash(CF_JEM, "Failed to get JEMalloc MIB for \"%s\" (errno %d)!", mib, errno);
		}

		mib = "thread.arena";
		if (mallctlnametomib(mib, thread_arena_mib, &thread_arena_miblen)) {
			cf_crash(CF_JEM, "Failed to get JEMalloc MIB for \"%s\" (errno %d)!", mib, errno);
		}

		mib = "thread.tcache.enabled";
		if (mallctlnametomib(mib, thread_tcache_enabled_mib, &thread_tcache_enabled_miblen)) {
			cf_crash(CF_JEM, "Failed to get JEMalloc MIB for \"%s\" (errno %d)!", mib, errno);
		}

		// Open for business.
		jem_enabled = true;
	} else {
		// Don't use the JEMalloc APIs.
		jem_enabled = false;
	}

	return 0;
}

/*
 *  Create a new JEMalloc arena.
 *  Returns the arena index (>= 0) upon success or -1 upon failure.
 */
int jem_create_arena(void)
{
	int retval = -1;
	unsigned new_arena = 0;

	if (jem_enabled) {
		size_t len = sizeof(unsigned);
		if ((retval = mallctlbymib(arenas_extend_mib, arenas_extend_miblen, &new_arena, &len, NULL, 0))) {
			cf_warning(CF_JEM, "Failed to create a new JEMalloc arena (rv %d ; errno %d)!", retval, errno);
		} else {
			cf_debug(CF_JEM, "Created new JEMalloc arena #%d.", new_arena);
		}
	}

	return (!retval ? new_arena : retval);
}

/*
 *  Get the arena currently associated with the current thread.
 *  Returns the arena index (>= 0) upon success or -1 upon failure.
 */
int jem_get_arena(void)
{
	int retval = -1;
	unsigned orig_arena = 0;

	if (jem_enabled) {
		size_t len = sizeof(unsigned);
		int tid = syscall(SYS_gettid);

		if ((retval = mallctlbymib(thread_arena_mib, thread_arena_miblen, &orig_arena, &len, NULL, 0))) {
			cf_warning(CF_JEM, "In TID %d:  Failed to get arena!", tid);
		} else {
			cf_debug(CF_JEM, "In TID %d:  Got arena #%d", tid, orig_arena);
		}
	}

	return (!retval ? orig_arena : retval);
}

/*
 *  Set the JEMalloc arena for the current thread.
 *  Returns 0 if successful, -1 otherwise.
 */
int jem_set_arena(int arena)
{
	int retval = -1;

	if (jem_enabled && (0 <= arena)) {
		unsigned orig_arena = 0;
		size_t len = sizeof(unsigned);
		int tid = syscall(SYS_gettid);

		if ((retval = mallctlbymib(thread_arena_mib, thread_arena_miblen, &orig_arena, &len, &arena, len))) {
			cf_warning(CF_JEM, "Failed to set arena to #%d for TID %d! (rv %d ; errno %d)", arena, tid, retval, errno);
		} else {
			cf_debug(CF_JEM, "TID %d changed from arena #%d ==> #%d", tid, orig_arena, arena);
		}
	}

	return retval;
}

/*
 *  Set the state of the thread allocation cache.
 *  Returns 0 if successful, -1 otherwise.
 */
int jem_enable_tcache(bool enabled)
{
	int retval = -1;

	if (jem_enabled) {
		bool orig_enabled;
		size_t len = sizeof(bool);
		int tid = syscall(SYS_gettid);

		if ((retval = mallctlbymib(thread_tcache_enabled_mib, thread_tcache_enabled_miblen, &orig_enabled, &len, &enabled, len))) {
			cf_warning(CF_JEM, "Failed to set tcached enabled to %d for TID %d (errno %d)!", enabled, tid, errno);
		} else {
			cf_debug(CF_JEM, "TID %d changed from tcache enabled state from %d ==> %d", tid, orig_enabled, enabled);
		}
	}

	return retval;
}

/*
 *  Allocate the requested number of bytes in the given JEMalloc arena.
 *  If use_allocm is true, use the "allocm()" JEMalloc API instead of "malloc()".
 *  Returns pointer to the memory if successful, NULL otherwise.
 */
void *jem_allocate_in_arena(int arena, size_t size, bool use_allocm)
{
	void *ptr = NULL;

	if (jem_enabled) {
		int retval = -1;
		int tid = syscall(SYS_gettid);

		if (jem_enabled) {
			if (use_allocm) {
				// Use the JEMalloc function to allocate within the requested arena.
				size_t rsize = 0;
				if ((retval = allocm(&ptr, &rsize, size, ALLOCM_ARENA(arena)))) {
					cf_warning(CF_JEM, "TID %d failed (rv %d) to allocate %zu bytes on its arena (#%d)!", tid, retval, size, arena);
				} else {
					cf_debug(CF_JEM, "TID %d allocated %zu (%zu / %zu usable) bytes on its arena (#%d)!",
							 tid, size, rsize, malloc_usable_size(ptr), arena);
				}
			} else {
				// Go into the specified JEmalloc arena.
				jem_set_arena(arena);

				// Simply use "malloc()".
				if (!(ptr = malloc(size))) {
					cf_warning(CF_JEM, "TID %d failed to allocate %zu bytes on its arena (#%d)!", tid, size, arena);
				} else {
					cf_debug(CF_JEM, "TID %d allocated %zu (%zu usable) bytes on its arena (#%d)!",
							 tid, size, malloc_usable_size(ptr), arena);
				}
			}
		}
	} else {
		// Use the default "malloc(3)", which may be JEMalloc.
		ptr = malloc(size);
	}

	return ptr;
}

/*
 *  Log information about the state of JEMalloc.
 *
 *  XXX -- Should be able to set the output stream as desired.
 */
void jem_log_stats(void)
{
	if (jem_enabled) {
		malloc_stats_print(NULL, NULL, NULL);
	}
}
