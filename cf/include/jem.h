/*
 * jem.h
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

#pragma once

#include <stdbool.h>
#include <stddef.h>

/* SYNOPSIS
 *  This is the declarations file for a simple interface to JEMalloc.
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
 *  Initialize the interface to JEMalloc.
 *  If enable is true, the JEMalloc features will be enabled, otherwise they will be disabled.
 *  Returns 0 if successful, -1 otherwise.
 */
int jem_init(bool enable);

/*
 *  Create a new JEMalloc arena.
 *  Returns the arena index (>= 0) upon success or -1 upon failure.
 */
int jem_create_arena(void);

/*
 *  Get the arena currently associated with the current thread.
 *  Returns the arena index (>= 0) upon success or -1 upon failure.
 */
int jem_get_arena(void);

/*
 *  Set the JEMalloc arena for the current thread.
 *  Returns 0 if successful, -1 otherwise.
 */
int jem_set_arena(int arena);

/*
 *  Set the state of the thread allocation cache.
 *  Returns 0 if successful, -1 otherwise.
 */
int jem_enable_tcache(bool enabled);

/*
 *  Allocate the requested number of bytes in the given JEMalloc arena.
 *  If use_allocm is true, use the "allocm()" JEMalloc API instead of "malloc()".
 *  Returns pointer to the memory if successful, NULL otherwise.
 */
void *jem_allocate_in_arena(int arena, size_t size, bool use_allocm);

/*
 *  Log information about the state of JEMalloc.
 *
 *  XXX -- Should be able to set the output stream as desired.
 */
void jem_log_stats(void);
