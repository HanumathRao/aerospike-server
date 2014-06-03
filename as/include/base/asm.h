/*
 * asm.h
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
 *  SYNOPSIS
 *    The ASMalloc interface provides the connection between the
 *    Aerospike Server and the ASMalloc memory accounting tool by
 *    defining threshold parameters for notification of memory
 *    allocation changes and functions to invoke the callback
 *    hook from, and send commands to, the ASMalloc library.
 */

#pragma once

#ifdef USE_ASM

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dlfcn.h>
#include <stdbool.h>
#include <stddef.h>
#include <time.h>

#include "asmalloc.h"


/*
 *  Maximum number of mallocations allocated on the stack in the main thread.
 *  [Note:  This number must be at least as great as the value of "NUM_MALLOCATIONS"
 *           in the generated file "gen/mallocations.h".]
 */
#define MAX_NUM_MALLOCATIONS               (1024)

/*
 *  Default minimum size of blocks triggering mallocation alerts.
 */
#define DEFAULT_THRESH_BLOCK_SIZE_BYTES    (512 * 1024)

/*
 *  Default minimum delta size between mallocation alerts per thread.
 */
#define DEFAULT_THRESH_DELTA_SIZE_BYTES    (1024 * 1024)

/*
 *  Default minimum time between mallocation alerts per thread.
 */
#define DEFAULT_THRESH_DELTA_TIME_SECONDS  (60)

/*
 *  Is periodic invocation of the ASMalloc hook and logging of messages enabled?
 */
extern bool g_asm_hook_enabled;

/*
 *  Is the ASMalloc callback function enabled?
 */
extern bool g_asm_cb_enabled;

/*
 *  Threshold values for triggering memory allocation callbacks.
 */
extern size_t g_thresh_block_size;
extern size_t g_thresh_delta_size;
extern time_t g_thresh_delta_time;

/*
 *  User-supplied private data to be passed to the callback function.
 */
extern void *g_my_cb_udata;

/*
 *  Initialize ASMalloc functions if the library has been preloaded.
 */
void asm_init(void);

/*
 *  Invoke the ASMalloc hook function.
 */
int as_asm_hook(void *arg, asm_stats_t **asm_stats, vm_stats_t **vm_stats);

/*
 *  Invoke the ASMalloc command function.
 */
int as_asm_cmd(asm_cmd_t cmd, ...);

#endif // defined(USE_ASM)
