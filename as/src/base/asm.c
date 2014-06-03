/*
 * asm.c
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
 *    The ASMalloc interface provides the connection between the
 *    Aerospike Server and the ASMalloc memory accounting tool by
 *    defining threshold parameters for notification of memory
 *    allocation changes and functions to invoke the callback
 *    hook from, and send commands to, the ASMalloc library.
 */

#ifdef USE_ASM

#include "base/asm.h"

#include <dlfcn.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <time.h>

#include "asmalloc.h"

#include "citrusleaf/alloc.h"


/*
 *  Is periodic invocation of the ASMalloc hook and logging of messages enabled?
 */
bool g_asm_hook_enabled = true;

/*
 *  Is the ASMalloc callback function enabled?
 */
bool g_asm_cb_enabled = true;

/*
 *  Threshold values for triggering memory allocation callbacks.
 */
size_t g_thresh_block_size = DEFAULT_THRESH_BLOCK_SIZE_BYTES;
size_t g_thresh_delta_size = DEFAULT_THRESH_DELTA_SIZE_BYTES;
time_t g_thresh_delta_time = DEFAULT_THRESH_DELTA_TIME_SECONDS;

/*
 *  User-supplied private data to be passed to the callback function.
 */
void *g_my_cb_udata = NULL;

/*
 *  Default hook function to be used when not using ASMalloc.
 */
static int original_hook(void *arg, asm_stats_t **asm_stats, vm_stats_t **vm_stats)
{
	return 0;
}

/*
 *  Global variable holding the hook function to invoke.
 */
static int (*g_hook)(void *arg, asm_stats_t **asm_stats, vm_stats_t **vm_stats) = original_hook;

/*
 *  Invoke the ASMalloc hook function.
 */
int as_asm_hook(void *arg, asm_stats_t **asm_stats, vm_stats_t **vm_stats)
{
	return (g_hook)(arg, asm_stats, vm_stats);
}

/*
 *  Default command function to be used when not using ASMalloc.
 */
static int original_cmd(asm_cmd_t cmd, ...)
{
#ifdef DEBUG_ASM
	fprintf(stderr, "In original_cmd(%d)!\n", cmd);
	fflush(stderr);
#endif

	return 0;
}

/*
 *  Global variable holding the command function to invoke.
 */
static int (*g_cmd)(asm_cmd_t cmd, ...) = original_cmd;

/*
 *  Invoke the ASMalloc command function.
 */
int as_asm_cmd(asm_cmd_t cmd, ...)
{
	va_list args;
	va_start(args, cmd);
	int retval = (g_cmd)(cmd, args);
	va_end(args);

	return retval;
}

/*
 *  Initialize ASMalloc functions if the library has been preloaded.
 */
void asm_init(void)
{
#ifdef DEBUG_ASM
	fprintf(stderr, "In asm_init()!\n");
	fflush(stderr);
#endif

	if (!(g_hook = dlsym(RTLD_NEXT, "asm_hook"))) {
		fprintf(stderr, "Could not find \"asm_hook\" ~~ Using \"original_hook\"!\n");
		g_hook = original_hook;
	}

	if (!(g_cmd = dlsym(RTLD_NEXT, "asm_cmd"))) {
		fprintf(stderr, "Could not find \"asm_cmd\" ~~ Using \"orginal_cmd\"!\n");
		g_cmd = original_cmd;
	}

	if (!(g_mallocation_set = dlsym(RTLD_NEXT, "asm_mallocation_set"))) {
		fprintf(stderr, "Could not find \"asm_mallocation_set\"!\n");
	}

	if (!(g_mallocation_get = dlsym(RTLD_NEXT, "asm_mallocation_get"))) {
		fprintf(stderr, "Could not find \"asm_mallocation_get\"!\n");
	}

	// Set up initial library parameters.
	as_asm_cmd(ASM_CMD_SET_FEATURES, ASM_LOG_DATESTAMP | ASM_LOG_THREAD_STATS | ASM_LOG_MEM_COUNT_STATS | ASM_LOG_VM_STATS);
	as_asm_cmd(ASM_CMD_SET_THRESHOLDS, g_thresh_block_size, g_thresh_delta_size, g_thresh_delta_time);
	as_asm_cmd(ASM_CMD_SET_CALLBACK, my_cb, g_my_cb_udata);
}

#endif // defined(USE_ASM)
