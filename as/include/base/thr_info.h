/*
 * thr_info.h
 *
 * Copyright (C) 2008-2014 Aerospike, Inc.
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
 * info function declarations
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "dynbuf.h"
#include "util.h"

#include "base/transaction.h"
#include "fabric/paxos.h"


typedef int (*as_info_get_tree_fn) (char *name, char *subtree, cf_dyn_buf *db);
typedef int (*as_info_get_value_fn) (char *name, cf_dyn_buf *db);
typedef int (*as_info_command_fn) (char *name, char *parameters, cf_dyn_buf *db);


extern void as_info_paxos_event(as_paxos_generation gen,
		as_paxos_change *change, cf_node succession[], void *udata);

extern void as_query_set_job_tracking(bool);

// Starting to calculate more and more stats in thr_info. Perhaps this should be
// elsewhere?
extern uint64_t thr_info_get_object_count();

// Sets a static value - set to 0 to remove a previous value.
extern int as_info_set_buf(const char *name, const uint8_t *value, size_t value_sz, bool def);
extern int as_info_set(const char *name, const char *value, bool def);


// For dynamic items - you will get called when the name is requested. The
// dynbuf will be fully set up for you - just add the information you want to
// return.
extern int as_info_set_dynamic(char *name, as_info_get_value_fn gv_fn, bool def);

// For tree items - you will get called when the name is requested, and it will
// have the name you registered (name) and the subtree portion (value). The
// dynbuf will be fully set up for you - just add the information you want to
// return
extern int as_info_set_tree(char *name, as_info_get_tree_fn gv_fn);

// For commands - you will be called with the parameters.
extern int as_info_set_command(char *name, as_info_command_fn command_fn );

// Processes an info request that comes in from the network, sends the response.
extern int as_info(as_transaction *tr);

// Processes a pure puffer request without any info header stuff.
extern int as_info_buffer(uint8_t *req_buf, size_t req_buf_len, cf_dyn_buf *rsp);

extern void info_debug_ticker_start();

// The info unit uses the fabric to communicate with the other members of the
// cluster so it needs to register for different messages and create listener
// threads, etc.
extern int as_info_init();

// The info port is used by more basic monitoring services.
extern int as_info_port_start();

// Acceptable timediffs in XDR lastship times.
// (Print warning only if time went back by at least 5 minutes.)
#define XDR_ACCEPTABLE_TIMEDIFF XDR_TIME_ADJUST
