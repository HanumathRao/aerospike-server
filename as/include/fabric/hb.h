/*
 * hb.h
 *
 * Copyright (C) 2008 Aerospike, Inc.
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
 * The heartbeat module is a set of network routines
 * to send and receive lightweight messages in the cluster
 *
 * State diagram:
 *
 * heartbeat_init starts listening for other nodes, and adds those nodes to the
 * list of active nodes without reporting them to interested parties.
 *
 * heartbeat_start finalizes the startup of the heartbeat system. It enforces
 * a time delay for startup, and after that, all new nodes will be reported.
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "clock.h"
#include "socket.h"
#include "util.h"


typedef enum { AS_HB_NODE_ARRIVE, AS_HB_NODE_DEPART, AS_HB_NODE_UNDUN, AS_HB_NODE_DUN } as_hb_event_type;

typedef struct as_hb_event_node_t {
	as_hb_event_type evt;
	cf_node nodeid;
	cf_node p_node; // the principal node from the succession list
} as_hb_event_node;

typedef void (*as_hb_event_fn) (int nevents, as_hb_event_node *events, void *udata);

extern void as_hb_init();
extern void as_hb_start();
extern bool as_hb_shutdown();
extern int as_hb_getaddr(cf_node node, cf_sockaddr *so);
extern int as_hb_register(as_hb_event_fn cb, void *udata);

extern void as_hb_process_fabric_heartbeat(cf_node node, int fd, cf_sockaddr socket, uint32_t addr, uint32_t port, cf_node *buf, size_t bufsz);
extern bool as_hb_get_is_node_dunned(cf_node node);
extern void as_hb_set_is_node_dunned(cf_node node, bool state, char *context);
extern int as_hb_set_are_nodes_dunned(char *node_str, int node_str_len, bool is_dunned);

// list a node as non-responsive for a certain amount of time
// 0 means un-snub
// use a very large value for 'forever'
extern int as_hb_snub(cf_node node, cf_clock ms);

// TIP the heartbeat system that there might be a cluster at a given IP address.
extern int as_hb_tip(char *host, int port);
extern int as_hb_tip_clear();

// Set the heartbeat protocol version.
extern int as_hb_set_protocol(hb_protocol_enum protocol);
