/*
 * fb_health.c
 *
 * Copyright (C) 2011 Aerospike, Inc.
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
 * Probe cluster fabric health by sending "bursts" of messages to other nodes
 * and monitoring the acks.
 *
 * Citrusleaf, 2011
 * All rights reserved
 */



//==========================================================
// Includes
//

#include "fabric/fb_health.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#include "clock.h"
#include "fault.h"
#include "msg.h"
#include "util.h"

#include "base/cfg.h"
#include "fabric/fabric.h"


//==========================================================
// Private "Class Members"
//

//------------------------------------------------
// Function Declarations
//

void* run_fb_health(void* pv_param);
static void refresh_node_list();
static void analyze_burst(uint32_t msg_per_burst);
static void make_callbacks(cf_node node, fb_health_status status);
static int recv_ping(cf_node node, msg* p_ping, void* pv_udata);
static int recv_ack(cf_node node, msg* p_ack, void* pv_udata);
static bool recv_msg(msg* p_msg, uint32_t* p_tgt_node_index,
		uint64_t* p_timestamp);

//------------------------------------------------
// Data
//

#define MAX_CB_FNS 4

typedef struct fb_health_s {
	pthread_t			thread;
	uint32_t			running;
	fb_health_cb_fn		cb_fns[MAX_CB_FNS];
	void*				cb_udatas[MAX_CB_FNS];
	as_node_list		node_list;
	cf_atomic32			ack_counts[MAX_NODES_LIST];
	fb_health_status	statuses[MAX_NODES_LIST];
} fb_health;

//------------------------------------------------
// Constants
//

typedef enum {
	FB_HEALTH_TGT_NODE_INDEX	= 0,
	FB_HEALTH_TIMESTAMP			= 1
} msg_template_field_id;

const msg_template fb_health_mt[] = {
	{ FB_HEALTH_TGT_NODE_INDEX, M_FT_UINT32 },
	{ FB_HEALTH_TIMESTAMP, M_FT_UINT64 }
};

const __useconds_t BURST_DURATION = 1750000;
const __useconds_t IDLE_DURATION  =  250000;


//==========================================================
// Globals
//

fb_health g_fb_health;
fb_health* this = NULL;


//==========================================================
// Public API
//

//------------------------------------------------
// Enable/disable handling pings from other nodes.
//
int
as_fb_health_ack_other_nodes(bool enable) {
	if (enable) {
		if (as_fabric_register_msg_fn(M_TYPE_FB_HEALTH_PING, fb_health_mt,
				sizeof(fb_health_mt), recv_ping, NULL)) {
			cf_warning(AS_FB_HEALTH, "can't register recv_ping cb");

			return -1;
		}
	}
	else {
		if (as_fabric_register_msg_fn(M_TYPE_FB_HEALTH_PING, NULL, 0, NULL,
				NULL)) {
			cf_warning(AS_FB_HEALTH, "can't unregister recv_ping cb");

			return -1;
		}
	}

	return 0;
}

//------------------------------------------------
// Start pinging other nodes.
//
int
as_fb_health_create() {
	this = &g_fb_health;
	memset(this, 0, sizeof(fb_health));

	if (as_fabric_register_msg_fn(M_TYPE_FB_HEALTH_ACK, fb_health_mt,
			sizeof(fb_health_mt), recv_ack, NULL)) {
		cf_warning(AS_FB_HEALTH, "can't register recv_ack cb");

		this = NULL;

		return -1;
	}

	if (pthread_create(&this->thread, NULL, run_fb_health, NULL)) {
		cf_warning(AS_FB_HEALTH, "can't start thread");

		as_fabric_register_msg_fn(M_TYPE_FB_HEALTH_ACK, NULL, 0, NULL, NULL);
		this = NULL;

		return -1;
	}

	return 0;
}

//------------------------------------------------
// Stop pinging other nodes.
//
void
as_fb_health_destroy() {
	this->running = 0;

	void* pv_value;

	// TODO - this join may take up to 1 second - improve if needed!
	pthread_join(this->thread, &pv_value);

	as_fabric_register_msg_fn(M_TYPE_FB_HEALTH_ACK, NULL, 0, NULL, NULL);
	this = NULL;
}

//------------------------------------------------
// Register for fabric health notifications.
//
int
as_fb_health_register_cb_fn(fb_health_cb_fn cb, void* pv_udata) {
	for (int i = 0; i < MAX_CB_FNS; i++) {
		if (! this->cb_fns[i]) {
			this->cb_fns[i] = cb;
			this->cb_udatas[i] = pv_udata;

			return 0;
		}
	}

	cf_warning(AS_FB_HEALTH, "can't register health cb");

	return -1;
}

//------------------------------------------------
// Unregister for fabric health notifications.
//
void
as_fb_health_unregister_cb_fn(fb_health_cb_fn cb, void* pv_udata) {
	for (int i = 0; i < MAX_CB_FNS; i++) {
		if (cb == this->cb_fns[i] && pv_udata == this->cb_udatas[i]) {
			this->cb_fns[i] = NULL;
			this->cb_udatas[i] = NULL;

			break;
		}
	}
}


//==========================================================
// Private Functions
//

//------------------------------------------------
// Thread "run" function - ping other nodes,
// analyze acks, and notify interested parties of
// changes in cluster fabric health.
//
void*
run_fb_health(void* pv_param) {
	this->running = 1;

	while (this->running) {
		uint32_t msg_per_burst = g_config.fb_health_msg_per_burst;

		// g_config.fb_health_msg_per_burst is our way of turning this on/off.
		if (! msg_per_burst) {
			usleep(BURST_DURATION + IDLE_DURATION);

			continue;
		}

		refresh_node_list();

		// This is thread-safe as long as g_config.fb_health_msg_timeout is
		// safely less than IDLE_DURATION.
		memset((void*)this->ack_counts, 0,
				sizeof(cf_atomic32) * this->node_list.sz);

		__useconds_t sleep_during_burst = BURST_DURATION / msg_per_burst;

		// Send pings during first part of "burst cycle".
		for (int i = 0; i < msg_per_burst; i++) {
			usleep(sleep_during_burst);

			// Ping all other nodes.
			for (uint n = 1; n < this->node_list.sz; n++) {
				msg* p_ping = as_fabric_msg_get(M_TYPE_FB_HEALTH_PING);

				if (! p_ping) {
					cf_warning(AS_FB_HEALTH, "can't obtain ping msg");

					continue;
				}

				msg_set_uint32(p_ping, FB_HEALTH_TGT_NODE_INDEX, n);
				msg_set_uint64(p_ping, FB_HEALTH_TIMESTAMP, cf_getms());

				as_fabric_send(this->node_list.nodes[n], p_ping,
						AS_FABRIC_PRIORITY_HIGH);
			}
		}

		// Wait for remainder of "burst cycle", then tally acks.
		usleep(IDLE_DURATION);
		analyze_burst(msg_per_burst);
	}

	return NULL;
}

//------------------------------------------------
// Before sending each "burst" of pings, refresh
// the list of cluster nodes.
//
static void
refresh_node_list() {
	// Note: first node in list (index 0) is always self - to exclude self we
	// start all our node list loops at index 1.

	as_node_list node_list;

	if (as_fabric_get_node_list(&node_list) < 1) {
		cf_warning(AS_FB_HEALTH, "failed to get node list");

		// Make sure this "burst cycle" is a no-op.
		this->node_list.sz = 0;

		return;
	}

	// Return quickly if the node list has not changed ...
	if (node_list.sz == this->node_list.sz && (node_list.sz == 1 ||
			! memcmp(node_list.nodes, this->node_list.nodes,
					node_list.sz * sizeof(cf_node)))) {
		return;
	}

	// ... otherwise refresh the node list, maintaining node statuses.

	fb_health_status statuses[node_list.sz];

	// Initialize all to FB_HEALTH_OK.
	memset(statuses, 0, sizeof(statuses));

	for (int n = 1; n < node_list.sz; n++) {
		cf_node node = node_list.nodes[n];

		for (int m = 1; m < this->node_list.sz; m++) {
			if (node == this->node_list.nodes[m]) {
				statuses[n] = this->statuses[m];

				break;
			}
		}
	}

	memcpy(this->statuses, statuses, sizeof(statuses));
	this->node_list = node_list;

	return;
}

//------------------------------------------------
// After sending each "burst" of pings, calculate
// the percentage of successful acks from each of
// the other nodes, and notify interested parties
// of status changes.
//
static void
analyze_burst(uint32_t msg_per_burst) {
	uint32_t ack_pcts[this->node_list.sz];
	bool is_bad_cluster = this->node_list.sz > 2; // 2 node cluster never bad

	for (uint n = 1; n < this->node_list.sz; n++) {
		ack_pcts[n] =
				(100 * cf_atomic32_get(this->ack_counts[n])) / msg_per_burst;

		// If any other node is ok, the cluster is not deemed bad.
		if (ack_pcts[n] >= g_config.fb_health_good_pct) {
			is_bad_cluster = false;
		}
	}

	if (is_bad_cluster) {
		cf_info(AS_FB_HEALTH, "fabric health detects no good node connections");
	}

	for (uint n = 1; n < this->node_list.sz; n++) {
		if (is_bad_cluster) {
			this->statuses[n] = FB_HEALTH_BAD_CLUSTER;
		}
		else if (ack_pcts[n] <= g_config.fb_health_bad_pct &&
				this->statuses[n] != FB_HEALTH_BAD_NODE) {
			this->statuses[n] = FB_HEALTH_BAD_NODE;
		}
		else if (ack_pcts[n] >= g_config.fb_health_good_pct &&
				this->statuses[n] != FB_HEALTH_OK) {
			this->statuses[n] = FB_HEALTH_OK;
		}
		else if (this->statuses[n] == FB_HEALTH_BAD_CLUSTER) {
			this->statuses[n] = ack_pcts[n] <= g_config.fb_health_bad_pct ?
					FB_HEALTH_BAD_NODE : FB_HEALTH_OK;
		}

		if (this->statuses[n] == FB_HEALTH_BAD_NODE) {
			cf_info(AS_FB_HEALTH,
					"fabric health detects bad connection to node %" PRIx64
					" (ack pct %3" PRIu32 ")", this->node_list.nodes[n],
					ack_pcts[n]);
		}

		make_callbacks(this->node_list.nodes[n], this->statuses[n]);
	}
}

//------------------------------------------------
// Make callbacks to interested parties.
//
static void
make_callbacks(cf_node node, fb_health_status status) {
	for (int i = 0; i < MAX_CB_FNS; i++) {
		if (this->cb_fns[i]) {
			this->cb_fns[i](node, status, this->cb_udatas[i]);
		}
	}
}

//------------------------------------------------
// Fabric message callback to handle pings from
// other nodes - copy each ping's data and send it
// back in an ack.
//
static int
recv_ping(cf_node node, msg* p_ping, void* pv_udata) {
	uint32_t tgt_node_index;
	uint64_t timestamp;

	if (! recv_msg(p_ping, &tgt_node_index, &timestamp)) {
		cf_warning(AS_FB_HEALTH, "can't get fields from ping");

		return -1;
	}

	msg* p_ack = as_fabric_msg_get(M_TYPE_FB_HEALTH_ACK);

	if (! p_ack) {
		cf_warning(AS_FB_HEALTH, "can't obtain ack msg");

		return -1;
	}

	msg_set_uint32(p_ack, FB_HEALTH_TGT_NODE_INDEX, tgt_node_index);
	msg_set_uint64(p_ack, FB_HEALTH_TIMESTAMP, timestamp);

	return as_fabric_send(node, p_ack, AS_FABRIC_PRIORITY_HIGH);
}

//------------------------------------------------
// Fabric message callback to handle acks from
// other nodes - if an ack has returned in time,
// increment the appropriate successful ack count.
//
static int
recv_ack(cf_node node, msg* p_ack, void* pv_udata) {
	uint32_t tgt_node_index;
	uint64_t timestamp;

	if (! recv_msg(p_ack, &tgt_node_index, &timestamp)) {
		cf_warning(AS_FB_HEALTH, "can't get fields from ack");

		return -1;
	}

	if (cf_getms() - timestamp < g_config.fb_health_msg_timeout) {
		cf_atomic32_incr(&this->ack_counts[tgt_node_index]);
	}

	return 0;
}

//------------------------------------------------
// Get fabric message fields and recycle message.
//
static bool
recv_msg(msg* p_msg, uint32_t* p_tgt_node_index, uint64_t* p_timestamp) {
	int err = 0;

	err += msg_get_uint32(p_msg, FB_HEALTH_TGT_NODE_INDEX, p_tgt_node_index);
	err += msg_get_uint64(p_msg, FB_HEALTH_TIMESTAMP, p_timestamp);

	as_fabric_msg_put(p_msg);

	return ! err;
}

