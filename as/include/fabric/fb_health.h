/*
 * fb_health.h
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
 */


//==========================================================
// Includes
//

#include <stdbool.h>
#include "util.h"


//==========================================================
// Typedefs
//

typedef enum {
	FB_HEALTH_OK,
	FB_HEALTH_BAD_NODE,
	FB_HEALTH_BAD_CLUSTER
} fb_health_status;

typedef int (*fb_health_cb_fn)
		(cf_node node, fb_health_status status, void* pv_udata);


//==========================================================
// Public API
//

int as_fb_health_ack_other_nodes(bool enable);
int as_fb_health_create();
void as_fb_health_destroy();
int as_fb_health_register_cb_fn(fb_health_cb_fn cb, void* pv_udata);
void as_fb_health_unregister_cb_fn(fb_health_cb_fn cb, void* pv_udata);

