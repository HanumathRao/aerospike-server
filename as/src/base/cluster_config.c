/*
 * ai_btree.h
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
 * Cluster Topology Information Management
 *
 */

#include "base/cfg.h"
#include "base/cluster_config.h"
#include "fabric/paxos.h"

// Help with tracing -- show module and last update date.
static char * MOD = "cluster_config.c::2013:06:12";
static bool DEBUG = false;

// String defined for the various cluster states
const char * cc_state_str[] = {"unknown", "balanced", "unbalanced", "invalid"};


/**
 * cluster_config defaults
 * Set up the defaults for the Cluster Topology.
 */
void
cc_cluster_config_defaults(cluster_config_t * cc ) {
	memset(cc, 0, sizeof(cluster_config_t));
	cc->cluster_state = unknown;  // not set yet.

} // end cluster_config_defaults()


/**
 * Locate Node Entry
 * Find a Node ID, return the index of that group id
 * RETURN:
 * (*) Success: Return index of the group id
 * (*) Failure: Return -1
 */
int
cc_locate_node(cluster_config_t * cc, cc_node_t node_id ) {
	int node_index = -1;
	// Look for the node -- if found, then save the index.
	// Simple search (for now).  Linear Scan of the group list.
	int i = 0;
	for( i = 0; i < cc->node_count; i++ ) {
		if ( node_id == cc->node_ids[i] ) {
			cf_detail(AS_PARTITION, "Found Node ID(%d)", node_id );
			break;
		}
	}  // end for each node name

	if( i < cc->node_count ) {
		node_index = i;
	}

	return node_index;
} // end locate_node()

/**
 * Add a Node Entry
 * Search the Node List and add a new Node ID if it is not already there.
 * Either way -- return the NODE INDEX of the node entry.
 * NOTE: This method is deprecated -- used only in the old config, but no
 * longer in the active cluster status check. Now we use FULL NODE values.
 * RETURN:
 * (*) Success: Return index of the node id
 * (*) Failure: Return -1
 */
int
cc_add_node(cluster_config_t * cc, cc_node_t node_id ) {
	// Look for the node -- if found, then save the index.
	// Simple search (for now).  Linear Scan of the node list.
	int node_index = -1;
	bool found = false;
	int i = 0;
	for( i = 0; i < cc->node_count; i++ ) {
		if ( node_id == cc->node_ids[i] ) {
			cf_detail(AS_PARTITION, "Found Node ID(%d)", node_id );
			found = true;
			break;
		}
	}  // end for each node name
	node_index = i; // Found or not, this is (or will be) the index

	// Didn't find it -- so just add it to the end and adjust the count.
	if( found == false ) {
		if( i >= CL_MAX_NODES ) {
			cf_crash_nostack(AS_PARTITION,
					"Exceeded Max Number of Nodes (%d) in Config File", CL_MAX_NODES);
		} else {
			cc->node_ids[ (cc->node_count)++ ] = node_id;
		}
	}

	return node_index;
} // end add_node()

/**
 * Add a FULL Node Entry (the full 64 bit value).
 * So -- just like the "add node id()" function, this function adds a node to
 * the node_id list -- but also adds it to the FULL NODE list.  It's the FULL
 * node value that we want to print out in the final cluster summary.
 * Search the Node List and add a new Node ID if it is not already there.
 * Either way -- return the NODE INDEX of the node entry.
 * RETURN:
 * (*) Success: Return index of the node id
 * (*) Failure: Return -1
 */
int
cc_add_fullnode(cluster_config_t * cc, cf_node fullnode ) {
	// Look for the node -- if found, then save the index.
	// Simple search (for now).  Linear Scan of the node list.
	int node_index = -1;
	bool found = false;
	int i = 0;
	cc_node_t node_id = cc_compute_node_id( fullnode );
	for( i = 0; i < cc->node_count; i++ ) {
		if ( node_id == cc->node_ids[i] ) {
			cf_detail(AS_PARTITION, "Found Node ID(%d)", node_id );
			found = true;
			break;
		}
	}  // end for each node name
	node_index = i; // Found or not, this is (or will be) the index

	// Didn't find it -- so just add it to the end and adjust the count.
	if( found == false ) {
		if( i >= CL_MAX_NODES ) {
			cf_crash_nostack(AS_PARTITION,
					"Exceeded Max Number of Nodes (%d) in Config File", CL_MAX_NODES);
		} else {
			cc->full_node_val[ cc->node_count ] = fullnode;
			cc->node_ids[ (cc->node_count)++ ] = node_id;
		}
	}

	return node_index;
} // end cc_add_fullnode()

/**
 * Locate Group Entry
 * Find a Group ID, return the index of that group id
 * RETURN:
 * (*) Success: Return index of the group id
 * (*) Failure: Return -1
 */
int
cc_locate_group(cluster_config_t * cc, cc_group_t group_id ) {
	int group_index = -1;
	bool found = false;
	// Look for the group -- if found, then save the index.
	// Simple search (for now).  Linear Scan of the group list.
	int i = 0;
	for( i = 0; i < cc->group_count; i++ ) {
		if ( group_id == cc->group_ids[i] ) {
			cf_detail(AS_PARTITION, "Found Group ID(%d)", group_id );
			found = true;
			break;
		}
	}  // end for each group name

	if( found == false ) {
		group_index = i;
	}

	return group_index;
} // end locate_group()

/**
 * Add a Group Entry
 * Search the Group List and add a new Group ID if it is not already there.
 * Either way -- return the Group INDEX of the group entry.
 * RETURN:
 * (*) Success: Return index of the new group id
 * (*) Failure: Return -1
 */
int
cc_add_group(cluster_config_t * cc, cc_group_t group_id ) {
	// Look for the group -- if found, then save the index.
	// Simple search (for now).  Linear Scan of the group list.
	int group_index = -1;
	bool found = false;
	int i = 0;
	for( i = 0; i < cc->group_count; i++ ) {
		if ( group_id == cc->group_ids[i] ) {
			cf_detail(AS_PARTITION, "Found Group ID(%d)", group_id );
			found = true;
			break;
		}
	}  // end for each group name
	group_index = i; // Found or not, this is (or will be) the index

	// Didn't find it -- so just add it to the end and adjust the count.
	if( found == false ) {
		if( i >=  CL_MAX_GROUPS ) {
			cf_crash_nostack(AS_PARTITION,
					"Exceeded Max Number of Groups (%d) in Config File", CL_MAX_GROUPS);
		} else {
			cc->group_ids[ (cc->group_count)++ ] = group_id;
		}
	}

	if( DEBUG )
		printf("<><><> ADD GROUP:: Found(%d) GroupID(%04x) NDX(%d)\n", found, group_id, group_index );

	return group_index;
} // end add_group()


/**
 * Add Node/Group Entry
 * Add a Node and Group to the Topology Info.
 */
int
cc_add_node_group_entry(cluster_config_t * cc, cc_node_t node, cc_group_t group ) {
	static char * meth = "add_node_group_entry()";
	int rc = 0;
//	printf("[ENTER]<%s:%s> node(%u) group(%u)\n", MOD, meth, node, group );

	// Look for the group -- if found, then save the index.
	// And, if not found, add it, and save the index.
	int group_ndx = cc_add_group( cc, group );

	// Group is all set.  Now add the node (we shouldn't have one already).
	int node_ndx = cc_add_node( cc, node );

	// Quick validation step -- if the membership array shows a NON-negative
	// entry, point that out, but ALSO
	if( cc->membership[node_ndx]  > 0 && cc->membership[node_ndx] != group_ndx) {
		cf_debug(AS_PARTITION, "[ERROR]<%s:%s>Adding NODE(%d) GROUP(%d) SET(%d)",
				MOD, meth, node, group, cc->membership[node_ndx] );
//		printf( "[ERROR]<%s:%s>Adding NODE[%d](%u) GROUP[%d](%u) MEM(%d) \n",
//				MOD, meth, node_ndx, node, group_ndx, group, cc->membership[node_ndx] );
		rc = -1;
	}

	// Just overwrite the weird case for now -- and we'll figure it out later.
	// TODO: Handle the overwrite error if it ever comes up.  It would probably
	// be ONLY a user error -- but it most likely shows that the user screwed
	// up the config file.
	cc->membership[node_ndx] = group_ndx;
	cc->group_node_count[group_ndx]++; // One more in this group

	return rc;
} // end add_node_group_entry()

/**
 * Add a FULL NODE Entry, which has port, group and node id inside
 * Add a Node and Group to the Topology Info.
 */
int
cc_add_fullnode_group_entry(cluster_config_t * cc, cf_node fullnode ) {
	static char * meth = "add_fullnode_group_entry()";
	int rc = 0;
//	printf("[ENTER]<%s:%s> fullnode(%"PRIx64")\n", MOD, meth, fullnode );

	// Look for the group -- if found, then save the index.
	// And, if not found, add it, and save the index.
	cc_group_t group_id = cc_compute_group_id( fullnode );
	int group_ndx = cc_add_group( cc, group_id );

	// Group is all set.  Now add the node (we shouldn't have one already).
	int node_ndx = cc_add_fullnode( cc, fullnode );

	// Quick validation step -- if the membership array shows a NON-negative
	// entry, point that out, but ALSO
	if( cc->membership[node_ndx]  > 0 && cc->membership[node_ndx] != group_ndx) {
		cf_debug(AS_PARTITION, "[ERROR]<%s:%s>Adding FULLNODE[%d](%"PRIx64") MEMBER(%d) \n",
				MOD, meth, node_ndx, fullnode, cc->membership[node_ndx] );
		rc = -1;
	}

	// Just overwrite the weird case for now -- and we'll figure it out later.
	// TODO: Handle the overwrite error if it ever comes up.  It would probably
	// be ONLY a user error -- but it most likely shows that the user screwed
	// up the config file.
	cc->membership[node_ndx] = group_ndx;
	cc->group_node_count[group_ndx]++; // One more in this group

	return rc;
} // end add_node_group_entry()

/**
 * Locate Node Group
 * For a given Node ID, return the Group ID that goes with it.
 * RETURN:
 * (*) Success: Return the group id
 * (*) Failure: Return -1
 */
int
cc_locate_node_group(cluster_config_t * cc, cc_node_t node_id ) {
//	static char * meth = "locate_node_group()";
//	printf("[ENTER]<%s:%s> Locate Node ID (%u)\n", MOD, meth, node_id );

	int node_index = cc_locate_node( cc, node_id );
	int group_index = cc->membership[ node_index];
	cc_group_t group_id = cc->group_ids[group_index];

	return group_id;
} // end locate_node_group()

/**
 * Extract the service port portion (the upper 16 bits) of the
 * 64 bit self-node value
 */
uint16_t
cc_compute_port( cf_node self_node ) {
	uint16_t result = (self_node >> 48) & 0xffff;
	return result;
} // end compute_port()

/**
 * Extract the Group ID portion (the Bits 33 to 48) of the 64 bit self-node value
 */
cc_group_t
cc_compute_group_id( cf_node self_node ) {
	cc_group_t result = (self_node >> 32) & 0xffff;
	return result;
} // end compute_group_id()

/**
 * Extract the Node ID portion (lower 32 bits) of the self-node value
 */
cc_node_t
cc_compute_node_id( cf_node self_node ) {
	cc_node_t result = self_node & 0xffffffff;
	return result;
} // end compute_gnode_id()

/**
 * Compute the combined value for the 64 bit self-node value:
 * Group ID (upper bits 33-48) and Node ID (lower 32 bits).
 * Generally this will be masked in with the original HW node (16 bits port plus
 * 48 bits MAC address).
 */
cf_node
cc_compute_self_node( uint16_t port_num, cc_group_t group_id, cc_node_t node_id ) {
	cf_node result = 0;
	// Use 64 bit temp vars to avoid any sign extensions or weird overflow.
	cf_node temp_port = port_num;
	cf_node temp_group = group_id;
	cf_node temp_node = node_id;

	result = temp_port << 48;
	result |= temp_group << 32;
	result |= temp_node;

	return result;
}

/**
 * Show the group and node status of the cluster -- for this instance
 * of the cluster_config_t value.  This may or may not be the CC that
 * is in the g_config structure.
 */
void
cc_show_cluster_state( cluster_config_t * cc )
{
	int i, j, buf_pos;
	char print_buf[512];
	if (CL_MODE_NO_TOPOLOGY == g_config.cluster_mode) {
		cf_info(AS_PARTITION, "Rack Aware is disabled.");
	} else {
		cf_info(AS_PARTITION, "\nRack Aware is enabled.  Mode: %s.",
				(CL_MODE_STATIC == g_config.cluster_mode ? CL_STR_STATIC : CL_STR_DYNAMIC));

		// For each group -- print out the stats.  This is somewhat
		// inefficient (N squared for N groups), but we don't expect N to
		// be a large number (usually under 10, NEVER over 64).
		cf_info(AS_PARTITION, "\n<<CLUSTER REPORT>> <<CLUSTER REPORT>> <<CLUSTER REPORT>> <<CLUSTER REPORT>>" );
		cf_info(AS_PARTITION, "\n<< CLUSTER STATE(%s) SelfNode(%"PRIx64") Group Count(%d) Total Node Count(%d) >>",
				cc_state_str[cc->cluster_state], g_config.self_node, cc->group_count, cc->node_count );
		for( i = 0; i < cc->group_count; i++ ) {
			sprintf(print_buf, "Group(%04x) GroupNodeCount(%u):: ",
					cc->group_ids[i], cc->group_node_count[i] );
			for( j = 0; j < cc->node_count; j++ ) {
				if( cc->membership[j] == i ) {
					buf_pos = strlen( print_buf ); // advance our position in buf
					sprintf(&(print_buf[buf_pos - 1]), " Node(%"PRIx64") ", cc->full_node_val[j]);
				}
			}
			cf_info(AS_PARTITION, "\n<<%s>>", print_buf );
		} // for each group
	} // Rack Aware Mode
} // end cc_show_cluster_state()

/**
 * Evaluate the state of the cluster.  Basically, count up the node counts
 * in all of the groups and, if they > 0 and equal, then it is a balanced
 * cluster.  Otherwise, it's unbalanced.  If it doesn't have
 * all of its fingers and toes, then it's invalid.
 */
cluster_state_t
cc_get_cluster_state( cluster_config_t * cc )
{
	int i;
	int first_group_count = 0;
	int first_group_ndx = 0; // Remember the ndx of 1st non-zero count. It SHOULD be 0.
	cluster_state_t result = unknown;
	if (CL_MODE_NO_TOPOLOGY == g_config.cluster_mode) {
		cf_info(AS_CFG, "Rack Aware is disabled.");
	} else {
		result = balanced;
		cf_info(AS_PARTITION, "Rack Aware is enabled.  Mode: %s.",
				(CL_MODE_STATIC == g_config.cluster_mode ? CL_STR_STATIC : CL_STR_DYNAMIC));

		if( DEBUG ) {
			printf("<< CLUSTER DEBUG:: Group Count(%d) >> \n", cc->group_count );
			for( i = 0; i < cc->group_count; i++ ) {
				printf("::Group(%d)[%04x] NodeCount(%d) \n", i, cc->group_ids[i], cc->group_node_count[i] );
			} // for each group
		}

		// For each group, check the counts; if they are not equal, then
		// declare this cluster unbalanced, and print out the counts.
		for( i = 0; i < cc->group_count; i++ ) {
			if( first_group_count == 0 ) {
				first_group_ndx = i;
				first_group_count = cc->group_node_count[i];
				cf_info(AS_PARTITION, "Setting First Group(%d)[%04x] Cnt(%d)", i, cc->group_ids[i], first_group_count );
			}
			if( first_group_count != cc->group_node_count[i] ) {
				result = unbalanced;
				cf_warning(AS_PARTITION, "UNBALANCED CLUSTER::Group Node Counts Differ: First Group(%04x):Cnt(%d) This Group(%04x)Cnt(%d)",
						cc->group_ids[first_group_ndx], first_group_count, cc->group_ids[i], cc->group_node_count[i] );
			}
		} // for each group
	} // Rack Aware Mode

	return result;
} // end cc_get_cluster_state()

/**
 * Log the status of the Rack Aware feature.
 * If verbose, when enabled, decode the fields of each node in the Paxos succession list.
 */
void
cc_cluster_config_dump(bool verbose)
{
	if (CL_MODE_NO_TOPOLOGY == g_config.cluster_mode) {
		cf_info(AS_PARTITION, "Rack Aware is disabled.");
	} else {
		cf_info(AS_PARTITION, "Rack Aware is enabled.  Mode: %s.",
				(CL_MODE_STATIC == g_config.cluster_mode ? CL_STR_STATIC : CL_STR_DYNAMIC));

		if (verbose) {
			as_paxos *p = g_config.paxos;
			bool self = false, principal = false;

			cf_node principal_node = as_paxos_succession_getprincipal();
			for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
				cf_node node = p->succession[i];
				if ((cf_node) 0 == node)
					continue;
				self = (node == g_config.self_node);
				principal = (node == principal_node);
				cf_info(AS_PARTITION, "SuccessionList[%d]: Node %"PRIx64" : Port %u ; GroupID %u ; NodeID %u %s%s",
						i, node, cc_compute_port(node), cc_compute_group_id(node), cc_compute_node_id(node), (self ? "[Self]" : ""), (principal ? "[Principal]" : ""));
			}
		}
	}
} // end cc_cluster_config_dump()
