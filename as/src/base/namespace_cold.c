/*
 * namespace_cold.c
 *
 * Copyright (C) 2014 Aerospike, Inc.
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

#include <stdint.h>

#include "citrusleaf/alloc.h"

#include "arenax.h"
#include "fault.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/index.h"

void
as_xmem_scheme_check() {
	// For enterprise version only.
}

void
as_namespace_setup(as_namespace* ns, uint32_t instance)
{
	ns->cold_start = true;

	cf_info(AS_NAMESPACE, "ns %s beginning COLD start", ns->name);

	//--------------------------------------------
	// Set up the set name vmap.
	//

	ns->p_sets_vmap = (cf_vmapx*)cf_malloc(cf_vmapx_sizeof(sizeof(as_set), AS_SET_MAX_COUNT));

	if (! ns->p_sets_vmap) {
		cf_crash(AS_NAMESPACE, "ns %s can't allocate sets vmap", ns->name);
	}

	cf_vmapx_err vmap_result = cf_vmapx_create(ns->p_sets_vmap, sizeof(as_set), AS_SET_MAX_COUNT, 1024, AS_SET_NAME_MAX_SIZE);

	if (vmap_result != CF_VMAPX_OK) {
		cf_crash(AS_NAMESPACE, "ns %s can't create sets vmap: %d", ns->name, vmap_result);
	}

	// Transfer configuration file information about sets.
	if (! as_namespace_configure_sets(ns)) {
		cf_crash(AS_NAMESPACE, "ns %s can't configure sets", ns->name);
	}

	//--------------------------------------------
	// Set up the bin name vmap.
	//

	if (! ns->single_bin) {
		ns->p_bin_name_vmap = (cf_vmapx*)cf_malloc(cf_vmapx_sizeof(BIN_NAME_MAX_SZ, MAX_BIN_NAMES));

		if (! ns->p_bin_name_vmap) {
			cf_crash(AS_NAMESPACE, "ns %s can't allocate bins vmap", ns->name);
		}

		vmap_result = cf_vmapx_create(ns->p_bin_name_vmap, BIN_NAME_MAX_SZ, MAX_BIN_NAMES, 4096, BIN_NAME_MAX_SZ);

		if (vmap_result != CF_VMAPX_OK) {
			cf_crash(AS_NAMESPACE, "ns %s can't create bins vmap: %d", ns->name, vmap_result);
		}
	}

	//--------------------------------------------
	// Set up the index arena.
	//

	ns->arena = (cf_arenax*)cf_malloc(cf_arenax_sizeof());

	if (! ns->arena) {
		cf_crash(AS_NAMESPACE, "ns %s can't allocate index arena", ns->name);
	}

	cf_arenax_err arena_result = cf_arenax_create(ns->arena, 0, as_index_size_get(ns), 0, 0, CF_ARENAX_BIGLOCK);

	if (arena_result != CF_ARENAX_OK) {
		cf_crash(AS_NAMESPACE, "ns %s can't create arena: %s", ns->name, cf_arenax_errstr(arena_result));
	}
}

void
as_namespace_xmem_trusted(as_namespace *ns)
{
	// For enterprise version only.
}
