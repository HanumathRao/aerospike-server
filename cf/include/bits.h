/*
 * bits.h
 *
 * Copyright (C) 2008-2012 Aerospike, Inc.
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

#include <stdint.h>
#include <string.h>
#include <citrusleaf/cf_arch.h>
#include <citrusleaf/cf_bits.h>

/******************************************************************************
 * ASLIASES
 ******************************************************************************/

#define LogTable256 cf_LogTable256
#define bits_find_last_set cf_bits_find_last_set
#define bits_find_last_set_64 cf_bits_find_last_set_64
#define cf_bits_find_first_set(__x) ffs(__x)
#define cf_bits_find_first_set_64(__x) ffsll(__x)

/******************************************************************************
 * INLINE FUNCTIONS
 ******************************************************************************/

#ifdef MARCH_i686
static inline uint8_t * cf_roundup_p(uint8_t * p, uint32_t modulus) {
	uint32_t i = (uint32_t) p;
	i = cf_roundup(i, modulus);
	return (void *) i;
}
#elif MARCH_x86_64
static inline uint8_t * cf_roundup_p(uint8_t * p, uint32_t modulus) {
	uint64_t i = (uint64_t) p;
	i = cf_roundup_64(i, modulus);
	return (void *) i;
}
#else
    MISSING ARCHITECTURE
#endif
