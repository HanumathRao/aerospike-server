/*
 * b64.h
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
#include <citrusleaf/cf_b64.h>

/******************************************************************************
 * ALIASES
 ******************************************************************************/

// Move these to a common file at some point if they work.
#define base64_validate_input cf_base64_validate_input
#define base64_encode_maxlen cf_base64_encode_maxlen
#define base64_encode cf_base64_encode
#define base64_tostring cf_base64_tostring
#define base64_decode_inplace cf_base64_decode_inplace
#define base64_decode cf_base64_decode
#define base64_test cf_base64_test
