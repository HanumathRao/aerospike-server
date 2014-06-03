/*
 * clock.h
 *
 * Copyright (C) 2010 Aerospike, Inc.
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
#include <citrusleaf/cf_clock.h>

/******************************************************************************
 * ALIASES
 ******************************************************************************/

#define TIMESPEC_TO_MS_P CF_TIMESPEC_TO_MS_P
#define TIMESPEC_TO_MS CF_TIMESPEC_TO_MS
#define TIMESPEC_TO_US CF_TIMESPEC_TO_US
#define TIMESPEC_ADD_MS CF_TIMESPEC_ADD_MS
