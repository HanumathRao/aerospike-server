/*
 * ai_globals.h
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

#pragma once

#include "ai_types.h"
#include <citrusleaf/cf_shash.h>

/***************** Declare Global Variables *****************/


/*
 *  Column type definitions.
 */
extern char *Col_type_defs[];

/*
 *  Array of tables.
 */
extern r_tbl_t *Tbl;

/*
 *  Number of tables.
 */
extern int Num_tbls;

/*
 *  Table High Watermark = Number of tables currently allocated.
 */
extern uint32 Tbl_HW;

/*
 *  Dictionary mapping table names to their definitions.
 */
extern shash *TblD;

/*
 *  List of empty table IDs.
 */
extern cf_ll *DropT;

/*
 *  Array of indexes.
 */
extern r_ind_t *Index;

/*
 *  Number of indexes.
 */
extern int Num_indx;

/*
 *  Index High Watermark = Number of indexes currently allocated.
 */
extern uint32 Ind_HW;

/*
 *  Dictionary mapping index names to their definitions.
 */
extern shash *IndD;

/*
 *  List of empty index IDs.
 */
extern cf_ll *DropI;
