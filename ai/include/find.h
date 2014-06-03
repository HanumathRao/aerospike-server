/*
 * find.h
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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
 * This file implements ind_table, index, column logic.
 */

#pragma once

#include "ai_types.h"

#define MATCH_INDICES(tmatch)							\
	cf_ll indl;											\
	cf_ll_init(&indl, ll_ai_match_destroy_fn, false);	\
	int matches = match_index(tmatch, &indl);           \
	INDS_FROM_INDL										\
	cf_ll_reduce(&indl, true, ll_ai_reduce_fn, NULL);

#define INDS_FROM_INDL									       \
	int  inds[indl.sz];			                               \
	cf_ll_element * ele;                                       \
	int i  = 0;											       \
	cf_ll_iterator *iter = cf_ll_getIterator(&indl, true);     \
	while ((ele = cf_ll_getNext(iter))) {					   \
		inds[i] = ((ll_ai_match_element *)ele)->match; i++;	   \
	}                                                          \
	cf_ll_releaseIterator(iter);

int find_index(int tmatch, icol_t *ic);

int match_index(int tmatch, cf_ll *indl);

int match_index_name(char *iname);

int find_partial_index(int tmatch, icol_t *ic);

int match_partial_index(int tmatch, cf_ll *indl);

int match_partial_index_name(char *iname);

int find_table(char *tname);

icol_t *find_column(int tmatch, char *column);
