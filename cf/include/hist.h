/*
 * hist.h
 *
 * Copyright (C) 2009-2014 Aerospike, Inc.
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

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <citrusleaf/cf_atomic.h>
#include "dynbuf.h"


/* SYNOPSIS
 * For timing things, you want to know this histogram of what took how much time.
 * So have an interface where you create a histogram object, can dump a histogram object,
 * and can "start" / "stop" a given timer and add it to the histogram - multithread safe,
 * of course, because WE'RE CITRUSLEAF
 */

// #define USE_CLOCK
#define USE_GETCYCLES

#ifdef USE_CLOCK
#include <time.h>
#endif

//
// The counts are powers of two.
// count[0] is 1024 * 1024 a second
// count[13] is about a millisecond (1/1024 second)
// count[25] is a second
//

#define N_COUNTS 64
#define HISTOGRAM_NAME_SIZE 128

typedef struct histogram_counts_s {
	uint64_t count[N_COUNTS];
} histogram_counts;

typedef struct histogram_s {
	char name[HISTOGRAM_NAME_SIZE];
	cf_atomic_int n_counts;
	cf_atomic_int count[N_COUNTS];
} histogram;

typedef struct histogram_measure_s {
#ifdef USE_CLOCK
	struct timespec start;
#endif
#ifdef USE_GETCYCLES
	uint64_t start;
#endif
} histogram_measure;


extern histogram * histogram_create(const char *name);
extern void histogram_clear(histogram *h);
extern void histogram_dump( histogram *h );  // for debugging, dumps to stderr
extern void histogram_start( histogram *h, histogram_measure *hm);
extern void histogram_stop(histogram *h, histogram_measure *hm);
extern void histogram_get_counts(histogram *h, histogram_counts *hc);

#ifdef USE_GETCYCLES
extern void histogram_insert_data_point(histogram *h, uint64_t start);
extern void histogram_insert_delta(histogram *h, uint64_t delta);
#endif

// this is probably the same as the processor specific call, see if it works the same...

static inline uint64_t
hist_getcycles()
{
    int64_t c;

    __asm__ __volatile__ ("rdtsc" : "=A"(c) :: "memory");
    return(c);
}


/* SYNOPSIS
 * Linear histogram partitions the values into 20 buckets, < 5%, < 10%, < 15%, ..., < 95%, rest
 */

#define MAX_LINEAR_BUCKETS 100
#define INFO_SNAPSHOT_SIZE 2048

//
// The counts are linear.
// count[0] is the number in the first 5%
// count[1] is all data between 5% and 10%
// count[18] is all data between 90% and 95%
// count[19] is all data > 95%
//

typedef struct linear_histogram_counts_s {
	uint64_t count[MAX_LINEAR_BUCKETS];
} linear_histogram_counts;

typedef struct linear_histogram_s {
	char name[HISTOGRAM_NAME_SIZE];
	int num_buckets;
	uint64_t start;
	uint64_t bucket_offset;
	cf_atomic_int n_counts;
	cf_atomic_int count[MAX_LINEAR_BUCKETS];
	pthread_mutex_t info_lock;
	char info_snapshot[INFO_SNAPSHOT_SIZE];
} linear_histogram;

extern linear_histogram * linear_histogram_create(char *name, uint64_t start, uint64_t max_offset, int num_buckets);
extern void linear_histogram_destroy(linear_histogram *h);
extern void linear_histogram_clear(linear_histogram *h, uint64_t start, uint64_t max_offset); // Note: not thread-safe!
extern void linear_histogram_dump( linear_histogram *h );  // for debugging, dumps to stderr
extern void linear_histogram_save_info(linear_histogram *h);
extern void linear_histogram_get_info(linear_histogram *h, cf_dyn_buf *db);
extern void linear_histogram_get_counts(linear_histogram *h, linear_histogram_counts *hc);
extern uint64_t linear_histogram_get_total(linear_histogram *h);
extern void linear_histogram_insert_data_point(linear_histogram *h, uint64_t point);
extern size_t linear_histogram_get_index_for_pct(linear_histogram *h, size_t pct);
extern bool linear_histogram_get_thresholds_for_fraction(linear_histogram* h, uint32_t tenths_pct, uint64_t* p_low, uint64_t* p_high, uint32_t* p_mid_tenths_pct); // Note: not thread-safe!
extern bool linear_histogram_get_thresholds_for_subtotal(linear_histogram* h, uint64_t subtotal, uint64_t* p_low, uint64_t* p_high, uint32_t* p_mid_tenths_pct); // Note: not thread-safe!
