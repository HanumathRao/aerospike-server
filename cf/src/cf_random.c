/*
 * cf_random.c
 *
 * Copyright (C) 2008-2014 Aerospike, Inc.
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

#include "cf_random.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <openssl/rand.h>


#define SEED_SZ 64
static uint8_t rand_buf[1024 * 8];
static uint32_t rand_buf_off = 0;
static int seeded = 0;
static pthread_mutex_t rand_buf_lock = PTHREAD_MUTEX_INITIALIZER;

int
cf_rand_reload()
{
	if (seeded == 0) {
		int rfd = open("/dev/urandom", O_RDONLY);
		int rsz = read(rfd, rand_buf, SEED_SZ);
		if (rsz < SEED_SZ) {
			fprintf(stderr, "warning! can't seed random number generator");
			return(-1);
		}
		close(rfd);
		RAND_seed(rand_buf, rsz);
		seeded = 1;
	}
	if (1 != RAND_bytes(rand_buf, sizeof(rand_buf))) {
		fprintf(stderr, "RAND_bytes not so happy.\n");
		pthread_mutex_unlock(&rand_buf_lock);
		return(-1);
	}
	rand_buf_off = sizeof(rand_buf);
	return(0);
}


int
cf_get_rand_buf(uint8_t *buf, int len)
{
	if (len >= sizeof(rand_buf))	return(-1);

	pthread_mutex_lock(&rand_buf_lock);

	if (rand_buf_off < len ) {
		if (-1 == cf_rand_reload()) {
			pthread_mutex_unlock(&rand_buf_lock);
			return(-1);
		}
	}

	rand_buf_off -= len;
	memcpy(buf, &rand_buf[rand_buf_off] ,len);

	pthread_mutex_unlock(&rand_buf_lock);

	return(0);
}


uint64_t
cf_get_rand64()
{
	pthread_mutex_lock(&rand_buf_lock);
	if (rand_buf_off < sizeof(uint64_t) ) {
		if (-1 == cf_rand_reload()) {
			pthread_mutex_unlock(&rand_buf_lock);
			return(0);
		}
	}

	rand_buf_off -= sizeof(uint64_t);
	uint64_t r = *(uint64_t *) (&rand_buf[rand_buf_off]);
	pthread_mutex_unlock(&rand_buf_lock);
	return(r);
}

uint32_t
cf_get_rand32()
{
	pthread_mutex_lock(&rand_buf_lock);
	if (rand_buf_off < sizeof(uint64_t) ) {
		if (-1 == cf_rand_reload()) {
			pthread_mutex_unlock(&rand_buf_lock);
			return(0);
		}
	}

	rand_buf_off -= sizeof(uint64_t);
	uint64_t r = *(uint64_t *) (&rand_buf[rand_buf_off]);
	pthread_mutex_unlock(&rand_buf_lock);
	return(r);
}
