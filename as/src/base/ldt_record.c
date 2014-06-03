/*
 * ldt_record.c
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
 * as_record interface for large stack objects
 *
 */

#include "base/feature.h" // Turn new AS Features on/off (must be first in line)

#include "base/ldt_record.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_aerospike.h"
#include "aerospike/as_rec.h"
#include "aerospike/as_val.h"

#include "fault.h"


/*********************************************************************
 * FUNCTIONS                                                         *
 *                                                                   *
 * NB: Entire ldt_record is just a wrapper over the udf_record       *
 *     implementation                                                *
 ********************************************************************/
extern as_aerospike g_as_aerospike;
int
ldt_record_init(ldt_record *lrecord)
{
	// h_urec is setup in udf_rw.c which point to the main record
	memset(lrecord, 0, sizeof(ldt_record));
	lrecord->as      = &g_as_aerospike;

	// No versioning right now !!!
	lrecord->version = 0;
	for(int i = 0; i < MAX_LDT_CHUNKS; i++) {
		lrecord->chunk[i].slot = -1;
	}
	return 0;
}

static as_val *
ldt_record_get(const as_rec * rec, const char * name)
{
	static const char * meth = "ldt_record_get()";
	if (!rec || !name) {
		cf_warning(AS_UDF, "%s Invalid Paramters: record=%p, name=%p", meth, rec, name);
		return NULL;
	}
	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return NULL;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	return as_rec_get(h_urec, name);
}

static int
ldt_record_set(const as_rec * rec, const char * name, const as_val * value)
{
	static const char * meth = "ldt_record_set()";
	if (!rec || !name) {
		cf_warning(AS_UDF, "%s Invalid Paramters: record=%p, name=%p", meth, rec, name);
		return 2;
	}
	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	return as_rec_set(h_urec, name, value);
}

static int
ldt_record_set_flags(const as_rec * rec, const char * name,  uint8_t  flags)
{
	static const char * meth = "ldt_record_set_flags()";
	if (!rec || !name) {
		cf_warning(AS_UDF, "%s Invalid Paramters: record=%p, name=%p", meth, rec, name);
		return 2;
	}
	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	return as_rec_set_flags(h_urec, name, flags);
}

static int
ldt_record_set_type(const as_rec * rec,  uint8_t rec_type )
{
	static const char * meth = "ldt_record_set_type()";
	if (!rec) {
		cf_warning(AS_UDF, "%s Invalid Paramters: record=%p", meth, rec);
		return 2;
	}

	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	return as_rec_set_type(h_urec, rec_type);
}

static int
ldt_record_remove(const as_rec * rec, const char * name)
{
	static const char * meth = "ldt_record_remove()";
	if (!rec || !name) {
		cf_warning(AS_UDF, "%s Invalid Paramters: record=%p, name=%p", meth, rec, name);
		return 2;
	}
	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 2;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	return as_rec_remove(h_urec, name);
}

static uint32_t
ldt_record_ttl(const as_rec * rec)
{
	static char * const meth = "ldt_record_ttl()";
	if (!rec) {
		cf_warning(AS_UDF, "%s Invalid Paramters: record=%p", meth, rec);
		return 0;
	}
	ldt_record *lrecord   = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 0;
	}
	const as_rec *h_urec  = lrecord->h_urec;
	// TODO: validate record r status, and  correctly handle bad status.
	return as_rec_ttl(h_urec);
}

static uint16_t
ldt_record_gen(const as_rec * rec)
{
	static const char * meth = "ldt_record_gen()";
	if (!rec) {
		cf_warning(AS_UDF, "%s Invalid Paramters: record=%p", meth, rec);
		return 0;
	}
	ldt_record *lrecord  = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 0;
	}

	const as_rec *h_urec = lrecord->h_urec;
	// TODO: validate record r status, and  correctly handle bad status.
	return as_rec_gen(h_urec);
}

static as_bytes *
ldt_record_digest(const as_rec * rec)
{
	static const char * meth = "ldt_record_digest()";
	if (!rec) {
		cf_warning(AS_UDF, "%s Invalid Paramters: record=%p", meth, rec);
		return NULL;
	}

	ldt_record *lrecord  = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return 0;
	}
	const as_rec *h_urec = lrecord->h_urec;
	// TODO: validate record r status, and  correctly handle bad status.
	return as_rec_digest(h_urec);
}

static bool
ldt_record_destroy(as_rec * rec)
{
	static const char * meth = "ldt_record_destroy()";
	if (!rec) {
		cf_warning(AS_UDF, "%s Invalid Paramters: record=%p", meth, rec);
		return false;
	}

	ldt_record *lrecord = (ldt_record *)as_rec_source(rec);
	if (!lrecord) {
		return false;
	}
	as_rec *h_urec      = lrecord->h_urec;

	// Note: destroy of udf_record today is no-op because all the closing
	// of record happens after UDF has executed.
	// TODO: validate chunk handling here.
	FOR_EACH_SUBRECORD(i, lrecord) {
		as_rec *c_urec = &lrecord->chunk[i].c_urec;
		as_rec_destroy(c_urec);
		lrecord->chunk[i].slot = -1;
	}
	// Dir destroy should release partition reservation and
	// namespace reservation.
	as_rec_destroy(h_urec);
	return true;
}

const as_rec_hooks ldt_record_hooks = {
	.get		= ldt_record_get,
	.set		= ldt_record_set,
	.remove		= ldt_record_remove,
	.ttl		= ldt_record_ttl,
	.gen		= ldt_record_gen,
	.destroy	= ldt_record_destroy,
	.digest		= ldt_record_digest,
	.set_flags	= ldt_record_set_flags,
	.set_type	= ldt_record_set_type,
	.numbins	= NULL
};
