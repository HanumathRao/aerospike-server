/*
 * udf_rw.h
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

#include <stdbool.h>
#include <stdint.h>

#include <aerospike/as_list.h>
#include <aerospike/as_result.h>
#include <citrusleaf/cf_digest.h>

#include "util.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "base/write_request.h"


// parameter read off from a transaction
#define UDF_MAX_STRING_SZ 128

typedef struct udf_call_s {
	bool			active;
	as_transaction	*transaction;
	char			filename[UDF_MAX_STRING_SZ];
	char			function[UDF_MAX_STRING_SZ];
	as_msg_field	*arglist;
	uint8_t			udf_type;
} udf_call;

typedef struct udf_response_udata_s {
	udf_call	*call;
	as_result	*res;
} udf_response_udata;

typedef enum {
	UDF_OPTYPE_NONE,
	UDF_OPTYPE_READ,
	UDF_OPTYPE_WRITE,
	UDF_OPTYPE_DELETE,
	UDF_OPTYPE_LDT_READ,
	UDF_OPTYPE_LDT_WRITE,
	UDF_OPTYPE_LDT_DELETE
} udf_optype;

#define UDF_OP_IS_DELETE(op) \
	(((op) == UDF_OPTYPE_DELETE) || ((op) == UDF_OPTYPE_LDT_DELETE))
#define UDF_OP_IS_READ(op) \
	(((op) == UDF_OPTYPE_READ) || ((op) == UDF_OPTYPE_LDT_READ))
#define UDF_OP_IS_WRITE(op) \
	(((op) == UDF_OPTYPE_WRITE) || ((op) == UDF_OPTYPE_LDT_WRITE))
#define UDF_OP_IS_LDT(op) \
	(((op) == UDF_OPTYPE_LDT_READ)        \
		|| ((op) == UDF_OPTYPE_LDT_WRITE) \
		|| ((op) == UDF_OPTYPE_LDT_DELETE))

// Main execute
typedef void (*send_callback) (as_result *res, udf_call *call, void *udata);

// Executes the script on a local record
int      udf_rw_local(udf_call *call, write_request *wr, udf_optype *optype);
bool     udf_rw_needcomplete(as_transaction *tr);
bool     udf_rw_needcomplete_wr(write_request *wr);
void     udf_rw_complete(as_transaction *tr, int retcode, char *filename, int lineno);
int      udf_call_init(udf_call *, as_transaction *);
void     udf_call_destroy(udf_call *);

as_val  *as_val_frombin(as_bin *bb);
void     as_val_tobuf(const as_val *v, uint8_t *buf, uint32_t *size);
int      to_particle_type(int from_as_type);

void     xdr_write(as_namespace *ns, cf_digest keyd, as_generation generation,
				   cf_node masternode, bool is_delete, uint16_t set_id);

// Initialize UDF subsystem.
void	as_udf_init(void);
