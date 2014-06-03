/*
 * udf_logger.c
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


#include <stdio.h>

#include "fault.h"

#include "base/udf_logger.h"

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static int udf_logger_enabled(const as_logger *, const as_logger_level);
static as_logger_level udf_logger_level(const as_logger *);
static int udf_logger_log(const as_logger *, const as_logger_level, const char *, const int, const char *, va_list);

/*****************************************************************************
 * CONSTANTS
 *****************************************************************************/

static struct {
	cf_fault_context context;
} udf_logger_source = {
	.context = CF_MISC
};

static const as_logger_hooks udf_logger_hooks = {
	.destroy	= NULL,
	.enabled	= udf_logger_enabled,
	.level		= udf_logger_level,
	.log		= udf_logger_log
};

static const cf_fault_severity level_to_severity[5] = {
	[AS_LOGGER_LEVEL_ERROR]	= CF_WARNING,
	[AS_LOGGER_LEVEL_WARN]	= CF_WARNING,
	[AS_LOGGER_LEVEL_INFO]	= CF_INFO,
	[AS_LOGGER_LEVEL_DEBUG]	= CF_DEBUG,
	[AS_LOGGER_LEVEL_TRACE]	= CF_DETAIL
};

static const cf_fault_severity severity_to_level[5] = {
	[CF_CRITICAL]	= AS_LOGGER_LEVEL_ERROR,
	[CF_WARNING]	= AS_LOGGER_LEVEL_WARN,
	[CF_INFO]		= AS_LOGGER_LEVEL_INFO,
	[CF_DEBUG]		= AS_LOGGER_LEVEL_DEBUG,
	[CF_DETAIL]		= AS_LOGGER_LEVEL_TRACE
};

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

as_logger * udf_logger_new(cf_fault_context c) {
	udf_logger_source.context = c;
	return as_logger_new(&udf_logger_source, &udf_logger_hooks);
}

static int udf_logger_enabled(const as_logger * logger, const as_logger_level level) {
	cf_fault_context * ctx = (cf_fault_context *) logger ? logger->source : NULL;
	if ( ctx ) return cf_context_at_severity(*ctx, level_to_severity[level]);
	return level >= AS_LOGGER_LEVEL_INFO;
}

static as_logger_level udf_logger_level(const as_logger * logger) {
	extern cf_fault_severity cf_fault_filter[CF_FAULT_CONTEXT_UNDEF];
	cf_fault_context * ctx = (cf_fault_context *) logger ? logger->source : NULL;
	if ( ctx ) return severity_to_level[cf_fault_filter[*ctx]];
	return AS_LOGGER_LEVEL_INFO;
}

static int udf_logger_log(const as_logger * logger, const as_logger_level level, const char * file, const int line, const char * format, va_list args) {
	cf_fault_context * ctx = (cf_fault_context *) logger ? logger->source : NULL;
	cf_fault_severity severity = level_to_severity[level];
	if ( ctx && cf_context_at_severity(*ctx, severity)  ) {
		char message[1024] = { '\0' };
		vsnprintf(message, 1024, format, args);
		cf_fault_event(*ctx, severity, file, NULL, line, message);
	}
	return 0;
}
