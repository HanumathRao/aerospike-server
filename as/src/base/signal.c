/*
 * signal.c
 *
 * Copyright (C) 2010-2014 Aerospike, Inc.
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
 * This code should be moved into the cf area, as it is generic to any given
 * system. the goal is simply to log something about the majority of crash
 * types.
 */

#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <execinfo.h> // provides backtrace

#include "fault.h"


// The mutex that the main function deadlocks on after starting the service.
extern pthread_mutex_t g_NONSTOP;
extern bool g_startup_complete;

// Signal handling provides another entry point to the system.

// WARNING
// The backtrace code allocates memory. In the case where we're using a
// different memory allocator, make sure we're using the right free() - the
// direct clib one.

sighandler_t g_old_term_handler = 0;
void
as_sig_handle_term(int sig_num)
{
	cf_warning(AS_AS, "Signal TERM received, shutting down");

	if (g_old_term_handler) {
		g_old_term_handler(sig_num);
	}

	if (! g_startup_complete) {
		cf_warning(AS_AS, "startup was not complete, exiting immediately");
		_exit(0);
	}

	pthread_mutex_unlock(&g_NONSTOP);
}

sighandler_t g_old_abort_handler = 0;
void
as_sig_handle_abort(int sig_num)
{
	cf_warning(AS_AS, "signal abort received, aborting");

	void *bt[50];
	int sz = backtrace(bt, 50);
	char **strings = backtrace_symbols(bt, sz);

	for(int i = 0; i < sz; ++i) {
		cf_warning(AS_AS, "stacktrace: frame %d: %s", i, strings[i]);
	}

	// N.B.:  This must literally be "free()", because "strings" is allocated by "backtrace_symbols()".
	free(strings);

	if (g_old_abort_handler) {
		g_old_abort_handler(sig_num);
	}
}

sighandler_t g_old_fpe_handler = 0;
void
as_sig_handle_fpe(int sig_num)
{
	cf_warning(AS_AS, "signal FPE received, aborting");

	void *bt[50];
	int sz = backtrace(bt, 50);
	char **strings = backtrace_symbols(bt, sz);

	for(int i = 0; i < sz; ++i) {
		cf_warning(AS_AS, "stacktrace: frame %d: %s", i, strings[i]);
	}

	// N.B.:  This must literally be "free()", because "strings" is allocated by "backtrace_symbols()".
	free(strings);

	if (g_old_fpe_handler) {
		g_old_fpe_handler(sig_num);
	}
}

sighandler_t g_old_int_handler = 0;
void
as_sig_handle_int(int sig_num)
{
	cf_warning(AS_AS, "Signal INT received, shutting down");

	if (g_old_int_handler) {
		g_old_int_handler(sig_num);
	}

	if (! g_startup_complete) {
		cf_warning(AS_AS, "startup was not complete, exiting immediately");
		_exit(0);
	}

	pthread_mutex_unlock(&g_NONSTOP);
}

sighandler_t g_old_hup_handler = 0;
void
as_sig_handle_hup(int sig_num)
{
	if (g_old_hup_handler) {
		g_old_hup_handler(sig_num);
	}

	cf_info(AS_AS, "Signal HUP received, rolling log");

	cf_fault_sink_logroll();
}

sighandler_t g_old_segv_handler = 0;
void
as_sig_handle_segv(int sig_num)
{
	cf_warning(AS_AS, "Signal SEGV received: stack trace");

	void *bt[50];
	int sz = backtrace(bt, 50);
	char **strings = backtrace_symbols(bt, sz);

	for(int i = 0; i < sz; ++i) {
		cf_warning(AS_AS, "stacktrace: frame %d: %s", i, strings[i]);
	}

	// N.B.:  This must literally be "free()", because "strings" is allocated by "backtrace_symbols()".
	free(strings);

	if (g_old_segv_handler) {
		g_old_segv_handler (sig_num);
	}

	_exit(-1);
}

void
as_signal_setup() {
	g_old_int_handler = signal(SIGINT , as_sig_handle_int);
	g_old_fpe_handler = signal(SIGFPE , as_sig_handle_fpe);
	g_old_term_handler = signal(SIGTERM , as_sig_handle_term);
	g_old_abort_handler = signal(SIGABRT , as_sig_handle_abort);
	g_old_hup_handler = signal(SIGHUP, as_sig_handle_hup);
	g_old_segv_handler = signal(SIGSEGV, as_sig_handle_segv);

	// Block SIGPIPE signal when there is some error while writing to pipe. The
	// write() call will return with a normal error which we can handle.
	struct sigaction sigact;

	memset(&sigact, 0, sizeof(sigact));
	sigact.sa_handler = SIG_IGN;
	sigemptyset(&sigact.sa_mask);
	sigaddset(&sigact.sa_mask, SIGPIPE);

	if (sigaction(SIGPIPE, &sigact, NULL) != 0) {
		cf_warning(AS_AS, "Not able to block the SIGPIPE signal");
	}
}
