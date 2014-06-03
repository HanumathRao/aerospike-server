/*
 * daemon.c
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

/*
 * process utilities
 */

#include "util.h" // we don't have our own header file

#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "fault.h"


void
cf_process_privsep(uid_t uid, gid_t gid)
{
	if (0 != getuid() || (uid == getuid() && gid == getgid())) {
		return;
	}

	/* Drop all auxiliary groups */
	if (0 > setgroups(0, (const gid_t *)0)) {
		cf_crash(CF_MISC, CF_CRITICAL, "setgroups: %s", cf_strerror(errno));
	}

	/* Change privileges */
	if (0 > setgid(gid)) {
		cf_crash(CF_MISC, CF_CRITICAL, "setgid: %s", cf_strerror(errno));
	}

	if (0 > setuid(uid)) {
		cf_crash(CF_MISC, CF_CRITICAL, "setuid: %s", cf_strerror(errno));
	}
}


/* Function to daemonize the server. Fork a new child process and exit the parent process.
 * Close all the file descriptors opened except the ones specified in the fd_ignore_list.
 * Redirect console messages to a file. */
void
cf_process_daemonize(int *fd_ignore_list, int list_size)
{
	int FD, j;
	char cfile[128];
	pid_t p;

	/* Fork ourselves, then let the parent expire */
	if (-1 == (p = fork())) {
		cf_crash(CF_MISC, CF_CRITICAL, "couldn't fork: %s", cf_strerror(errno));
	}

	if (0 != p) {
		exit(0);
	}

	/* Get a new session */
	if (-1 == setsid()) {
		cf_crash(CF_MISC, CF_CRITICAL, "couldn't set session: %s", cf_strerror(errno));
	}

	/* Drop all the file descriptors except the ones in fd_ignore_list*/
	for (int i = getdtablesize(); i > 2; i--) {
		for (j = 0; j < list_size; j++) {
			if (fd_ignore_list[j] == i) {
				break;
			}
		}

		if (j ==  list_size) {
			close(i);
		}
	}

	/* Open a temporary file for console message redirection */
	snprintf(cfile, 128, "/tmp/aerospike-console.%d", getpid());

	if (-1 == (FD = open(cfile, O_WRONLY|O_CREAT|O_APPEND,S_IRUSR|S_IWUSR))) {
		cf_crash(CF_MISC, CF_CRITICAL, "couldn't open console redirection file: %s", cf_strerror(errno));
	}

	if (-1 == chmod(cfile, (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH))) {
		cf_crash(CF_MISC, CF_CRITICAL, "couldn't set mode on console redirection file: %s", cf_strerror(errno));
	}

	/* Redirect stdout, stderr, and stdin to the console file */
	for (int i = 0; i < 3; i++) {
		if (-1 == dup2(FD, i)) {
			cf_crash(CF_MISC, CF_CRITICAL, "couldn't duplicate FD: %s", cf_strerror(errno));
		}
	}
}
