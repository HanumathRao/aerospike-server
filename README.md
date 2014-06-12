# Aerospike Database Server

Welcome to the Aerospike Database Server source code tree!

## Build Prerequisites

The Aerospike Database Server can be built and deployed on various
current GNU/Linux platform versions, such as the Red Hat family (e.g.,
CentOS 6 or later), Debian 6 or later, and Ubuntu 10.04 or later.

### Dependencies

The majority of the Aerospike source code is written in the C
programming language, conforming to the ANSI C99 standard. Building
Aerospike requires the GCC 4.1 or later toolchain, with the standard
GNU/Linux development tools and libraries installed in the build
environment, including `autoconf` and `libtool`. In particular, the
following libraries are needed:

#### OpenSSL

OpenSSL 0.9.8b or later is required for cryptographic hash functions
(RIPEMD-160 & SHA-1) and pseudo-random number generation.

* The CentOS 6 OpenSSL packages to install are:  `openssl`,
`openssl-devel`, `openssl-static`.

* The Debian 6/7 and Ubuntu 10/12/14 OpenSSL packages to install are:
`openssl` and `libssl-dev`.

#### Lua 5.1

The Lua 5.1 language is required for User Defined Function (UDF) support.

* The CentOS 6 Lua packages to install are:  `lua`, `lua-devel`, and
`lua-static`.

* The Debian 6/7 and Ubuntu 10/12/14 Lua packages to install are:
`lua5.1` and `liblua5.1-dev`.

### Submodules

The Aerospike Database Server build depends upon 6 submodules:

| Submodule | Description |
|---------- | ----------- |
| asmalloc  | The ASMalloc Memory Allocation Tracking Tool |
| common    | The Aerospike Common Library |
| jansson   | C library for encoding, decoding and manipulating JSON data |
| jemalloc  | The JEMalloc Memory Allocator |
| lua-core  | The Aerospike Core Lua Source Files |
| mod-lua   | The Aerospike Lua Interface |

After the initial cloning of the `aerospike-server` repo., the
submodules must be fetched for the first time using the following
command:

	$ git submodule update --init

## Building Aerospike

### Default Build

	$ make          -- Perform the default build (no packaging.)

### Build Options

	$ make deb      -- Build the Debian (Ubuntu) package.

	$ make rpm      -- Build the Red Hat Package Manager (RPM) package.

	$ make tar      -- Build the "Every Linux" compressed "tar" archive (".tgz") package.

	$ make source   -- Package the source code as a compressed "tar" archive.

	$ make clean    -- Delete any existing build products, excluding built packages.

	$ make cleanpkg -- Delete built packages.

	$ make cleanall -- Delete all existing build products, including built packages.

	$ make strip    -- Build "strip(1)"ed versions of the server executables.

### Advanced Build Options

	$ make asm      -- Build the server with ASMalloc support.

### Overriding Default Build Options

	$ make {<Target>}* {<VARIABLE>=<VALUE>}*  -- Build <Target>(s) with optional variable overrides.

#### Example:

	$ make USE_JEM=0   -- Default build *without* JEMalloc support.

## Configuring Aerospike

Sample Aerospike configuration files are provided in `as/etc`.  The
developer configuration file, `aerospike_dev.conf`, contains basic
settings that should work out-of-the-box on most systems. The package
example configuration files, `aerospike.conf`, and the Solid State Drive
(SSD) version, `aerospike_ssd.conf`, are suitable for running Aerospike
as a system daemon.

These sample files may be modified for specific use cases (e.g., setting
network addresses, defining namespaces, and setting storage engine
properties) and tuned for for maximum performance on a particular
system.  Also, system resource limits may need to be increased to allow,
e.g., a greater number of concurrent connections to the database.  See
"man limits.conf" for how to change the system's limit on a process'
number of open file descriptors ("nofile".)

## Running Aerospike

There are several options for running the Aerospike database. Which
option to use depends upon whether the primary purpose is production
deployment or software development.

The preferred method for running Aerospike in a production environment
is to build and install the Aerospike package appropriate for the target
Linux distribution (i.e., an `".rpm"`, `".deb"`, or `".tgz"` file), and
then to control the state of the Aerospike daemon via the daemon init
script commands, e.g., `service aerospike start`.

A convenient way to run Aerospike in a development environment is to use
the following commands from within the top-level directory of the source
code tree (`aerospike-server`):

To create and initialize the `run` directory with the files needed for
running Aerospike, use:

	$ make init

or, equivalently:

	$ mkdir -p run/{log,work/{smd,{sys,usr}/udf/lua}}
	$ cp -pr modules/lua-core/src/* run/work/sys/udf/lua

To launch the server:

	$ make start

or, equivalently:

	$ target/Linux-x86_64/bin/asd --config-file as/etc/aerospike_dev.conf

To halt the server:

	$ make stop

or, equivalently:

	$ kill `cat run/asd.pid` ; rm run/asd.pid

Please refer to the full documentation on the Aerospike web site,
`www.aerospike.com`, for more detailed information about configuring
and running the Aerospike Database Server, as well as the about the
Aerospike client API packages for popular programming languages.
