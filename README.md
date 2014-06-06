# Aerospike Database Server

Welcome to the Aerospike Database Server source code tree!

## Prerequisites

The Aerospike Database Server can be built and deployed on various
current GNU/Linux platform versions, such as the Red Hat family (e.g.,
CentOS 5 or later), Debian 6 or later, and Ubuntu 10.04 or later.

### Dependencies

The majority of the Aerospike source code is written in the C
programming language, conforming to the ANSI C99 standard.  Building
Aerospike requires the GCC 4.1 or later toolchain, with the standard
GNU/Linux development tools and libraries installed in the build
environment. In particular, the following libraries are needed:

* OpenSSL 0.9.8b or later is required for cryptographic hash functions
(RIPEMD-160 & SHA-1) and pseudo-random number generation.

* User Defined Function (UDF) support requires Lua 5.1 or later.

### Submodules

The Aerospike Database Server build depends upon 6 submodules:

| Submodule | Description |
|---------- | ----------- |
| asmalloc | The ASMalloc Memory Allocation Tracking Tool |
| common | The Aerospike Common Library |
| jansson | C library for encoding, decoding and manipulating JSON data |
| jemalloc | The JEMalloc Memory Allocator |
| lua-core | The Aerospike Core Lua Source Files |
| mod-lua | The Aerospike Lua Interface |

After the initial cloning of the `aerospike-server` repo., the
submodules must be fetched for the first time using the following
command:

	$ git submodule update --init

## Usage

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
