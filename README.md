# Aerospike Server

Welcome to the Aerospike Server!

## Prerequisites

The Aerospike Server can be built and deployed on various current
GNU/Linux platform versions, such as the Red Hat family (e.g., CentOS 5
or later), Debian 6 or later, and Ubuntu 10.04 or later.

### Dependencies

Building Aerospike requires the GCC 4.1 or later toolchain. User Defined
Function (UDF) support requires Lua 5.1 or later. OpenSSL 0.9.8b or
later is also required for cryptographic hash functions (RIPEMD-160 & SHA-1)
and pseudo-random number generation.

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
