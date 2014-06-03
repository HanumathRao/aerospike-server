/*
 * ldt.c
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

/* LDT Module
 *  BASICS
 *
 *    Structure: LDT bins are the bin types which can store large or linked
 *               data. The structures as
 *
 *              +------------------------------------------------------------
 * LDT RECORD   |       LDT BIN                  |                |     .....
 *              +------------------------------------------------------------
 *                  |  |        |       |
 *           +------+  +-----+  +-----+ +-----+
 *           | ESR  |  | SUB |  | SUB | | SUB |
 *           +------+  +-----+  +-----+ +-----+
 *                      |   |            |   |
 *                +-----+   +-----+      |   |
 *                | SUB |   | SUB |      |   |
 *                +-----+   +-----+      |   |
 *                                       |   |
 *                                 +-----+   +-----+
 *                                 | SUB |   | SUB |
 *                                 +-----+   +-----+
 *
 *  There are three parts to it
 *
 *  LDT RECORD : Which is the parent record which contain LDT bin type.
 *  LDT ESR    : It is LDT existence record one corresponding to each LDT bin.
 *  LDT SUBREC : Many different LDT subrecords which linked together to
 *               implement Large(linked) data type. The structure/organization
 *               and links between these are user programmable.
 *  LDT SUB    : SUBREC + ESR are collectively called SUB
 *
 *  LDT bin type is user exposed type AS_LDT. This type of bin can only be
 *  manipulated through System User Defined Function (SDF), through specially
 *  implemented functions.
 *
 *  Three types of maps
 *
 *  Record Property Map:  RPM_*
 *  Property Map:         PM__*
 *
 *  HIDDEN BIN TO STORE METADATA INFO
 *
 *              +------------------------------------------------------------
 * LDT RECORD   | REC_LDT_CTRL_BIN type:RPM |  LDT BIN  | LDT BIN | ....
 *              +------------------------------------------------------------
 *
 *              +--------------------------------
 * LDT BIN      | list {type:PM, type:M} | .....
 *              +--------------------------------
 *
 *              +------------------------------------------------------
 * LDT SUBREC   | SUBREC_PROP_BIN type:PM | other lua specific stuff | .....
 *              +-----------------------------------------------------
 *
 *              +------------------------------------------------------
 * LDT ESR      | SUBREC_PROP_BIN type:PM | ???
 *              +-----------------------------------------------------
 *
 *
 *  Example of implementation of (Large) Linked Data Type is LSTACK/LLIST/LSET.
 *
 *  ldt_aerospike.c ldt_record.c implement as_aerospike / as_record to expose
 *  primitives which are used by LUA to implement LDT.
 *
 *  This file implements functionality which is required for handling server
 *  side functionality like maintenance / expiry / expiration / deletion /
 *  read / write / migration / duplication resolution for the aerospike
 *  records containing Large (linked) data type bins.
 *
 *
 *  Major Changes:
 *
 *  - Partition structure will have its own tree from which the LDT subrecord
 *    will be hanging. This is to make sure LDT record and LDT subrecord
 *    has different search space. This is avoid digest collision between two
 *    spaces.
 *
 *  - The LDT subrecord has its own digest to identify it. Following logic is used to
 *    make sure that in entire life of server no two subrecord digest which is
 *    generated repeat.  [See ldt_aerospike.c for randomizer function ]
 *
 *      - 12 bits of partition distinguishing bits and hence LDT_SUB for record
 *        in two different partition are in different space. This also means the
 *        when LDT_SUB digest is generated on two different nodes they can never
 *        collide as at any point of time the write to record in a given partition
 *        happens only on one node.
 *
 *      - 2 bytes for lock has [2-3] for same lock
 *
 *      - 3 random bytes to make sure of uniqueness. At a given clock time on
 *        multiple node. [4-6]. The see for this random number if picked from
 *        the digest itself to make it thread safe for a digest.
 *
 *      - 1 byte to for the storage distribution ... [7]. We want to make sure
 *        LDT subrecord falls on the same device as the LDT record
 *
 *      - 6 bytes are based on system clock [8-14]
 *
 *      - 6 bytes of version which is generated at partition version creation
 *
 *
 *  - Every time partition migrates in it will generate a new version for the given
 *    LDT. This is to make sure that all the incoming subrecord will have new digest
 *    so the subrecord never collide and create new copies. When the parent record
 *    of the winner LDT moves in it will have the partition version stamped in it.
 *    All the subrecord will be lazily / aggressively cleaned up from the system.
 *    [ See migrate.c for the detail code for this ]
 *
 *  - Read/Write: Because LDT are huge size we cannot ship get op (duplicate
 *    resolution) for entire record. So for the records with LDT bins the request
 *    is send to all the duplicates [writes/UDF] as well, with the protection at
 *    the initiating node (master if it is sync / origin [acting master node] in
 *    case of proxy). In case op is read the result is returned back to the
 *    originating node along with the generation and winner result is sent back
 *    to the client. In case of write after applying UDF then normal replication
 *    is triggered to send result to replica set along with generation. And the
 *    winner generation wins. Need better solution. Also it is necessary to make
 *    sure that the ops have order what it means is when the op is send it gets
 *    executed unless cluster view has changed. [ ldt.c / proxy.c / thr_rw.c to
 *    see the shipped op logic ]
 *
 *    NB: See code for the details of locking and protection mechanism. And
 *        ACID semantics
 *    NB: See the code comments for the details of reducing the network bandwidth.
 *
 *  - Replication: Replication at both at the time of the duplicates/migration
 *    and at the normal runtime would pack entire changes done to LDT into one
 *    packet and send it over to the replica set. This is to make sure the
 *    Atomic semantics becomes less dependent on the network "finickiness". And
 *    make sure entire thing makes to replica [We still not solved the case
 *    where entire thing either makes to the storage or none makes it. But
 *    that cane be independent additional change]
 *
 *  - Migration: At the source of migrations the migration runs reduce on the
 *    primary and creates the list of the subrecord which is part of it. Then
 *    it walk through the subrecord and ships the entire list. Along with the
 *    sub record it sends generation of the parent LDT record and also the
 *    partition version. And in the end ships the LDT record.
 *
 *    At the target of the migration. All the incoming migrating subrecord
 *    is accepted and inserted into the migrate subrecord tree. The sub record
 *    is not accepted if incoming generation cannot win. [OPTIMIZATION: If
 *    the first subrecord (which is sent along with generation) fails to win
 *    then the entire LDT shipping is aborted to save the network band width]
 *    When finally the parent LDT is received then reduce function is run so
 *    in single context all the subrecord are moved from migrate tree to the
 *    main tree.
 *
 *    Sub record also has the partition version hidden bin. This is needed
 *    to make sure both version can coexist on the storage. So the cold restart
 *    can handle it. See cold restart logic for more detail of its usage.
 *
 *  - Expiry / Eviction : Nsup threads only runs on the LDT record tree. If
 *    the parent record is deleted then all the existence subrecord are deleted
 *    from the sub record tree. See the logic the version cleaned and delete
 *    thread logic for sub record tree for details of how subrecord gets cleaned
 *    up. (NOTE: NSup refers to the Namespace Supervisor thread, which takes
 *    care of record expiration and eviction.)
 *
 *  - Warm restart : Both the record and subrecord are tree are allocated from
 *    arena shared memory .. attaching at the boot time simply bring back the
 *    entire state of system as it was when the node went down.
 *
 *  - Subrecord Version clean / delete: A background thread walk through the
 *    sub record main tree and keep checking if the existence record is present
 *    for the LDT subrecord ... if it exists then it checks to see if the LDT
 *    parent record version matches. If it does not match the version of LDT
 *    subrecord is cleaned up. If it matches the version is retained. In case
 *    existence sub record is missing the subrecord is cleaned up from the
 *    sub record tree.
 *
 *    NB: When the bin is deletes the ESR record is simply deleted from the
 *    sub record tree. And LDT subrecords are lazily cleaned up.
 *
 *    See the details in code in case two different ESR show up for two
 *    different versions of LDT.
 *
 *  - Defrag : Defrag when walking through storage looks up key in both record
 *    tree and sub record tree. If not found the record is candidate for defrag.
 */

#include "base/feature.h" // Turn new AS Features on/off (must be first in line)

#include <base/datamodel.h>
#include <base/ldt.h>
#include <base/ldt_record.h>
#include <fabric/fabric.h>
#include <base/thr_rw_internal.h>
#include <base/write_request.h>
#include "base/thr_proxy.h"
#include "base/udf_rw.h"

#include <aerospike/as_types.h>
#include <aerospike/as_msgpack.h>

// Use this to turn on/off debug sections
#define DEBUG false

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// LDT specific Property Map (PM) Fields:
// One Prop Map per LDT (in the main record), and one Prop Map per subrecord
// bin.  For Sub Records, the property map is in bin: "SR_PROP_BIN", and is
// referenced by variable SUBREC_PROP_BIN (in both Aerospike C and Lua code).
// In Top Records, the contents of the LDT Bin  comprises a list of two maps.
// The first map (at location ldtList[1]) is the the Property Map (PM)
// and is same for all LDTs.  The second map (at location ldtList[2]) is a
// control map that is specific to the LDT (LSet, LList, LMap, LStack).
//
// We can access the Property Map from either the Lua code:   e.g.
// local ldtList = topRec[bin];
// local propMap = ldtList[1];
// local esrDigest = propMap[PM_EsrDigest];
//
// Or, we can access it from the C code:
// (NOTE: This needs to change if we change the code from which this example
//  was taken.)
//	char * prop_bin_name = SUBREC_PROP_BIN;
//	int namesz = strlen( prop_bin_name );
//	as_bin * binp = as_bin_get( rd, prop_bin_name, namesz );
//	as_val * valp = as_val_frombin( binp );
//	as_map * mapp = as_map_fromval(valp); // keep this step for debugging
//	as_bytes * digest_bytes = (as_bytes *) as_map_get( mapp, PM_ParentDigest );

//-- ------------------------------------------------------------------------
//-- Control Map Names: for Property Maps and Control Maps
//-- ------------------------------------------------------------------------
//-- Note:  All variables that are field names will be upper case.
//-- It is EXTREMELY IMPORTANT that these field names ALL have unique char
//-- values -- within any given map.  They do NOT have to be unique across
//-- the maps (and there's no need -- they serve different purposes).
//-- Note that we've tried to make the mapping somewhat cannonical where
//-- possible.
//
// Here are the fields (the contents) of the Property Maps.  We've annotated
// the fields that are used by TopRecords and SubRecords (and both).
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#define PM_ItemCount             'I' // (Top): Count of all items in LDT
#define PM_Version               'V' // (Top): Code Version
#define PM_LdtType               'T' // (Top): Type: stack, set, map, list
#define PM_BinName               'B' // (Top): LDT Bin Name
#define PM_Magic                 'Z' // (All): Special Sauce
#define PM_EsrDigest             'E' // (All): Digest of ESR
#define PM_RecType               'R' // (All): Type of Rec:Top,Ldr,Esr,CDir
#define PM_LogInfo               'L' // (All): Log Info (currently unused)
#define PM_ParentDigest          'P' // (Subrec): Digest of TopRec
#define PM_SelfDigest            'D' // (Subrec): Digest of THIS Record

// Here are the fields that are found in the SINGLE "Hidden" LDT Control Map.
//-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//-- Record Level Property Map (RPM) Fields: One RPM per record
//-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#define RPM_LdtCount             'C'  // Number of LDTs in this rec
#define RPM_Version              'V'  // Partition Version Info (6 bytes)
#define RPM_Magic                'Z'  // Special Sauce
#define RPM_SelfDigest           'D'  // Digest of this record


// Define the LDT Hidden Bin Name -- for any record that contains LDTs
#define REC_LDT_CTRL_BIN         "LDTCONTROLBIN"

// Define the Property Map Bin Name for Sub Records
#define SUBREC_PROP_BIN          "SR_PROP_BIN"
#define LDT_VERSION_SZ           6

/*
 * Used by migration to generate version at the beginning of partition
 * migration... based on the MAC address and current clock time....
 * LDT version is 5byte value .... that would mean there can be around
 * 2^40 different versions ... reasonably big number
 *
 * Much of this logic depends on the fact that the system clocks are in sync.
 */
uint64_t
as_ldt_generate_version()
{
	as_config * c    = &g_config;
	// MAC address for starting bits
	uint64_t version = c->hw_self_node;

	// clock for randomizer
	srand(cf_clock_getabsoluteus());

	// Randomize last 6 bytes
	version += (rand() + 1) & 0x0000FF;
	version += (rand() + 1) & 0x0000FF00;
	version += (rand() + 1) & 0x0000FF0000;
	version += (rand() + 1) & 0x0000FF000000;
	version += (rand() + 1) & 0x0000FF00000000;
	version += (rand() + 1) & 0x0000FF0000000000;
	return (version & 0x0000FFFFFFFFFFFF);
}

/*
 * Set version in the subrecord digest
 */
void
as_ldt_subdigest_setversion(cf_digest *dig, uint64_t version)
{
	int s = DIGEST_VERSION_START_POS;
	// overwrite the last 6 bytes
	cf_detail(AS_LDT, "Set Version %"PRIx64"", version);
	dig->digest[s]     = (version & 0xff0000000000) >> 40;
	dig->digest[s + 1] = (version & 0xff00000000) >> 32;
	dig->digest[s + 2] = (version & 0xff000000) >> 24;
	dig->digest[s + 3] = (version & 0xff0000) >> 16;
	dig->digest[s + 4] = (version & 0xff00) >> 8;
	dig->digest[s + 5] = (version & 0xff);
}

/*
 * Reset version in the subrecord digest
 */
void
as_ldt_subdigest_resetversion(cf_digest *dig)
{
	// overwrite the last 5 bytes
	for (int i = 0; i < LDT_VERSION_SZ; i++) {
		dig->digest[DIGEST_VERSION_START_POS + i] = 0;
	}
	return;
}


uint64_t
as_ldt_subdigest_getversion(cf_digest *dig)
{
	uint64_t version = 0;
	uint64_t temp = 0;
	temp = dig->digest[DIGEST_VERSION_START_POS];
	version += temp << 40;
	temp = dig->digest[DIGEST_VERSION_START_POS + 1];
	version += temp << 32;
	temp = dig->digest[DIGEST_VERSION_START_POS + 2];
	version += temp << 24;
	temp = dig->digest[DIGEST_VERSION_START_POS + 3];
	version += temp << 16;
	temp = dig->digest[DIGEST_VERSION_START_POS + 4];
	version += temp << 8;
	temp = dig->digest[DIGEST_VERSION_START_POS + 5];
	version += temp;

	return version;
}

void
as_ldt_subrec_storage_validate(as_storage_rd *rd, char *op)
{
	if (!as_ldt_record_is_sub(rd->r)) {
		cf_warning(AS_LDT, "LDT_INDEXBITS SUBREC bit not set in SUBREC index");
	}

	cf_digest esr_digest;
	if (as_ldt_subrec_storage_get_edigest(rd, &esr_digest)) {
		cf_detail(AS_LDT, "Cannot get esr digest");
	}
	cf_digest parent_digest;
	if (as_ldt_subrec_storage_get_pdigest(rd, &parent_digest)) {
		cf_detail(AS_LDT, "Cannot get parent digest");
	}

	as_partition_id  esr_pid    = as_partition_getid(esr_digest);
	as_partition_id  parent_pid = as_partition_getid(parent_digest);
	as_partition_id  subrec_pid = as_partition_getid(rd->r->key);
	cf_detail(AS_LDT, "parent_pid = %d, esr_pid=%d subrec_pid=%d",
			parent_pid, esr_pid, subrec_pid);

	if ((parent_pid != esr_pid) || (parent_pid != subrec_pid) || (esr_pid != subrec_pid)) {
		cf_info_digest(AS_LDT, &parent_digest, "Parent Digest: ");
		cf_info_digest(AS_LDT, &esr_digest, "ESR Digest: ");
		cf_info_digest(AS_LDT, &rd->r->key, "Sub-Rec Digest: ");

		cf_warning(AS_LDT, "%s Corrupted Property Map ... digest mismatch [%d %d %d]",
				op, parent_pid, esr_pid, subrec_pid);
	}
}

/*
 * Internal function: To generate digest for the chunk record
 *
 * Parameter:
 * 		ns:		        namespace of record
 * 		keyd(in/out):    digest of the parent digest
 *
 * Return: Nothing
 *
 * Description:
 * 		This function manipulates digest bits to produce the digest for the
 * 		chunk record. The bits in passed digest is changed.
 *
 * Side Note:
 * There is need to make sure sub record digest never repeat themselves for
 * because if they do collide then LDT_SUB for two different record may show
 * up as duplicates and no one can be winner. Deciding on one winner needs
 * complex re-randomization logic and update of parent SUBRECORD/RECORD of the
 * current LDT_SUB. This is could possibly lead to LDT corruption. Also if
 * LDT_SUB of same record come together committing atomic winner for a entire
 * LDT record is impossible.
 *
 * Randomizer_new Logic:
 *
 *   - 12 bits of partition distinguishing bits and hence LDT_SUB for record
 *     in two different partition are in different space. This also means the
 *     when LDT_SUB digest is generated on two different nodes they can never
 *     collide as at any point of time the write to record in a given partition
 *     happens only on one node.
 *
 *   - 2 bytes for lock has [2-3] for same lock
 *
 *   - 3 random bytes to make sure of uniqueness. At a given clock time on
 *     multiple node. [4-6]. The see for this random number if picked from
 *     the digest itself to make it thread safe for a digest.
 *
 *   - 1 byte to for the storage distribution ... [7]. We want to make sure
 *     LDT subrecord falls on the same device as the LDT record
 *
 *   - 6 bytes are based on system clock [8-14]
 *
 *   - 6 bytes of version which is generated at partition version creation
 *
 */
void
as_ldt_digest_randomizer(as_namespace *ns, cf_digest *dig)
{
	// 4 Bytes Randomizer. srand() and rand() used to make things node safe and
	// thread safe
	//
	// For collision to happen in digest randomizer. Two condition needs
	// to be met.
	// 1. The four bytes from digest scrambler start position should match
	// 2. digest_randomizer_seed should match (This is based on MAC address)
	// 3. And above two should happen at the same clock tick of microsecond
	//    precision.
	//
	// Reasonably ok Randomizer of digest bits
	as_config * c = &g_config;
	uint32_t digest_randomizer_seed =  c->hw_self_node & 0xffffffff;
	srand(digest_randomizer_seed);

	// 3 bytes make there could be 10^8 digest generated in a microsecond
	// window to have collision
	dig->digest[DIGEST_SCRAMBLE_BYTE1] = dig->digest[DIGEST_SCRAMBLE_BYTE1] + rand() + 1;
	dig->digest[DIGEST_SCRAMBLE_BYTE2] = dig->digest[DIGEST_SCRAMBLE_BYTE1 + 1] + rand() + 1;
	dig->digest[DIGEST_SCRAMBLE_BYTE3] = dig->digest[DIGEST_SCRAMBLE_BYTE2 + 1] + rand() + 1;

	// 6 bytes system clock in microsecond ... make it good for 10^16 microseconds
	// = 317 years
	uint64_t clock = cf_clock_getabsoluteus();
	dig->digest[DIGEST_CLOCK_ZERO_BYTE]                  = ((clock & 0x0000ff0000000000) >> 40);
	*(uint64_t *)(&dig->digest[DIGEST_CLOCK_START_BYTE]) = ((clock & 0x000000ffffffffff)); // 6 byte clock

	as_ldt_subdigest_resetversion(dig);
}

/*
 * LDT Read/Write/apply UDF Algorithm
 *
 * When the user request reaches the master or acting master (in case master is
 * not in sync mode origin is acting master, master proxy request to the
 * acting master)
 * Following code path is followed
 *
 * -- thr_tsvc
 *    -- as_rw_start
 *       -- internal_rw_start
 *
 * In case duplicate resolution is triggered and if in the duplicates that are
 * received, if any has LDT bin, there can two possible cases
 *  case 1: The record which wins the duplicate resolution has LDT bin
 *  case 2: The record which wins the duplicate resolution does not have
 *          LDT bin.
 *
 * In case 2 the winner record is written locally and then normal code path of
 * read/write/UDF apply is followed.
 *
 * NB: When duplicate resolution request is received by the node having duplicate
 *     version of partition it only sends back the place holder for the LDT bins
 *     and not the entire record. [Reason being the entire record with LDT bin may
 *     be prohibitively large. There could some sort of bounds of up to which size
 *     it can sent and after which it cannot be for duplicate resolution, but this
 *     could mean sending back many records in one single packet].
 *
 * In case 1 the Read/Write/UDF apply is sent to the winner node. At the winner
 * node the op (winner node actually creates transaction has write hash etc and
 * lock; this actually means the synchronization is done both at the master and
 * the node where op is being performed .. but write hash entry is needed so that
 * winner node can replicate) ... in case the task is running on winner node which
 * is migrating its partition to the remote the replication has some special
 * condition where in it has to make sure that unless the partition has reached
 * the record replication stage only subrecords are sent And in case the partition
 * is still being replicated in the subrecord go into migrate tree [please see the
 * details of migration in the migration logic section].
 *
 * Weird (unexpected) Cases
 *
 * 1. If replication is slow for some reason and the client request times out.
 *    Not sure how to handle it.
 *
 * 2. If the winner node dies without replicating ... then the data is kind of
 *    un available and probably lost if the LDT bin get updates in the node's
 *    absence. We still do not have mechanism of merging LDT ...
 *
 * NB: In future if we decide to do record merging instead of the entire one
 *     record overwriting other... this mechanism of shipping op will not work
 *     because record needs to be merged always before any operation on LDT
 *     can be performed. To merge two versions of records both have to be on
 *     the same node all the time which is kind of difficult. Other options
 *     is all the ops go to all the nodes. And no replication happens and let
 *     the record come in the normal course of replication.
 */
int
as_ldt_shipop(write_request *wr, cf_node dest_node)
{
	// Create transaction to trigger proxy request. The only
	// parts of transaction needed is msgp / keyd / proto_fd
	as_proxy_shipop(dest_node, wr);
	return 0;
}

extern int as_record_flatten_component(as_partition_reservation *rsv, as_storage_rd *rd,
									   as_index_ref *r_ref, as_record_merge_component *c);
int
as_ldt_flatten_component(as_partition_reservation *rsv, as_storage_rd *rd,
					as_index_ref *r_ref, as_record_merge_component *c)
{

	// Setup index flags
	if (COMPONENT_IS_LDT_ESR(c)) {
		as_index_set_flags(r_ref->r, AS_INDEX_FLAG_CHILD_ESR);
	} else if (COMPONENT_IS_LDT_SUBREC(c)) {
		as_index_set_flags(r_ref->r, AS_INDEX_FLAG_CHILD_REC);
	} else if (COMPONENT_IS_LDT_PARENT(c)) {
		as_index_set_flags(r_ref->r, AS_INDEX_FLAG_SPECIAL_BINS);
	} else {
		cf_warning(AS_LDT, "NON LDT record fell through into LDT flatten code");
	}

	// default to normal record flattening
	return as_record_flatten_component(rsv, rd, r_ref, c);
}

/*
 * This is an internal function, that will get the requested prop_type from
 * inside prop_map and populate it inside value pointer(type of which is
 * determined by property type). Caller has responsibility of making sure
 * value has enough space.
 *
 * Create the key field to access the map. We're starting with a single char,
 * so we have to turn that into a string, and then that string into an as_val,
 * which is an as_string.
 *
 * Parameters: prop_map    : Property Map
 *             prop_type   : Type of property requested
 *             value(out)  : Value to be populated. Type is determined by prop_type
 *
 * Returns: 0 in case of success
 *          o/w failure
*/
int
as_ldt_get_from_map(const as_map *prop_map, char prop_type, void *value)
{
	cf_detail(AS_LDT, "Property %c from map", prop_type);
	// Create the key field to access the map.  We're starting with a single char,
	// so we have to turn that into a string, and then that string into an as_val,
	// which is an as_string.
	switch(prop_type) {
		case PM_EsrDigest:
		case PM_ParentDigest:
		case PM_SelfDigest:
		{
			char key_buffer[2];
			key_buffer[0]           = prop_type;
			key_buffer[1]           = 0;
			as_string key_val;
			as_string_init(&key_val, key_buffer, false);
			as_bytes * digest_bytes = (as_bytes *) as_map_get( (const as_map *)prop_map, (as_val *)&key_val);
			if (!digest_bytes) {
				cf_warning(AS_LDT, "Could not find %c type info in property map",
						prop_type);
				// Not necessary to destroy key_val.
				return -2;
			}
			as_ldt_bytes_todigest(digest_bytes, (cf_digest *) value);
			if ( DEBUG) {
				char * valstr = as_val_tostring( (as_val *) digest_bytes);
				cf_detail(AS_LDT, "Got digest %s", valstr);
				cf_free(valstr);
			}
			// Not necessary to destroy digest_bytes or key_val.
			break;
		}
		case RPM_Version:
		{
			char key_buffer[2];
			key_buffer[0]           = prop_type;
			key_buffer[1]           = 0;
			as_string key_val;
			as_string_init(&key_val, key_buffer, false);
			as_integer *int_valp    = (as_integer *)as_map_get((const as_map *)prop_map, (as_val *)&key_val);
			if (!int_valp) {
				cf_warning(AS_LDT, "Failed to get version %c", prop_type);
				// Not necessary to destroy key_val.
				return -2;
			}
			*((uint64_t *)value)    = as_integer_toint(int_valp);

			// when using as_val_tostring(), always place in a temp var and
			// then free it after use.
			if ( DEBUG ) {
				char * valstr1 = as_val_tostring((as_val *) &key_val);
				char * valstr2 = as_val_tostring((as_val *) int_valp);
				cf_detail(AS_LDT, "Get Version Key(%s):Int(%s)",
						valstr1, valstr2);
				cf_free(valstr1);
				cf_free(valstr2);
			}
			// Not necessary to destroy key_val or int_val.
			break;
		}
		default:
		{}
	}
	return 0;
}

/*
 * This is an internal function, that will set the passed in value for the
 * prop_type in the property map.
 *
 * Create the key field to access the map. We're starting with a single char,
 * so we have to turn that into a string, and then that string into an as_val,
 * which is an as_string.
 *
 * Parameters: prop_map    : Property Map
 *             prop_type   : Type of property requested to be set
 *             value       : Value to be set. Type is determined by prop_type
 *
 * Returns: 0 in case of success
 *          o/w failure
 */
int
as_ldt_set_in_map(as_map *prop_map, char prop_type, void *value)
{
	cf_debug(AS_LDT, "[ENTER] PropType(%c)", prop_type );

	int rv = 0;
	switch(prop_type) {
		case RPM_Version:
		{
			uint64_t ldt_version = *(uint64_t *)value;
			char key_buffer[2];
			key_buffer[0]           = prop_type;
			key_buffer[1]           = 0;
			as_string *key_val =  as_string_new_strdup(key_buffer);
			as_integer *int_val = as_integer_new(ldt_version);
			if (int_val) {
				// when using as_val_tostring(), always place in a temp var and
				// then free it after use.
				as_map_set(prop_map, (as_val *)key_val, (as_val *)int_val);
				if ( DEBUG ) {
					char * valstr1 = as_val_tostring((as_val*)key_val);
					char * valstr2 = as_val_tostring((as_val*)int_val);
					cf_detail(AS_LDT, "Set Version %s:%s", valstr1, valstr2);
					cf_free(valstr1);
					cf_free(valstr2);
				}
			} else {
				cf_detail(AS_LDT, "Failed to set version %c=%d",
						prop_type, ldt_version);
				// note: not necessary to destroy key_val here.
				rv = -2;
			}
			break;
		}
		default:
		{}
	}
	return rv;
}


/*
 * Sets the version value on the record. The passed in record should be
 * opened / locked and partition reserved. New bin will be created in case
 * the bin does not exits (Could happen but not sure yet how !!!)
 *
 * Parameters:
 * 			rd:          Record to set version into.
 * 			ldt_version: 5 LSB as version for 8 passed in bytes
 *
 * Returns: 0 in case of success
 *          o/w failure
 */
int
as_ldt_parent_storage_set_version(as_storage_rd *rd, uint64_t ldt_version, uint8_t **pp_stack_particles)
{
	// No op when version is disabled
	if (!rd->ns->ldt_enabled)
		return 0;

	as_bin * binp           = as_bin_get(rd, (byte *)REC_LDT_CTRL_BIN, strlen(REC_LDT_CTRL_BIN));
	int rv                  = 0;
	if (!binp) {
		cf_debug(AS_LDT, "Control bin not found");
		return -1;
	}
	as_val * valp           = as_val_frombin( binp );
	if (!valp) {
		cf_debug(AS_LDT, "Control bin not found");
		return -2;
	}

	// We must always retrieve typed values from as_val using the type specific
	// accessor function -- which will return NULL if we guessed wrong on the
	// type that we're extracting.
	as_map * prop_map        = as_map_fromval(valp);
	if( !prop_map ) {
		cf_warning(AS_LDT, "Control bin is not of type MAP");
		return -2;
	}
	rv = as_ldt_set_in_map(prop_map, RPM_Version, (void *)&ldt_version);

	if (rv) {
		cf_debug(AS_LDT, "Could not set map ");
		return -3;
	}
	// as_val_tostring() values must always be captured and freed.
	char * valstr =  as_val_tostring(valp);
	cf_detail(AS_LDT, "After property map set result %s", valstr );
	cf_free(valstr);

	// Abstract it out in some API .. bad duplication here  ...
	as_buffer buf;
	as_buffer_init(&buf);
	as_serializer s;
	as_msgpack_init(&s);
	as_serializer_serialize(&s, valp, &buf);

#if 0
	// Check not needed space is already there
	if ( !as_storage_bin_can_fit(rd->ns, buf.size) ) {
		cf_warning(AS_UDF, "map-list: bin size too big");
		rsp = -1;
	}
#endif

	if (rd->ns->storage_data_in_memory) {
		as_particle_frombuf(binp, AS_PARTICLE_TYPE_HIDDEN_MAP, (uint8_t *) buf.data, buf.size, NULL, true);
	}
	else {
		uint8_t pbytes = buf.size + as_particle_get_base_size(AS_PARTICLE_TYPE_HIDDEN_MAP);
		as_particle_frombuf(binp, AS_PARTICLE_TYPE_HIDDEN_MAP, (uint8_t *) buf.data,
					buf.size, *pp_stack_particles, rd->ns->storage_data_in_memory);
		*pp_stack_particles += pbytes;
	}
	as_serializer_destroy(&s);
	as_buffer_destroy(&buf);
	as_val_destroy(valp);

	rd->write_to_device = true;
	return rv;
}

/*
 * Gets the version value on the record. The passed in record should be
 * opened / locked and partition reserved. New bin will be created in case
 * the bin does not exits (Could happen but not sure yet how !!!)
 *
 * Returns: 0 in case of success
 *          o/w failure
 *
 * TODO: We need a better version not partition version and preferably something
 *      in memory rather than storage.
 *
 * Parameter: vinfo (out) gets populated with the version information
 */
int
as_ldt_parent_storage_get_version(as_storage_rd *rd, uint64_t *ldt_version)
{
	// No op when version is disabled
	if (!rd->ns->ldt_enabled)
		return 0;

	// Pull out bin
	as_bin * binp           = as_bin_get(rd, (byte *)REC_LDT_CTRL_BIN, strlen(REC_LDT_CTRL_BIN));
	int       rv            = 0;
	if (!binp) {
		if (as_ldt_record_is_parent(rd->r)) {
			cf_warning(AS_LDT, "Control bin not found LDT parent record");
		} else {
			cf_detail(AS_LDT, "Control bin not found");
		}
		rv                      = -1;
	} else {
		const as_val * valp           = as_val_frombin( binp );
		// We must always retrieve typed values from as_val using the type specific
		// accessor function -- which will return NULL if we guessed wrong on the
		// type that we're extracting.
		const as_map * prop_map        = as_map_fromval(valp);
		if( !prop_map ) {
			cf_warning(AS_LDT, "Control bin is not of type MAP");
			return -2;
		}

		rv                      = as_ldt_get_from_map(prop_map, RPM_Version, (void *)ldt_version);
		as_val_destroy(valp);
	}
	cf_detail(AS_LDT, "Version Search find %ld with rv = %d", *ldt_version, rv);
	return rv;
}


/*
 * Given the record pointer, and bin_name, get the LDT Property bin,
 * and from that get the ESR digest.
 * The design of all LDTs is that the LDT bin contents comprises a list of
 * two maps.  The first map (at location list[1]) is the same for all LDTs.
 *
 * Unpack the Property map and look inside it's soul (for that pass it into
 * as_ldt__get_digest_from_pmap.
 *
 * Parameters: rd:         : Opened storage record.
 *			   digest_type : Type of digest value to pull out ESR/PARENT
 * 			   keyd (out)  : key digest for the parent record is filled up.
 *             NULL:         in case of failure
 *
 * Returns: 0 in case of success
 *          o/w failure
 */
int
as_ldt_subrec_storage_get_digest(as_storage_rd *rd, char digest_type, cf_digest *keyd)
{
	if (!rd || !keyd) {
		cf_warning(AS_LDT, "Invalid Paramter [%p %p]", rd, keyd);
		return -1;
	}

	as_bin * binp        = as_bin_get(rd, (byte *)SUBREC_PROP_BIN, strlen(SUBREC_PROP_BIN));
	if (!binp) {
		cf_debug(AS_LDT, "Property Bin Not found");
		return -1;
	}

	as_val * valp        = as_val_frombin(binp);
	if (!valp) {
		cf_warning(AS_LDT, "Property Bin %s Corrupted", SUBREC_PROP_BIN);
		return -2;
	}
	cf_debug(AS_LDT, "Got a value from the bin: type(%d)", valp->type);

//	const as_map * prop_map    = as_map_fromval(valp); // keep this step for debugging

	// We must always retrieve typed values from as_val using the type specific
	// accessor function -- which will return NULL if we guessed wrong on the
	// type that we're extracting.
	const as_map * prop_map        = as_map_fromval(valp);
	if( !prop_map ) {
		cf_debug(AS_LDT, "Control bin is not of type MAP");
		return -2;
	}

	// when using as_val_tostring(), always place in a temp var and
	// then free it after use.
	char * valstr = as_val_tostring( valp );
	cf_debug(AS_LDT, "Map %s", valstr );
	cf_free(valstr);

	if (as_ldt_get_from_map(prop_map, digest_type, keyd)) {
		as_val_destroy(valp);
		cf_warning(AS_LDT, "Property Bin %s Corrupted cannot get %c digest",
				SUBREC_PROP_BIN, digest_type);
		return -3;
	}
	as_val_destroy(valp);
	return 0;
}


/*
 * Return the digest of the sub record's parent.
 */
int
as_ldt_subrec_storage_get_pdigest(as_storage_rd *rd, cf_digest *keyd)
{
	return as_ldt_subrec_storage_get_digest(rd, PM_ParentDigest, keyd);
}

/*
 * Return the digest of the sub record's Existence Sub Record.
 */
int
as_ldt_subrec_storage_get_edigest(as_storage_rd *rd, cf_digest *keyd)
{
	return as_ldt_subrec_storage_get_digest(rd, PM_EsrDigest, keyd);
}

inline void
as_ldt_record_set_rectype_bits(as_index *r, const as_rec_props *props)
{
	uint16_t *ldt_rectype_bits;
	if (props->p_data) {
		if (as_rec_props_get_value(props, CL_REC_PROPS_FIELD_LDT_TYPE, NULL,
									(uint8_t**)&ldt_rectype_bits) == 0) {
			cf_detail(AS_LDT, "Setting flag %d in index", *ldt_rectype_bits);
			as_index_set_flags(r, *ldt_rectype_bits);
		}
	}
}

/*
 * This is debug validation code. Following validations are done.
 * - In case it is LDT ESR it has to have correct digest to the PARENT. Its version
 *   matches that of the parent. And also matches what is in its digest.
 * - In case it is LDT SUBREC it has to have correct digest to the ESR / PARENT. Its
 *   version matches that of the ESR it is pointing to as also with the PARENT
 */
inline bool
as_ldt_flag_has_subrec(uint16_t flag)
{
	if (flag & AS_INDEX_FLAG_CHILD_REC) {
		return true;
	} else {
		return false;
	}
}

inline bool
as_ldt_flag_has_esr(uint16_t flag)
{
	if (flag & AS_INDEX_FLAG_CHILD_ESR) {
		return true;
	} else {
		return false;
	}
}

inline bool
as_ldt_flag_has_sub(uint16_t flag)
{
	if ((flag & AS_INDEX_FLAG_CHILD_REC)
			|| (flag & AS_INDEX_FLAG_CHILD_ESR)) {
		return true;
	} else {
		return false;
	}
}

inline bool
as_ldt_flag_has_parent(uint16_t flag)
{
	if (flag & AS_INDEX_FLAG_SPECIAL_BINS) {
		return true;
	} else {
		return false;
	}
}

/*
 * Function to peek in ESR to determine if the version of the subrec
 * and the one in ESR matches. If it does then return true otherwise
 * false
 *
 * Assumption: the partition is reserved and tree is locked already
 * ESR record when opened in here is with skip_lock.
 */
bool
as_ldt_version_match(uint64_t subrec_version, as_index_tree *tree, cf_digest *keyd, as_namespace *ns)
{
	int rv = 0;
	as_storage_rd rd;
	as_index_ref  r_ref;
	r_ref.skip_lock = true;

	if (as_record_get(tree, keyd, &r_ref, ns)) {
		cf_warning(AS_LDT, "LDT_SUB_GC Could not find parent record");
		return false;
	}
	as_index *r     = r_ref.r;

	if (!as_ldt_record_is_parent(r)) {
		// if parent is not a LDT parent version does not match
		cf_warning(AS_LDT, "LDT_INDEXBIT Expected Parent Bits Not Found");
		as_record_done(&r_ref, ns);
		return false;
	}

	// TODO: this sucks for defrag it will blow up number of I/O that
	// is performed ...
	// TODO: Need to make sure version info is in the index for storage
	// on disk case
	rv              = as_storage_record_open(ns, r, &rd, keyd);
	if (0 != rv) {
		cf_warning(AS_UDF, "LDT_SUB_GC Could not open record %"PRIx64"!! @ version rv=%d", *(uint64_t *)keyd, rv);
		as_record_done(&r_ref, ns);
		return false;
	}
	rd.n_bins = as_bin_get_n_bins(r, &rd);
	as_bin stack_bins[rd.ns->storage_data_in_memory ? 0 : rd.n_bins];
	rd.bins = as_bin_get_all(r, &rd, stack_bins);

	uint64_t parent_version = 0;
	rv              = as_ldt_parent_storage_get_version(&rd, &parent_version);
	if (0 != rv) {
		cf_detail(AS_LDT, "LDT_SUB_GC Something wrong could not get LDT parent version rv = %d", rv);
		goto Cleanup;
	}

	cf_detail(AS_LDT, "LDT_SUB_GC Subrec and parent version check %"PRIx64" %ld != %ld %p",
			  *(uint64_t *)keyd, parent_version, subrec_version, rd.r);
	if (parent_version == subrec_version) {
		rv = 0;
	} else {
		rv = -4;
	}

Cleanup:
	as_storage_record_close(r, &rd);
	as_record_done(&r_ref, ns);
	if (rv) return false;
	else    return true;
	return rv;
}

/*
 * Main thread that walks through the LDT subrecord tree and cleans
 * up stale SUBRECORD entries. This is kind of parallel to the nsup
 * thread ... it has the following responsibilities:
 *
 * 1. LDT subrecord cleanup if the parent LDT record is deleted
 *
 * 2. LDT subrecord cleanup if the LDT bin is deleted
 *
 * 3. LDT subrecord cleanup in case after migration some stale versions
 *    of are lying around.
 *
 * Synchronization:
 * 		Will hold sub_tree lock while creating list
 * 		Will hold subrecord lock while cleaning it up and deleting it from tree
 * 		Will hold tree and record lock while determining if the record
 * 		need to be deleted.
 *
 * Side effect:
 *
 *    Sub records will be deleted
 *    Memory usage counters will be updated
 *
 * Returns : Never
 *
 * Parameter: None
 *
 * TODO: make sure this starts after the data is already loaded
 * from the disk
 *
 * TODO: Not needed once we have 128byte subrecord index record
 * need to be opened only in case of storage in memory case, for storage
 * acccounting
 */
void
as_ldt_sub_gc_fn(as_index_ref *r_ref, void *udata)
{
	ldt_sub_gc_info *linfo  = (ldt_sub_gc_info *)udata;
	as_index *r             = r_ref->r;
	as_namespace *ns        = linfo->ns;
	as_partition *p         = &ns->partitions[as_partition_getid(r->key)];

	// If record is migrating avoid sub tree GC in general. But it is too big
	// a window ... We can actually migrate when the partition is sending out
	// data ...fix it !!!
	if ((p->rxstate == AS_PARTITION_MIG_RX_STATE_RECORD)
			|| (p->rxstate == AS_PARTITION_MIG_RX_STATE_SUBRECORD)) {
		cf_detail(AS_LDT, " Skipping Defrag Partition Mig State is RECV_SUBRECORD");
		as_record_done(r_ref, ns);
		return;
	}

	// TODO: Enable the check
	//	if (r->void_time != 0) {
	//		cf_warning(AS_LDT, "No void time should be set in subrecord !!! found %d", r->void_time);
	//	}

	if (!as_ldt_record_is_sub(r)) {
		cf_warning(AS_LDT, "LDT_Defrag: Missing Index bits !!");
		as_record_done(r_ref, ns);
		return;
	}

	// STEP 1: OPEN SUBRECORD AND PULL OUT
	// a) PARENT DIGEST
	// b) ESR DIGEST
	// c) VERSION
	// d) For inmemory case memory usage
	//
	//
	// TODO: Needs Improvement.
	// Once subrecord tree is 128byte push all the info about parent and esr
	// and version in there ... and do not do IO. Potential excessive I/O.
	as_storage_rd rd;
	int rv                  = as_storage_record_open(ns, r, &rd, &r->key);
	cf_digest subrec_digest = rd.keyd;
	if (0 != rv) {
		cf_warning(AS_UDF, "LDT_SUB_GC Could not open record %"PRIx64"!! rv=%d", *(uint64_t *)&rd.keyd, rv);
		as_record_done(r_ref, ns);
		return;
	}
	rd.n_bins               = as_bin_get_n_bins(r, &rd);
	as_bin stack_bins[rd.n_bins];
	if ( ! ns->storage_data_in_memory && ! ns->single_bin ) {
		rd.n_bins = sizeof(stack_bins) / sizeof(as_bin);
	}
	rd.bins                 = as_bin_get_all(r, &rd, stack_bins);

	// Subrecord Version
	uint64_t subrec_version = 0;
	subrec_version          = as_ldt_subdigest_getversion(&subrec_digest);
	cf_detail(AS_LDT, "SUBRECVERSION OUT OF SUBREC_DIGEST %ld", subrec_version);

	// ESR digest with matching Version
	cf_digest esr_digest;
	if (as_ldt_subrec_storage_get_edigest(&rd, &esr_digest)) {
		goto Cleanup;
	}
	as_ldt_subdigest_setversion(&esr_digest, subrec_version);

	// Parent Version
	cf_digest parent_digest;
	if (as_ldt_subrec_storage_get_pdigest(&rd, &parent_digest)) {
		goto Cleanup;
	}

	uint64_t starting_memory_bytes = 0;
	if (ns->storage_data_in_memory) {
		starting_memory_bytes = as_storage_record_get_n_bytes_memory(&rd);
	}

	as_ldt_subrec_storage_validate(&rd, "Defragging");
	as_storage_record_close(r, &rd);

	// STEP 2: Check if we should delete subrec
	// a) Check if parent record exist (if not the record is deleted)
	// b) Check if the esr record exists (if not the bin is deleted)
	// c) Check if the version matches that of parent/esr ??? (if not the version is invalid)
	bool delete = false;
	char type   = 0;

	if (as_record_exists(p->sub_vp, &esr_digest, ns)) {
		delete = true;
		type   = 1;
	} else if (as_record_exists(p->vp, &parent_digest, ns)) {
		delete = true;
		type   = 2;
	} else if (!as_ldt_version_match(subrec_version, p->vp, &parent_digest, ns)) {
		delete = true;
		type   = 3;
		linfo->num_version_mismatch_gc++;
	} else {
		cf_detail(AS_LDT, "LDT_SUB_GC Found both parent and ESR record !!");
	}

	if (delete) {
		if (ns->storage_data_in_memory) {
			cf_atomic_int_sub(&p->n_bytes_memory, starting_memory_bytes);
		}
		cf_detail(AS_LDT, "LDT_SUB_GC Expiry of the SubRecord type=%d version=%ld partition state is %d:%d",
				type, subrec_version, p->rxstate, p->txstate);
		cf_info_digest(AS_LDT, &subrec_digest, "Sub-Rec Digest: ");
		cf_info_digest(AS_LDT, &esr_digest, "ESR Digest: ");
		cf_info_digest(AS_LDT, &parent_digest, "Parent Digest: ");
		as_index_delete(p->sub_vp, &subrec_digest);
		linfo->num_gc++;
	}
	as_record_done(r_ref, ns);

	// TODO: Have a better slow down strategy !!!
	usleep(100);
	return;

Cleanup:
	as_storage_record_close(r, &rd);
	as_record_done(r_ref, ns);
	usleep(100);
}


/*
 * Function to look at the parent and check if the migration component
 * is a merge candidate. Following checks are made
 * 1. Check generation and void time of the incoming subrec component
 *    with its parent generation and void time on sending node and the
 *    receiving node.
 *
 * Assumption: Caller should have proper partition reservation...
 *
 * Parameter:
 *          rsv:   Partition Reservation
 *          c:     Incoming component
 */
bool
as_ldt_merge_component_is_candidate(as_partition_reservation *rsv, as_record_merge_component *c)
{
	as_index_ref r_ref;
	r_ref.skip_lock     = false;
	if (as_record_get(rsv->tree, &c->pdigest, &r_ref, rsv->ns)) {
		return true;
	}
	as_index *r  = r_ref.r;

	// If component has higher generation ttl then it is merge candidate
	if (c->pgeneration > r->generation
			|| (c->pgeneration == r->generation && c->pvoid_time > r->void_time)) {
		as_record_done(&r_ref, rsv->ns);
		return true;
	} else {
		cf_debug_digest(AS_LDT,&r->key, "Local Parent Wins ... ignoring incoming:");
//		PRINTD(&r->key);
		as_record_done(&r_ref, rsv->ns);
		return false;
	}

#if 0
	// We have version as well want to do something smart ???
	as_storage_rd rd;
	as_storage_record_open(rsv->ns, r, &rd, keyd);
	uint64_t parent_version;
	int is_candidate = false;
	if (!as_ldt_parent_storage_get_version(&rd, &parent_version)) {
		cf_warning(AS_LDT, "Something wrong: could not get LDT parent version");
		as_storage_record_close(r, &rd);
		as_record_done(&r_ref, rsv->ns);
		return true;
	}
	if (parent_version == c->version) { /* do something */ }
#endif
}
