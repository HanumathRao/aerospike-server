/*
 * packet_compression.h
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

#pragma once

#include <stdint.h>

/*
 * Function to decompress the given data
 * Expected arguments
 * 1. Type of compression
 * 	1 for zlib
 * 2. Length of buffer to be decompressed - mandatory
 * 3. Pointer to buffer to be decompressed - mandatory
 * 4. Length of buffer to hold decompressed data - mandatory
 * 5. Pointer to buffer to hold decompressed data - mandatory
 */
int
as_decompress(int argc, uint8_t **argv);

/*
 * Function to get back decompressed packet from PROTO_TYPE_AS_MSG_COMPRESSED packet
 * Packet :  Header - Original size of message - Compressed message
 * Input : buf - Pointer to PROTO_TYPE_AS_MSG_COMPRESSED packet. - Input
 *         decompressed_packet - Pointer holding address of decompressed packet. - Output
 */
int
as_packet_decompression(uint8_t *buf, uint8_t *decompressed_packet);

/*
 * Function to compress the given data
 * Expected arguments
 * 1. Type of compression
 *  1 for zlib
 * 2. Length of buffer to be compressed - mandatory
 * 3. Pointer to buffer to be compressed - mandatory
 * 4. Length of buffer to hold compressed data - mandatory
 * 5. Pointer to buffer to hold compressed data - mandatory
 * 6. Compression level - Optional, default Z_DEFAULT_COMPRESSION
 *                                          Z_NO_COMPRESSION         0
 *                                          Z_BEST_SPEED             1
 *                                          Z_BEST_COMPRESSION       9
 *                                          Z_DEFAULT_COMPRESSION  (-1)
 */
int
as_compress(int argc, uint8_t *argv[]);

/*
 * Function to create packet to send compressed data.
 * Packet :  Header - Original size of message - Compressed message.
 * Input : buf - Pointer to data to be compressed. - Input
 *     buf_sz - Size of the data to be compressed. - Input
 *     compressed_packet : Pointer holding address of compressed packet. - Output
 *     compressed_packet_sz : Size of the compressed packet. - Output
 */
int
as_packet_compression(uint8_t *buf, size_t buf_sz, uint8_t **compressed_packet, size_t *compressed_packet_sz);
