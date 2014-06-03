/*
 * stream.h
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
/*
 * This file implements stream parsing for rows
 */

#pragma once

#include "ai_obj.h"
#include "bt.h"

void *row_malloc(bt *ibtr, int size);

// LUATBL
void pushLuaVar(int tmatch, icol_t ic, ai_obj *apk);
bool setLuaVar (int tmatch, icol_t ic, ai_obj *apk);

// LRU
uchar  getLruSflag();
int    cLRUcol(ulong l, uchar *sflag, ulong *col);
uint32 streamLRUToUInt(uchar *data);
void   overwriteLRUcol(uchar *row,    ulong icol);

// LFU
uchar getLfuSflag();
int   cLFUcol(ulong l, uchar *sflag, ulong *col);
void  overwriteLFUcol(uchar *row, ulong icol);
ulong streamLFUToULong(uchar *data);

// NORMAL
int     getCSize(ulong l,   bool isi);
int     cIcol   (ulong l,   uchar *sflag, ulong   *col, bool isi);
int     cr8Icol (ulong l,   uchar *sflag, ulong   *col);
int     cr8Lcol (ulong l,   uchar *sflag, ulong   *col);
int     cr8Xcol (uint128 x,               uint128 *col);
int     cr8Ycol (uint160 y,               uint160 *col);

void    writeUIntCol (uchar **row, uchar sflag, ulong icol);
void    writeULongCol(uchar **row, uchar sflag, ulong icol);
void    writeU128Col (uchar **row, uint128 xcol);
void    writeU160Col (uchar **row, uint160 ycol);
void    writeFloatCol(uchar **row, bool  fflag, float fcol);
uint32  streamIntToUInt   (uchar *data, uint32 *clen);
ulong   streamLongToULong (uchar *data, uint32 *clen);
uint128 streamToU128(uchar *data, uint32 *clen);
uint160 streamToU160(uchar *data, uint32 *clen);
float   streamFloatToFloat(uchar *data, uint32 *clen);


uint32 getStreamMallocSize(bt *ibtr, uchar *stream);
uint32 getStreamRowSize   (bt *ibtr, uchar *stream);

/* INODE_I/L,UU,UL,LU,LL cmp */
int uintCmp (void *s1, void *s2);
int ulongCmp(void *s1, void *s2);
int u128Cmp (void *s1, void *s2);
int u160Cmp (void *s1, void *s2);

int uuCmp   (void *s1, void *s2);
int ulCmp   (void *s1, void *s2);
int uxCmp   (void *s1, void *s2);
int uyCmp   (void *s1, void *s2);
int luCmp   (void *s1, void *s2);
int llCmp   (void *s1, void *s2);
int lxCmp   (void *s1, void *s2);
int lyCmp   (void *s1, void *s2);
int xuCmp   (void *s1, void *s2);
int xlCmp   (void *s1, void *s2);
int xxCmp   (void *s1, void *s2);
int xyCmp   (void *s1, void *s2);
int yuCmp   (void *s1, void *s2);
int ylCmp   (void *s1, void *s2);
int yxCmp   (void *s1, void *s2);
int yyCmp   (void *s1, void *s2);

/* BT DATA cmp */
int btIntCmp  (void *a, void *b);
int btLongCmp (void *a, void *b);
int btU128Cmp (void *a, void *b);
int btU160Cmp (void *a, void *b);
int btFloatCmp(void *a, void *b);
int btTextCmp (void *a, void *b);

char *createBTKey(ai_obj *key, bool *med, uint32 *ksize, bt *btr, btk_t *btk);
void  destroyBTKey(char *btkey, bool  med);

void   convertStream2Key(uchar *stream, ai_obj *key, bt *btr);
uchar *parseStream(uchar *stream, bt *btr);

void *createStream(bt *btr, void *val, char *btkey,
				   uint32 klen, uint32 *ssize, crs_t *crs);
bool  destroyStream(bt *btr, uchar *ostream);
