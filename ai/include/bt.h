/*
 * bt.h
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
 * Creation of different btree types and
 * Public Btree Operations w/ stream abstractions under the covers
 */

#pragma once

#include "ai_obj.h"
#include "btreepriv.h"

bt *abt_resize(bt *obtr, uchar trans);

bt *createU_S_IBT  (uchar ktype, int imatch, uchar pktyp);
bt *createU_MCI_IBT(uchar ktype, int imatch, uchar pktyp);
bt *createMCI_MIDBT(uchar ktype, int imatch);
bt *createIndexBT  (uchar ktype, int imatch);
bt *createDBT      (uchar ktype, int tmatch);
bt *createIndexNode(uchar pktyp, uchar obctype);

/* different Btree types */
#define BTREE_TABLE    0
#define BTREE_INDEX    1
#define BTREE_INODE    2
#define BTREE_MCI      3
#define BTREE_MCI_MID  4
#define BT_MCI_UNIQ    5
#define BT_SIMP_UNIQ   6

// SPAN OUTS
// This values are choosen to fit the node size into multiples
// of cacheline (64 byte)
#define BTREE_LONG_TYPE_DEGREE    31 // node size becomes 504
#define BTREE_STRING_TYPE_DEGREE  18 // node size becomes 512

/* INT Inodes have been optimised */
#define INODE_I(btr) \
  (btr->s.btype == BTREE_INODE && C_IS_I(btr->s.ktype) && \
   !(btr->s.bflag & BTFLAG_OBC))
/* LONG Inodes have been optimised */
#define INODE_L(btr) \
  (btr->s.btype == BTREE_INODE && C_IS_L(btr->s.ktype) && \
   !(btr->s.bflag & BTFLAG_OBC))
/* U128 Inodes have been optimised */
#define INODE_X(btr) \
  (btr->s.btype == BTREE_INODE && C_IS_X(btr->s.ktype) && \
   !(btr->s.bflag & BTFLAG_OBC))
/* U160 Inodes have been optimised */
#define INODE_Y(btr) \
  (btr->s.btype == BTREE_INODE && C_IS_Y(btr->s.ktype) && \
   !(btr->s.bflag & BTFLAG_OBC))
#define INODE(btr) (INODE_I(btr) || INODE_L(btr) || \
                    INODE_X(btr) || INODE_Y(btr))

#define SIMP_UNIQ(btr) (btr->s.btype == BT_SIMP_UNIQ)
#define MCI_UNIQ(btr)  (btr->s.btype == BT_MCI_UNIQ)

#define OBYI(btr) (btr->s.bflag & BTFLAG_OBC)

/* UU tables containing ONLY [PK=INT,col1=INT]  have been optimised */
#define UU(btr) (btr->s.bflag & BTFLAG_UINT_UINT)
#define UU_SIZE 8
typedef struct uint_ulong_key {
	uint32 key;
	ulong  val;
}  __attribute__ ((packed)) ulk;
#define UL(btr) (btr->s.bflag & BTFLAG_UINT_ULONG)
#define UL_SIZE 12
typedef struct uint_u128_key {
	uint32  key;
	uint128 val;
}  __attribute__ ((packed)) uxk;
#define UX(btr) (btr->s.bflag & BTFLAG_UINT_U128)
#define UX_SIZE 20
typedef struct uint_u160_key {
	uint32  key;
	uint160 val;
}  __attribute__ ((packed)) uyk;
#define UY(btr) (btr->s.bflag & BTFLAG_UINT_U160)
#define UY_SIZE 24

/* LU tables containing ONLY [PK=LONG,col1=INT] have been optimised */
typedef struct ulong_uint_key {
	ulong  key;
	uint32 val;
}  __attribute__ ((packed)) luk;
#define LU(btr) (btr->s.bflag & BTFLAG_ULONG_UINT)
#define LU_SIZE 12
typedef struct ulong_ulong_key {
	ulong key;
	ulong val;
}  __attribute__ ((packed)) llk;
#define LL(btr) (btr->s.bflag & BTFLAG_ULONG_ULONG)
#define LL_SIZE 16
typedef struct ulong_u128_key {
	ulong   key;
	uint128 val;
}  __attribute__ ((packed)) lxk;
#define LX(btr) (btr->s.bflag & BTFLAG_ULONG_U128)
#define LX_SIZE 24
typedef struct ulong_u160_key {
	ulong   key;
	uint160 val;
}  __attribute__ ((packed)) lyk;
#define LY(btr) (btr->s.bflag & BTFLAG_ULONG_U160)
#define LY_SIZE 28

/* XU tables containing ONLY [PK=U128,col1=INT] have been optimised */
typedef struct u128_uint_key {
	uint128 key;
	uint32  val;
}  __attribute__ ((packed)) xuk;
#define XU(btr) (btr->s.bflag & BTFLAG_U128_UINT)
#define XU_SIZE 20
typedef struct u128_ulong_key {
	uint128 key;
	ulong   val;
}  __attribute__ ((packed)) xlk;
#define XL(btr) (btr->s.bflag & BTFLAG_U128_ULONG)
#define XL_SIZE 24
typedef struct u128_u128_key {
	uint128 key;
	uint128 val;
}  __attribute__ ((packed)) xxk;
#define XX(btr) (btr->s.bflag & BTFLAG_U128_U128)
#define XX_SIZE 32
typedef struct u128_u160_key {
	uint128 key;
	uint160 val;
}  __attribute__ ((packed)) xyk;
#define XY(btr) (btr->s.bflag & BTFLAG_U128_U160)
#define XY_SIZE 36

/* YU tables containing ONLY [PK=U160,col1=INT] have been optimised */
typedef struct u160_uint_key {
	uint160 key;
	uint32  val;
}  __attribute__ ((packed)) yuk;
#define YU(btr) (btr->s.bflag & BTFLAG_U160_UINT)
#define YU_SIZE 24
typedef struct u160_ulong_key {
	uint160 key;
	ulong   val;
}  __attribute__ ((packed)) ylk;
#define YL(btr) (btr->s.bflag & BTFLAG_U160_ULONG)
#define YL_SIZE 28
typedef struct u160_u128_key {
	uint160 key;
	uint128 val;
}  __attribute__ ((packed)) yxk;
#define YX(btr) (btr->s.bflag & BTFLAG_U160_U128)
#define YX_SIZE 36
typedef struct u160_u160_key {
	uint160 key;
	uint160 val;
}  __attribute__ ((packed)) yyk;
#define YY(btr) (btr->s.bflag & BTFLAG_U160_U160)
#define YY_SIZE 40

typedef struct crg_t {
	ulk UL;
	uxk UX;
	uyk UY;
	luk LU;
	llk LL;
	lxk LX;
	lyk LY;
	xuk XU;
	xlk XL;
	xxk XX;
	xyk XY;
	yuk YU;
	ylk YL;
	yxk YX;
	yyk YY;
} crg_t;

#define BTK_BSIZE 256
typedef struct btk_t {
	uchar btkeybuffer[BTK_BSIZE];
	ulk UL;
	uxk UX;
	uxk UY;
	luk LU;
	llk LL;
	lxk LX;
	lxk LY;
	xuk XU;
	xlk XL;
	xxk XX;
	xxk XY;
	yuk YU;
	ylk YL;
	yxk YX;
	yxk YY;
} btk_t;

#define DECLARE_BT_KEY(akey, ret)                                            \
    bool  med; uint32 ksize; btk_t btk;                                      \
    char *btkey = createBTKey(akey, &med, &ksize, btr, &btk);/*FREE ME 026*/ \
    if (!btkey) return ret;

typedef struct crs_t {
	ulk UL_StreamPtr;
	uxk UX_StreamPtr;
	uyk UY_StreamPtr;
	luk LU_StreamPtr;
	llk LL_StreamPtr;
	lxk LX_StreamPtr;
	lyk LY_StreamPtr;
	xuk XU_StreamPtr;
	xlk XL_StreamPtr;
	xxk XX_StreamPtr;
	xyk XY_StreamPtr;
	yuk YU_StreamPtr;
	ylk YL_StreamPtr;
	yxk YX_StreamPtr;
	yyk YY_StreamPtr;
} crs_t;

/* Indexes containing INTs AND LONGs have been optimised */
#define UP(btr)  (btr->s.bflag & BTFLAG_UINT_INDEX)

//TODO LUP, XUP, XXP possibly not used
//     are LYP, XYP needed?
#define LUP(btr) (btr->s.bflag & BTFLAG_ULONG_INDEX && \
                  btr->s.bflag & BTFLAG_ULONG_UINT)
#define LLP(btr) (btr->s.bflag & BTFLAG_ULONG_INDEX && \
                  btr->s.bflag & BTFLAG_ULONG_ULONG)

#define XUP(btr) (btr->s.bflag & BTFLAG_UINT_INDEX && \
                  btr->s.bflag & BTFLAG_U128_UINT)
#define XLP(btr) (btr->s.bflag & BTFLAG_ULONG_INDEX && \
                  btr->s.bflag & BTFLAG_U128_ULONG)
#define XXP(btr) (btr->s.bflag & BTFLAG_U128_INDEX && \
                  btr->s.bflag & BTFLAG_U128_ULONG)

#define YLP(btr) (btr->s.bflag & BTFLAG_ULONG_INDEX && \
                  btr->s.bflag & BTFLAG_U160_ULONG)
#define YYP(btr) (btr->s.bflag & BTFLAG_U160_INDEX && \
                  btr->s.bflag & BTFLAG_U160_ULONG)

#define UKEY(btr) (UU(btr) || LU(btr) || XU(btr) || YU(btr))
#define LKEY(btr) (UL(btr) || LL(btr) || XL(btr) || YL(btr))
#define XKEY(btr) (UX(btr) || LX(btr) || XX(btr) || YX(btr))
#define YKEY(btr) (UY(btr) || LY(btr) || XY(btr) || YY(btr))

/* NOTE OTHER_BT covers *P as they are [UL,LL,XL] respectively */
#define OTHER_BT(btr) (btr->s.bflag >= BTFLAG_UINT_UINT)
/* NOTE: BIG_BT means the KEYS are bigger than 8 bytes */
#define BIG_BT(btr)   (btr->s.ksize > 8)
#define NORM_BT(btr)  (btr->s.bflag == BTFLAG_NONE)

#define IS_GHOST(btr, rrow) (NORM_BT(btr) && rrow && !(*(uchar *)rrow))

int    btAdd    (bt *btr, ai_obj *apk, void *val);
void  *btFind   (bt *btr, ai_obj *apk);
dwm_t  btFindD  (bt *btr, ai_obj *apk);
int    btReplace(bt *btr, ai_obj *apk, void *val);
int    btDelete (bt *btr, ai_obj *apk);
bool   btEvict  (bt *btr, ai_obj *apk);

void  btIndAdd   (bt *ibtr, ai_obj *ikey, bt  *nbtr);
bt   *btIndFind  (bt *ibtr, ai_obj *ikey);
bool  btIndExist (bt *ibtr, ai_obj *ikey);
int   btIndDelete(bt *ibtr, ai_obj *ikey);
int   btIndDeleteIfLeaf(bt *ibtr, ai_obj *ikey);
int   btIndNull  (bt *ibtr, ai_obj *ikey);

bool  btIndNodeAdd    (bt *nbtr, ai_obj *apk);
bool  btIndNodeExist  (        bt *nbtr, ai_obj *apk);
int   btIndNodeDelete (        bt *nbtr, ai_obj *apk, ai_obj *ocol);
int   btIndNodeDeleteIfLeaf(   bt *nbtr, ai_obj *apk, ai_obj *ocol);
void  btIndNodeDeleteD(        bt *nbtr, ai_obj *apk, ai_obj *ocol);
int   btIndNodeEvict  (        bt *nbtr, ai_obj *apk, ai_obj *ocol);

// HELPER HELPER HELPER HELPER HELPER HELPER HELPER HELPER HELPER HELPER
uint32  btGetDR    (bt *btr, ai_obj *akey);
ai_obj *btGetNext  (bt *btr, ai_obj *akey);
bool    btDecrDR_PK(bt *btr, ai_obj *akey, uint32 by);

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
//#define TEST_WITH_TRANS_ONE_ONLY
