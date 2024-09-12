/******************************************************************************
* Copyright (c) 2024-present JC Wang. All rights reserved
*
*   https://crossdb.org
*   https://github.com/crossdb-org/crossdb
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
******************************************************************************/

#ifndef __CROSS_DB_H__
#define __CROSS_DB_H__

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include "../src/parser/xdb_token.h"
#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
	CrossDB Error
******************************************************************************/
	typedef enum cdf_op_t {
		CDF_OP_NONE,
		CDF_OP_EQ 	= XDB_TOK_EQ,
		CDF_OP_LT 	= XDB_TOK_LT, 	// <
		CDF_OP_LE 	= XDB_TOK_LE, 	// <=
		CDF_OP_GT 	= XDB_TOK_GT, 	// >
		CDF_OP_GE 	= XDB_TOK_GE, 	// >=
		CDF_OP_NE 	= XDB_TOK_NE, 	// != <>
		CDF_OPLOG_MAX	= CDF_OP_NE,
		CDF_OP_ADD 	= XDB_TOK_ADD,
		CDF_OP_SUB 	= XDB_TOK_SUB,
		CDF_OP_MUL 	= XDB_TOK_MUL,
		CDF_OP_DIV 	= XDB_TOK_DIV,
	} cdf_op_t;
typedef struct cdf_str_t {
	char *str;
	int len;
} cdf_str_t;

typedef struct cdf_filter_t {
	char *fidldName;
	xdb_token_type type;
	// int fLen;
	cdf_op_t op;
	cdf_str_t val;
} cdf_filter_t;
typedef enum {
	XDB_OK,
	XDB_ERROR,
	XDB_E_PARAM,
	XDB_E_STMT,
	XDB_E_NODB,
	XDB_E_NOTFOUND,
	XDB_E_EXISTS,
	XDB_E_FULL,
	XDB_E_CONSTRAINT,
	XDB_E_AUTH,
	XDB_E_MEMORY,
	XDB_E_FILE,
	XDB_E_SOCK,
} xdb_errno_e;

// return - value of xdb_errno_e
typedef int xdb_ret;


/******************************************************************************
	CrossDB Data Types
******************************************************************************/

typedef enum {
	XDB_TYPE_NULL       = 0,  // 1 bit
	XDB_TYPE_TINYINT	= 1,  // 1 byte
	XDB_TYPE_SMALLINT   = 2,  // 2 bytes
	XDB_TYPE_INT        = 3,  // 4 bytes
	XDB_TYPE_BIGINT     = 4,  // 8 bytes
	XDB_TYPE_UTINYINT   = 5,  // 1 byte
	XDB_TYPE_USMALLINT  = 6,  // 2 bytes
	XDB_TYPE_UINT       = 7,  // 4 bytes
	XDB_TYPE_UBIGINT    = 8,  // 8 bytes
	XDB_TYPE_FLOAT      = 9,  // 4 bytes
	XDB_TYPE_DOUBLE     = 10, // 8 bytes
	XDB_TYPE_TIMESTAMP	= 11, // 8 bytes
	XDB_TYPE_CHAR       = 12, // fixed-length string(at most 65535 byte)
	XDB_TYPE_BINARY     = 13, // fixed-length binary(at most 65535 byte)
	XDB_TYPE_VCHAR      = 14, // varied-length string(at most 65535 byte)
	XDB_TYPE_VBINARY    = 15, // varied-length binary(at most 65535 byte)
	// MAC,IPv4,IPv6,CIDR
	XDB_TYPE_DECIMAL    = 16, // TBD decimal
	//XDB_TYPE_GEOMETRY   = 17, // TBD geometry
	//XDB_TYPE_JSON 	    = 18, // TBD json string
	//XDB_TYPE_DYNAMIC	= 20, 
	XDB_TYPE_MAX        = 21
} xdb_type_t;


/******************************************************************************
	CrossDB Reuslt
******************************************************************************/

typedef enum {
	XDB_RET_ROW = 0,
	XDB_RET_REPLY,
	XDB_RET_META,
	XDB_RET_MSG,
	XDB_RET_COMPRESS,
	XDB_RET_INSERT,	// (meta + row)
	XDB_RET_DELETE, // (meta + row)
	XDB_RET_UPDATE, // (old meta, old row, set meta, set row)
	XDB_RET_EOF = 0xF,
} xdb_restype_t;

typedef enum {
	XDB_STATUS_MORE_RESULTS 	= (1<<3),
} xdb_status_t;

typedef uint64_t xdb_row_t;	// xdb_rowdat_t

#define XDB_ROW_COL_CNT		4096

typedef struct {
	uint32_t	rl_count;
	uint32_t	rl_curid;
	xdb_row_t 	rl_pRows[XDB_ROW_COL_CNT];
} xdb_rowlist_t;

typedef struct {
	uint32_t	len_type;		// MSB 4bit are type xdb_restype_t
	uint16_t	errcode;		// 4
	uint16_t	status;			// 6 xdb_status_t

	uint32_t	meta_len;		// 8
	uint16_t	col_count;		// 12
	uint8_t		stmt_type;		// 14 SQL type(create/delete/drop/show/select/delete/update...)
	uint8_t		rsvd;

	uint64_t	row_count;		// 2*8 SELECT/SHOW
	uint64_t	affected_rows;	// 3*8 INSERT/UPDATE/DELETE
	uint64_t	insert_id;		// 4*8 INSERT
	uint64_t	col_meta;		// 5*8 xdb_meta_t, <ptr:ptr off: 0 following is meta>
	uint64_t	row_data;		// 6*8 xdb_rowlist_t, ptr: base ptr or error str or information xdb_msg_t
	uint64_t	data_len;		// 7*8
} xdb_res_t;

typedef struct {
	uint32_t	len_type;		// LSB 4bit are type
	uint16_t	len;
	char		msg[2048];
} xdb_msg_t;

typedef struct {
	uint32_t	len_type;		// LSB 4bit are type
	uint8_t		rowdat[];
} xdb_rowdat_t;

typedef struct {
	uint16_t	col_len;		// colum total len
	uint8_t		col_type;		// xdb_type_t
	uint8_t		col_rsvd;
	uint32_t	col_off;
	uint16_t	col_rsvd2;
	uint8_t		col_nmlen;
	char		col_name[];
} xdb_col_t;

typedef struct {
	uint32_t	len_type;		// LSB 4bit are type
	uint16_t	col_count;		// 3*4
	uint16_t	null_off;		// 3*4+2
	uint16_t	row_size;
	uint16_t	rsvd;
	uint64_t	col_list;		// xdb_col_t list
	xdb_col_t	cols[];
} xdb_meta_t;

typedef struct xdb_conn_t xdb_conn_t;

typedef struct xdb_stmt_t xdb_stmt_t;


/**************************************
 Connection
***************************************/

xdb_conn_t*
xdb_open (const char *path);

xdb_conn_t*
xdb_connect (const char *host, const char *user, const char *pass, const char *db, uint16_t port);

void
xdb_close (xdb_conn_t *pConn);

const char*
xdb_curdb (xdb_conn_t* pConn);


/**************************************
 SQL
***************************************/
// 	xdb_res_t *
// 	xdb_createdb(xdb_conn_t *pConn, const char *dbName);
//
// 	xdb_res_t *
// cdf_create_table(xdb_conn_t *pConn, ...);

xdb_res_t *
cdf_insert_data(xdb_conn_t *pConn, const char *tblName, int count, void **dataArr);

xdb_res_t *
cdf_exec_by_stmt(xdb_stmt_t *stmt);

xdb_res_t *
cdf_select_data(xdb_conn_t *pConn, const char *tblName, int count, void **dataArr);

xdb_res_t *
cdf_delete_data(xdb_conn_t *pConn, const char *tblName, int count, void **dataArr);

xdb_res_t*
xdb_exec (xdb_conn_t *pConn, const char *sql);

xdb_res_t*
xdb_exec2 (xdb_conn_t *pConn, const char *sql, int len);

xdb_res_t*
xdb_bexec (xdb_conn_t *pConn, const char *sql, ...);

xdb_res_t*
xdb_vbexec (xdb_conn_t *pConn, const char *sql, va_list ap);

xdb_res_t*
xdb_pexec (xdb_conn_t *pConn, const char *sql, ...);

xdb_res_t*
xdb_next_result (xdb_conn_t *pConn);

bool
xdb_more_result (xdb_conn_t *pConn);

void
xdb_free_result (xdb_res_t *pRes);


/**************************************
 Result
***************************************/

static inline xdb_col_t* 
xdb_column_meta (uint64_t meta, uint16_t iCol)
{
	return (xdb_col_t*)((((uint64_t*)((xdb_meta_t*)meta)->col_list))[iCol]);
}

xdb_row_t*
xdb_fetch_row (xdb_res_t *pRes);

int 
xdb_column_int (uint64_t meta, xdb_row_t *pRow, uint16_t iCol);

int64_t 
xdb_column_int64 (uint64_t meta, xdb_row_t *pRow, uint16_t iCol);

float
xdb_column_float (uint64_t meta, xdb_row_t *pRow, uint16_t iCol);

double
xdb_column_double (uint64_t meta, xdb_row_t *pRow, uint16_t iCol);

const char*
xdb_column_str (uint64_t meta, xdb_row_t *pRow, uint16_t iCol);

const char*
xdb_column_str2 (uint64_t meta, xdb_row_t *pRow, uint16_t iCol, int *pLen);


/**************************************
 Prepared Statment
***************************************/

xdb_stmt_t*
xdb_stmt_prepare (xdb_conn_t *pConn, const char *sql);

xdb_ret
xdb_bind_int (xdb_stmt_t *pStmt, uint16_t para_id, int val);

xdb_ret
xdb_bind_int64 (xdb_stmt_t *pStmt, uint16_t para_id, int64_t val);

xdb_ret
xdb_bind_float (xdb_stmt_t *pStmt, uint16_t para_id, float val);

xdb_ret
xdb_bind_double (xdb_stmt_t *pStmt, uint16_t para_id, double val);

xdb_ret
xdb_bind_str (xdb_stmt_t *pStmt, uint16_t para_id, const char *str);

xdb_ret
xdb_bind_str2 (xdb_stmt_t *pStmt, uint16_t para_id, const char *str, int len);

xdb_ret
xdb_clear_bindings (xdb_stmt_t *pStmt);

xdb_res_t*
xdb_stmt_exec (xdb_stmt_t *pStmt);

xdb_res_t*
xdb_stmt_bexec (xdb_stmt_t *pStmt, ...);

xdb_res_t*
xdb_stmt_vbexec (xdb_stmt_t *pStmt, va_list ap);

void
xdb_stmt_close (xdb_stmt_t *pStmt);


/**************************************
 Transaction
***************************************/

xdb_ret
xdb_begin (xdb_conn_t* pConn);

xdb_ret
xdb_commit (xdb_conn_t* pConn);

xdb_ret
xdb_rollback (xdb_conn_t* pConn);


/**************************************
 Misc
***************************************/

#if defined(__GNUC__) || defined(__INTEL_COMPILER) || defined(__clang__)
#define xdb_likely(x)			__builtin_expect(x, 1)
#define xdb_unlikely(x)			__builtin_expect(x, 0)
#else
#define xdb_likely(x)
#define xdb_unlikely(x)
#endif

#define XDB_CHECK(expr, action...)	if (xdb_unlikely(!(expr))) { action; }
#define XDB_RESCHK(pRes, action...)	if (xdb_unlikely(pRes->errcode)) { fprintf (stderr, "==== ERROR %d: %s\n", pRes->errcode, xdb_errmsg(pRes)); action; }

const char*
xdb_errmsg (xdb_res_t *pRes);

int 
xdb_print_row (uint64_t meta, xdb_row_t *pRow, int format);

const char*
xdb_version();

#ifdef __cplusplus
}
#endif

#endif // __CROSS_DB_H__
