//
// Created by Administrator on 2024/9/10.
//

#include "crossdb.h"
#include "cdf_metadata.h"


// XDB_STATIC xdb_stmt_t *
// cdf_create_table_stmt(xdb_conn_t *pConn,...) {
//
//     xdb_stmt_tbl_t *pStmt = &pConn->stmt_union.tbl_stmt;
//     pStmt->stmt_type = XDB_STMT_CREATE_TBL;
//     pStmt->pSql = NULL;
//
//     //处理字段
//     va_list ap;
//     va_start(ap, pConn);
//     int len = va_arg(ap, int);
//     for (int i = 0; i < len; ++i) {
//         column_metadata curr = va_arg(ap, column_metadata);
//
//     }
//     return NULL;
// }

XDB_STATIC xdb_stmt_t *
cdf_insert_stmt(xdb_conn_t *pConn, bool bPStmt, char *tableName, int count, void **dataArr) {
    xdb_stmt_insert_t *pStmt;
    if (xdb_unlikely(bPStmt)) {
        pStmt = xdb_malloc(sizeof (*pStmt));
        // XDB_EXPECT(NULL != pStmt, XDB_E_MEMORY, "Run out of memory");
    } else {
        pStmt = &pConn->stmt_union.insert_stmt;
    }

    pStmt->stmt_type = XDB_STMT_INSERT;
    pStmt->pSql = NULL;
    pStmt->pRowsBuf = NULL;
    xdb_token_t pTknObj = {};
    pTknObj.tk_type = XDB_TOK_ID;
    pTknObj.token = tableName;
    xdb_token_t *pTkn = &pTknObj;
    xdb_token_type type = XDB_TOK_ID;
    XDB_PARSE_DBTBLNAME();
    xdb_tblm_t *pTblm = pStmt->pTblm;

    pStmt->fld_count = 0;
    pStmt->row_count = 0;
    pStmt->bind_count = 0;

    bool bColList = false;

    pStmt->fld_count = pTblm->fld_count;
    // pStmt->tbl_name = tableName;
    if (pTblm->row_size < sizeof (pStmt->row_buf)) {
        pStmt->pRowsBuf = pStmt->row_buf;
        pStmt->buf_len = sizeof (pStmt->row_buf);
    } else {
        pStmt->buf_len = 4 * pTblm->row_size;
        pStmt->pRowsBuf = xdb_malloc(pStmt->buf_len);
    }

    uint32_t offset = 0;

    //解析数据

    for (int i = 0; i < count; ++i) {
        void *currRow = dataArr[i];

        void *pRow = pStmt->pRowsBuf + offset;

        if (xdb_unlikely(offset + pTblm->row_size > pStmt->buf_len)) {
            uint32_t buf_len = pStmt->buf_len <<= 1;
            void *pRowsBuf;
            if (pStmt->pRowsBuf == pStmt->row_buf) {
                pRowsBuf = xdb_malloc(buf_len);
                if (NULL != pRowsBuf) {
                    memcpy(pRowsBuf, pStmt->row_buf, sizeof (pStmt->row_buf));
                }
            } else {
                pRowsBuf = xdb_realloc(pStmt->pRowsBuf, buf_len);
            }
            pStmt->pRowsBuf = pRowsBuf;
            pStmt->buf_len = buf_len;
        }

        // TBD copy from default?
        memset(pRow, 0, pTblm->row_size);
        pStmt->row_offset[pStmt->row_count++] = offset;
        // memcpy(pRow, currRow, pTblm->row_size);

        int fld_seq = 0;
        // for (int j = 0; j < pStmt->fld_count; ++j) {
        //     type = XDB_TOK_NUM;
        //     int fld_id = bColList ? pStmt->fld_list[fld_seq] : fld_seq;
        //     XDB_EXPECT(++fld_seq <= pStmt->fld_count, XDB_E_STMT, "Too many values");
        //     xdb_field_t *pFld = &pTblm->pFields[fld_id];
        //
        //     if (type <= XDB_TOK_NUM) {
        //         switch (pFld->fld_type) {
        //             case XDB_TYPE_INT:
        //                 // XDB_EXPECT((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
        //                 // *(int32_t *) (pRow + pFld->fld_off) = *(int32_t*)(currRow + pFld->fld_off);
        //                 *(int32_t *) (pRow + pFld->fld_off) = *(int32_t*)(currRow + pFld->fld_off);
        //             //xdb_dbgprint ("%s %d\n", pFld->fld_name, vi32);
        //                 break;
        //             case XDB_TYPE_BIGINT:
        //                 // XDB_EXPECT((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
        //                 *(int64_t *) (pRow + pFld->fld_off) =  *(int64_t*)(currRow + pFld->fld_off);
        //             //xdb_dbgprint ("%s %d\n", pFld->fld_name, vi32);
        //                 break;
        //             case XDB_TYPE_TINYINT:
        //                 // XDB_EXPECT((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
        //                 *(int8_t *) (pRow + pFld->fld_off) =  *(int8_t *) (currRow + pFld->fld_off);
        //             //xdb_dbgprint ("%s %d\n", pFld->fld_name, vi32);
        //                 break;
        //             case XDB_TYPE_SMALLINT:
        //                 // XDB_EXPECT((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
        //                 *(int16_t *) (pRow + pFld->fld_off) = *(int16_t *) (currRow + pFld->fld_off);
        //             //xdb_dbgprint ("%s %d\n", pFld->fld_name, vi32);
        //                 break;
        //             case XDB_TYPE_FLOAT:
        //                 // XDB_EXPECT((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
        //                 *(float *) (pRow + pFld->fld_off) = *(float *) (currRow + pFld->fld_off);
        //             //xdb_dbgprint ("%s %d\n", pFld->fld_name, vi32);
        //                 break;
        //             case XDB_TYPE_DOUBLE:
        //                 // XDB_EXPECT((XDB_TOK_NUM == type), XDB_E_STMT, "Expect number");
        //                 *(double *) (pRow + pFld->fld_off) =  *(double *) (currRow + pFld->fld_off);
        //             //xdb_dbgprint ("%s %d\n", pFld->fld_name, vi32);
        //                 break;
        //             case XDB_TYPE_CHAR:
        //                 // XDB_EXPECT((XDB_TOK_STR == type) && (pTkn->tk_len <= pFld->fld_len), XDB_E_STMT,
        //                 //            "Expect string <= %d", pFld->fld_len);
        //                 // *(uint16_t *) (pRow + pFld->fld_off - 2) = pTkn->tk_len;
        //                 memcpy(pRow + pFld->fld_off, currRow + pFld->fld_off, pFld->fld_len);
        //             //xdb_dbgprint ("%s %s\n", pFld->fld_name, pTkn->token);
        //                 break;
        //             default: break;
        //         }
        //
        //     } else if (XDB_TOK_QM == type) {
        //         pStmt->pBindRow[pStmt->bind_count] = pRow;
        //         pStmt->pBind[pStmt->bind_count++] = pFld;
        //     } else {
        //         break;
        //     }
        //
        //
        // }

        memcpy(pRow, currRow, pTblm->row_size);
        offset += pTblm->row_size;
    }
    return (xdb_stmt_t *) pStmt;

error:
    xdb_stmt_free((xdb_stmt_t *) pStmt);
    return NULL;
}


XDB_STATIC xdb_stmt_t *
cdf_stmt_common_create(xdb_conn_t *pConn, cdf_stmt_type type, va_list ap) {
    switch (type) {
        case INSERT_ROW: {
            char *tab = va_arg(ap, char*);
            int count = va_arg(ap, int);
            void **arr = va_arg(ap, void**);
            xdb_stmt_t *res = cdf_insert_stmt(pConn, false, tab, count, arr);
            return res;
        }

        case UPDATE_ROW:
        case DELETE_ROW:
        case SELECT_ROW:
            return NULL;
        default: return NULL;
    }
    return NULL;
}
