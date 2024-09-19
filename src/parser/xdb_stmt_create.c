//
// Created by Administrator on 2024/9/10.
//

#include "crossdb.h"
//#include "cdf_metadata.h"


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
        pStmt = xdb_malloc(sizeof(*pStmt));
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
    if (pTblm->row_size < sizeof(pStmt->row_buf)) {
        pStmt->pRowsBuf = pStmt->row_buf;
        pStmt->buf_len = sizeof(pStmt->row_buf);
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
                    memcpy(pRowsBuf, pStmt->row_buf, sizeof(pStmt->row_buf));
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

        memcpy(pRow, currRow, pTblm->row_size);
        offset += pTblm->row_size;
    }
    return (xdb_stmt_t *) pStmt;

    error:
    xdb_stmt_free((xdb_stmt_t *) pStmt);
    return NULL;
}

XDB_STATIC int
cdf_parse_where(xdb_conn_t *pConn, xdb_stmt_select_t *pStmt, int count, cdf_filter_t **cdf_filter_arr) {
    uint8_t bmp[8];
    xdb_tblm_t *pTblm = pStmt->pTblm;
    memset(bmp, 0, sizeof(bmp));
    xdb_token_type type;
    xdb_token_type vtype;
    int vlen, flen;
    xdb_op_t op;
    char *pVal, *pFldName;

    //处理每个字段
    for (int i = 0; i < count; ++i) {
        cdf_filter_t *currFilter = cdf_filter_arr[i];
        pFldName = currFilter->fidldName;
        flen = strlen(pFldName);
        op = (xdb_op_t) currFilter->op;
        vtype = currFilter->type;
        pVal = currFilter->val.str;
        vlen = currFilter->val.len;

        int fld_id = xdb_find_field(pStmt->pTblm, pFldName, flen);
        xdb_filter_t *pFilter = &pStmt->filters[pStmt->filter_count];
        pStmt->pFilters[pStmt->filter_count++] = pFilter;
        //pFilter->fld_off	= pField->fld_off;
        //pFilter->fld_type	= pField->fld_type;
        xdb_field_t *pField = &pStmt->pTblm->pFields[fld_id];
        pFilter->pField = pField;

        if (xdb_likely (XDB_OP_EQ == op)) {
            bmp[fld_id >> 3] |= (1 << (fld_id & 7));
        }
        pFilter->cmp_op = op;

        if (xdb_unlikely (XDB_TOK_QM == vtype)) {
            pFilter->val.fld_type = pField->fld_type;
            pFilter->val.val_type = pField->sup_type;
            pStmt->pBind[pStmt->bind_count++] = &pFilter->val;
        } else {
            switch (pField->fld_type) {
                case XDB_TYPE_INT:
                case XDB_TYPE_BIGINT:
                case XDB_TYPE_TINYINT:
                case XDB_TYPE_SMALLINT: {
                    pFilter->val.ival = atoll(pVal);
                    pFilter->val.val_type = XDB_TYPE_BIGINT;
                    break;
                }
                case XDB_TYPE_CHAR: {
                    pFilter->val.str.len = vlen;
                    pFilter->val.str.str = pVal;
                    pFilter->val.val_type = XDB_TYPE_CHAR;
                    break;
                }
                case XDB_TYPE_FLOAT:
                case XDB_TYPE_DOUBLE: {
                    pFilter->val.fval = atof(pVal);
                    pFilter->val.val_type = XDB_TYPE_DOUBLE;
                    break;
                }
                default:
                    break;
            }
        }
    }

    //索引判断
    int fid;
    for (int i = 0; i < XDB_OBJM_COUNT(pTblm->idx_objm); ++i) {
        xdb_idxm_t *pIdxm = XDB_OBJM_GET(pTblm->idx_objm, pTblm->idx_order[i]);
        if (pStmt->filter_count < pIdxm->fld_count) {
            continue;
        }
        for (fid = 0; fid < pIdxm->fld_count; ++fid) {
            uint16_t fld_id = pIdxm->pFields[fid]->fld_id;
            if (!(bmp[fld_id >> 3] & (1 << (fld_id & 7)))) {
                break;
            }
        }
        if (fid == pIdxm->fld_count) {
            //xdb_dbglog ("use index %s\n", XDB_OBJ_NAME(pIdxm));
            pStmt->pIdxm = pIdxm;
            int idx_id = XDB_OBJ_ID(pIdxm);
            for (fid = 0; fid < pStmt->filter_count; ++fid) {
                xdb_filter_t *pFltr = &pStmt->filters[fid];
                int idx_fid = pFltr->pField->idx_fid[idx_id];
                if (idx_fid >= 0) {
                    // matched in index
                    pStmt->pIdxVals[idx_fid] = &pFltr->val;
                } else {
                    // extra filters
                    pStmt->pIdxFlts[pStmt->idx_flt_cnt++] = pFltr;
                }
            }
            break;
        }
    }

    return 0;
    error:
    return -1;
}

XDB_STATIC int
cdf_parse_setcol(xdb_stmt_select_t *pStmt, int colCount, cdf_set_col_t **arr) {
    int flen;
    char *pVal;
    for (int i = 0; i < colCount; ++i) {
        cdf_set_col_t *currCol = arr[i];
        pVal = currCol->val.str;
        flen = (int) strlen(currCol->fidldName);
        int fld_id = xdb_find_field(pStmt->pTblm, currCol->fidldName, flen);
        xdb_field_t *pField = &pStmt->pTblm->pFields[fld_id];
        xdb_setfld_t *pSetFld = &pStmt->set_flds[pStmt->set_count++];
        pSetFld->pField = pField;

        if (xdb_unlikely (XDB_TOK_QM == currCol->type)) {
            pSetFld->exp.op_val[0].fld_type = pField->fld_type;
            pSetFld->exp.op_val[0].val_type = pField->sup_type;
            pSetFld->exp.op_val[0].sup_type = pField->sup_type;
            pStmt->pBind[pStmt->bind_count++] = &pSetFld->exp.op_val[0];
        } else {
//            rc = xdb_parse_val (pStmt, &pSetFld->exp.op_val[0], pTkn);
            switch (pField->fld_type) {
                case XDB_TYPE_INT:
                case XDB_TYPE_BIGINT:
                case XDB_TYPE_TINYINT:
                case XDB_TYPE_SMALLINT: {
                    pSetFld->exp.op_val[0].ival = atoll(pVal);
                    pSetFld->exp.op_val[0].val_type = XDB_TYPE_BIGINT;
                    pSetFld->exp.op_val[0].sup_type = XDB_TYPE_BIGINT;
                    break;
                }
                case XDB_TYPE_CHAR: {
                    pSetFld->exp.op_val[0].str.len = currCol->val.len;
                    pSetFld->exp.op_val[0].str.str = pVal;
                    pSetFld->exp.op_val[0].val_type = XDB_TYPE_CHAR;
                    break;
                }
                case XDB_TYPE_FLOAT:
                case XDB_TYPE_DOUBLE: {
                    pSetFld->exp.op_val[0].fval = atof(pVal);
                    pSetFld->exp.op_val[0].val_type = XDB_TYPE_DOUBLE;
                    pSetFld->exp.op_val[0].sup_type = XDB_TYPE_DOUBLE;
                    break;
                }
                default:
                    break;
            }
        }
    }
}


XDB_STATIC xdb_stmt_t *
cdf_parse_update(xdb_conn_t *pConn, bool bPStmt, char *tableName, int filterCount, cdf_filter_t **arr, int colCount,
                 cdf_set_col_t **colArr) {
    xdb_stmt_select_t *pStmt;
    if (xdb_unlikely (bPStmt)) {
        pStmt = xdb_malloc(sizeof(*pStmt));
        XDB_EXPECT(NULL != pStmt, XDB_E_MEMORY, "Run out of memory");
    } else {
        pStmt = &pConn->stmt_union.select_stmt;
    }
    pStmt->stmt_type = XDB_STMT_UPDATE;
    pStmt->pSql = NULL;

    xdb_init_where_stmt(pStmt);

    xdb_token_t pTknObj = {};
    pTknObj.tk_type = XDB_TOK_ID;
    pTknObj.token = tableName;
    xdb_token_t *pTkn = &pTknObj;
    xdb_token_type type = XDB_TOK_ID;
    XDB_PARSE_DBTBLNAME();

    //替换成自己的
    type = cdf_parse_setcol(pStmt, colCount, colArr);

    cdf_parse_where(pConn, pStmt, filterCount, arr);

    return (xdb_stmt_t *) pStmt;

    error:
    xdb_stmt_free((xdb_stmt_t *) pStmt);
    return NULL;
}

XDB_STATIC xdb_stmt_t *
cdf_select_stmt(xdb_conn_t *pConn, bool bPStmt, char *tableName, va_list ap) {
    int rc;
    xdb_stmt_select_t *pStmt;
    // xdb_value_t *pVal;
    // xdb_value_t *pVal1;

    if (xdb_unlikely(bPStmt)) {
        pStmt = xdb_malloc(sizeof(*pStmt));
    } else {
        pStmt = &pConn->stmt_union.select_stmt;
    }

    pStmt->stmt_type = XDB_STMT_SELECT;
    pStmt->pSql = NULL;
    pStmt->meta_size = 0; // no alloc
    pStmt->pTblm = NULL;

    xdb_init_where_stmt(pStmt);

    // [db_name.]tbl_name
    xdb_token_t pTknObj = {};
    pTknObj.tk_type = XDB_TOK_ID;
    pTknObj.token = tableName;
    xdb_token_t *pTkn = &pTknObj;
    xdb_token_type type = XDB_TOK_ID;
    XDB_PARSE_DBTBLNAME();

    xdb_tblm_t *pTblm = pStmt->pTblm;

    pStmt->pMeta = pTblm->pMeta;
    pStmt->col_count = pTblm->pMeta->col_count;
    pStmt->meta_size = 0;

    //处理where条件
    int count = va_arg(ap, int);
    cdf_filter_t **arr = va_arg(ap, cdf_filter_t **);
    int parse_where = cdf_parse_where(pConn, pStmt, count, arr);
    if (parse_where != 0) {
        printf("parse where error\n");
    }
    return (xdb_stmt_t *) pStmt;

    error:
    xdb_stmt_free((xdb_stmt_t *) pStmt);
    return NULL;
}

XDB_STATIC xdb_stmt_t *
cdf_delete_stmt(xdb_conn_t *pConn, bool bPStmt, char *tableName, int count, cdf_filter_t **arr) {

    xdb_stmt_select_t *pStmt;

    if (xdb_unlikely (bPStmt)) {
        pStmt = xdb_malloc(sizeof(*pStmt));
        XDB_EXPECT(NULL != pStmt, XDB_E_MEMORY, "Run out of memory");
    } else {
        pStmt = &pConn->stmt_union.select_stmt;
    }
    pStmt->stmt_type = XDB_STMT_DELETE;
    pStmt->pSql = NULL;

    xdb_init_where_stmt(pStmt);


    xdb_token_t pTknObj = {};
    pTknObj.tk_type = XDB_TOK_ID;
    pTknObj.token = tableName;
    xdb_token_t *pTkn = &pTknObj;
    xdb_token_type type = XDB_TOK_ID;
    XDB_PARSE_DBTBLNAME();

    if (xdb_unlikely (pStmt->pTblm->pDbm->bSysDb)) {
        XDB_EXPECT (pConn == s_xdb_sysdb_pConn, XDB_E_CONSTRAINT, "Can't delete form table '%s' in system database",
                    XDB_OBJ_NAME(pStmt->pTblm));
    }

    int parse_where = cdf_parse_where(pConn, pStmt, count, arr);
    if (parse_where != 0) {
        printf("parse where error\n");
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
        case UPDATE_ROW: {
            char *tab = va_arg(ap, char*);
            int filterCount = va_arg(ap, int);
            cdf_filter_t **arr = va_arg(ap, cdf_filter_t**);
            int colCount = va_arg(ap, int);
            cdf_set_col_t **colArr = va_arg(ap, cdf_set_col_t**);
            return cdf_parse_update(pConn, false, tab, filterCount, arr, colCount, colArr);
        }
        case DELETE_ROW: {
            char *tab = va_arg(ap, char*);
            int count = va_arg(ap, int);
            cdf_filter_t **arr = va_arg(ap, cdf_filter_t**);
            return cdf_delete_stmt(pConn, false, tab, count, arr);
        }
        case SELECT_ROW: {
            char *tab = va_arg(ap, char*);
            return cdf_select_stmt(pConn, false, tab, ap);
        }
        default: {
            return NULL;
        }
    }
    return NULL;
}
