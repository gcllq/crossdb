//
// Created by Administrator on 2024/9/20.
//

int
cdf_idx_insert_all (xdb_tblm_t *pTblm, uint64_t rid, void *pRow) {
    return xdb_idx_addRow(NULL, pTblm, (xdb_rowid) rid, pRow);
}


int
cdf_idx_insert_one(xdb_tblm_t *pTblm, char *idxName, uint64_t rid, void *pRow) {
    int rc = 0;
    xdb_idxm_t *pIdxm = xdb_find_index(pTblm, idxName);
    rc = pIdxm->pIdxOps->idx_add (pIdxm, rid, pRow);
    if (xdb_unlikely (rc != XDB_OK)) {
        // recover added index
        return rc;
    }
    return rc;
}

int
cdf_pk_idx_insert (xdb_tblm_t *pTblm, uint64_t rid, void *pRow) {
    return cdf_idx_insert_one(pTblm, "PRIMARY", rid, pRow);
}

int
cdf_pk_idx_select(xdb_tblm_t *pTblm, cdf_filter_t **filterArr, int **rowIdList) {

    return cdf_idx_select(pTblm, "PRIMARY", 1, filterArr, rowIdList);
}

int
cdf_idx_select(xdb_tblm_t *pTblm, char *idxName, int filterCount, cdf_filter_t **filterArr, int **rowIdList) {
    //首先构造xdb_filter
    xdb_stmt_select_t pStmt = {};
    xdb_init_where_stmt(&pStmt);
    pStmt.pTblm = pTblm;
    cdf_parse_where(&pStmt, filterCount, filterArr);

    //调用索引查询
    xdb_idxm_t *pIdxm = xdb_find_index(pTblm, idxName);

    xdb_rowset_t pRowSet = {};
    if (pStmt.filter_count > 0 && (NULL != pStmt.pIdxm)) {
        pIdxm->pIdxOps->idx_query(pStmt.pIdxm, pStmt.pIdxVals, pStmt.pIdxFlts, pStmt.idx_flt_cnt,
                                  &pRowSet);
    } else {
        return -1;
    }


    *rowIdList = (int*)malloc(pRowSet.count * sizeof(int));;
    for (int i = 0; i < pRowSet.count; ++i) {
        *rowIdList[i] = pRowSet.pRowList[i].rid;
    }
    return 0;
}