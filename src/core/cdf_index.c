//
// Created by Administrator on 2024/9/20.
//
int
cdf_idx_addRow(xdb_tblm_t *pTblm, uint64_t rid, void *pRow) {
    return xdb_idx_addRow(NULL, pTblm, (xdb_rowid) rid, pRow);
}

int
cdf_idx_select(xdb_tblm_t *pTblm, char *idxName, int filterCount, cdf_filter_t **filterArr, int *rowIdList) {
    //首先构造xdb_filter
    xdb_stmt_select_t pStmt = {};
    xdb_init_where_stmt(&pStmt);
    pStmt.pTblm = pTblm;
    cdf_parse_where(&pStmt, filterCount, filterArr);

    //调用索引查询
    xdb_idxm_t *pIdxm = xdb_find_index(pTblm, idxName);

    xdb_rowset_t pRowSet;
    if (pStmt.filter_count > 0 && (NULL != pStmt.pIdxm)) {
        pStmt.pIdxm->pIdxOps->idx_query(pStmt.pIdxm, pStmt.pIdxVals, pStmt.pIdxFlts, pStmt.idx_flt_cnt,
                                        &pRowSet);
    } else {
        return -1;
    }
    for (int i = 0; i < pRowSet.count; ++i) {
        rowIdList[i] = pRowSet.pRowList[i].rid;
    }
    return 0;
}