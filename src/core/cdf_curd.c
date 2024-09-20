//
// Created by Administrator on 2024/9/20.
//
int cdf_insert_data_row(xdb_tblm_t *pTblm, int count, void **pRows, int *rowIdList) {
    //xdb_dbgprint ("ok insert tbl '%s' count %d\n", pStmt->XDB_OBJ_NAME(pTblm), pStmt->row_count);

    for (int i = 0; i < count; ++i) {
        void *pRow = pRows[i];
        xdb_stgmgr_t *pStgMgr = &pTblm->stg_mgr;

        void *pRowDb;
        xdb_rowid rid = xdb_stg_alloc(pStgMgr, &pRowDb);
        if (xdb_unlikely (rid <= 0)) {
            xdb_dbglog ("No space");
            return -1;
        }
        rowIdList[i] = rid;
        //xdb_dbglog ("alloc %d max%d\n", rid, XDB_STG_CAP(pStgMgr));

        memcpy(pRowDb, pRow, pTblm->row_size);
        //插入索引数据
        cdf_idx_insert_one(pTblm, "PRIMARY", rid, pRow);
    }

    return count;
}

int cdf_find_dbm_ptr(xdb_conn_t *pConn, xdb_dbm_t **dbmPtr) {
    *dbmPtr = pConn->pCurDbm;
    return 0;
}

int cdf_find_table_ptr(char *tableName, xdb_dbm_t *pCurDbm, xdb_tblm_t **pTblm) {
    *pTblm = xdb_find_table(pCurDbm, tableName);
    return 0;
}

