//
// Created by Administrator on 2024/9/20.
//
#include "crossdb.h"


struct student {
    uint32_t id;
    char name[16];
    uint32_t age;
    char class[16];
    float score;
    char info[255];
};
typedef struct student student;

int main(int argc, char **argv) {
    xdb_res_t *pRes;
    xdb_row_t *pRow;

    xdb_conn_t *pConn = xdb_open(argc > 1 ? argv[1] : ":memory:");
    // xdb_conn_t	*pConn = xdb_open ("example");
    XDB_CHECK (NULL != pConn, printf("failed to create DB\n"); return -1;);

    // Create Table
    pRes = xdb_exec(pConn,
                    "CREATE TABLE IF NOT EXISTS student (id INT PRIMARY KEY, name CHAR(16), age INT, class CHAR(16), score FLOAT, info CHAR(255), INDEX (age))");

    xdb_dbm_t *dbm;
    cdf_find_dbm_ptr(pConn, &dbm);
    xdb_tblm_t *tablePtr;
    cdf_find_table_ptr("student",dbm, &tablePtr);

    student data = {10,"jack1",100,"3-11",90, "I am a student."};
    student data1 = {20,"jack1",1000,"3-11",90, "I am a student."};
    void * arr [] = {&data, &data1};
    int rowIdList[2];
    printf("insert data\n");
    cdf_insert_data_row(tablePtr, 2, arr, rowIdList);

    printf("insert rowIdList: %d, %d\n", rowIdList[0], rowIdList[1]);

    printf("add index\n");
//    cdf_idx_insert_all(tablePtr, rowIdList[0], &data);
//    cdf_idx_insert_all(tablePtr, rowIdList[1], &data1);
    cdf_idx_insert_one(tablePtr, "age",rowIdList[0], &data);
    cdf_idx_insert_one(tablePtr, "age",rowIdList[1], &data1);

    printf("query data\n");
    cdf_filter_t filter = {"age", XDB_TOK_NUM,  CDF_OP_EQ, {"1000", 4}};
    cdf_filter_t * filterArr1 [] = {&filter};
    int rowIdListNew[2] = {};
    cdf_idx_select(tablePtr, "age", 1, filterArr1, rowIdListNew);
    printf("query result: %d, %d\n", rowIdListNew[0], rowIdListNew[1]);

    printf("select by pk data\n");
    cdf_filter_t pkFilter = {"id", XDB_TOK_NUM,  CDF_OP_EQ, {"10", 2}};
    cdf_filter_t * pkFilterArr [] = {&pkFilter};
    int pkRowIdList[2] = {};
    cdf_pk_idx_select(tablePtr, 1, pkFilterArr, pkRowIdList);
    printf("pk query result: %d\n", pkRowIdList[0]);
}