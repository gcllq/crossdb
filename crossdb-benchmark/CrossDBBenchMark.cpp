#include <benchmark/benchmark.h>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>

#include "../include/crossdb.h"
// #include "../src/parser/xdb_stmt.h"


int id = 0;
bool init = false;
std::mutex ml;
xdb_conn_t *pConn = nullptr;
struct student {
    int32_t id;
    int32_t age;
    int32_t name;
    int32_t sex;
};
// 基准测试类
class CDF_DBBenchmark : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State &state) override {
        ml.lock();
        if (!init) {
            xdb_res_t *pRes;
            xdb_row_t *pRow;

            pConn = xdb_open(":memory:");
            XDB_CHECK(NULL != pConn, printf ("failed to create DB\n"); return);

            // Create Table
            pRes = xdb_exec(
                pConn, "CREATE TABLE IF NOT EXISTS student (id INT PRIMARY KEY, name INT, age INT,sex INT)");
            XDB_RESCHK(pRes, printf ("Can't create table student\n"););
            // pRes = xdb_exec(pConn, "CREATE INDEX idx_age ON student (age)");
            XDB_RESCHK(pRes, printf ("Can't create index age on student\n"););
            // pRes = xdb_exec(pConn, "CREATE INDEX idx_name ON student (name)");
            XDB_RESCHK(pRes, printf ("Can't create index age on student\n"););

            //创建表
            std::cout << "create table over" << std::endl;
            xdb_stmt_t *pStmt = xdb_stmt_prepare (pConn, "INSERT INTO student (id,name,age,sex) VALUES (?,?,?,?)");
            for (int i = 0; i < 100000; i++) {

                if (NULL != pStmt) {
                    pRes = xdb_stmt_bexec (pStmt, id, i, i, i);
                    // pRow = xdb_fetch_row (pRes);
                    // handle pRow
                    // xdb_free_result (pRes);

                    // close when finish using
                    // xdb_stmt_close (pStmt);
                }


            }
            xdb_stmt_close (pStmt);
            std::cout << "init over " << std::endl;
            init = true;
        }

        ml.unlock();
    }

    void TearDown(const ::benchmark::State &state) override {
    }
};


BENCHMARK_DEFINE_F(CDF_DBBenchmark, PreparedInsertBenchmark)(benchmark::State &state) {

    xdb_res_t *pRes;

    for (auto _: state) {
        xdb_stmt_t *pStmt = xdb_stmt_prepare (pConn, "INSERT INTO student (id,name,age,sex) VALUES (?,?,?,?)");
        id++;
        pRes = xdb_stmt_bexec (pStmt, id, id, id, id);
        XDB_RESCHK(pRes, printf ("Can't insert data\n"););
        xdb_free_result (pRes);
        xdb_stmt_close (pStmt);
    }

    state.SetItemsProcessed(state.iterations());
}


BENCHMARK_DEFINE_F(CDF_DBBenchmark, PreparedBatchInsertBenchmark)(benchmark::State &state) {

    xdb_res_t *pRes;
    xdb_stmt_t *pStmt = xdb_stmt_prepare (pConn, "INSERT INTO student (id,name,age,sex) VALUES (?,?,?,?)");
    for (auto _: state) {

        id++;
        pRes = xdb_stmt_bexec (pStmt, id, id, id, id);
        XDB_RESCHK(pRes, printf ("Can't insert data\n"););
        xdb_free_result (pRes);

    }
    xdb_stmt_close (pStmt);
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_DEFINE_F(CDF_DBBenchmark, InsertNorBenchmark)(benchmark::State &state) {
    // xdb_stmt_t *pStmt = xdb_stmt_prepare (pConn, "INSERT INTO student (id,name,age,sex) VALUES (?,?,?,?)");
    xdb_res_t *pRes;
    for (auto _: state) {
        id++;
        // pRes = xdb_stmt_bexec (pStmt, id, id, id, id);
        char buffer[100];
        char name[] = "world";
        sprintf(buffer, "INSERT INTO student (id,name,age,sex) VALUES (%d,%d,%d,%d)", id, id, id, id);
        pRes = xdb_exec(pConn, buffer);
        XDB_RESCHK(pRes, printf ("Can't insert data\n"););
        xdb_free_result (pRes);
    }
    // xdb_stmt_close (pStmt);
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_DEFINE_F(CDF_DBBenchmark, InsertSelfSTMTBenchmark)(benchmark::State &state) {
    // xdb_stmt_t *pStmt = xdb_stmt_prepare (pConn, "INSERT INTO student (id,name,age,sex) VALUES (?,?,?,?)");
    xdb_res_t *pRes;
    for (auto _: state) {
        id++;
        // pRes = xdb_stmt_bexec (pStmt, id, id, id, id);
        student data = {id,id,id,id};
        void * arr [] = {&data};
        pRes = cdf_insert_data(pConn, "student", 1, arr);
        XDB_RESCHK(pRes, printf ("Can't insert data\n"););
        xdb_free_result (pRes);
    }
    // xdb_stmt_close (pStmt);
    state.SetItemsProcessed(state.iterations());
}


BENCHMARK_DEFINE_F(CDF_DBBenchmark, selectEQ)(benchmark::State &state) {
    xdb_stmt_t *pStmt = xdb_stmt_prepare (pConn, "SELECT * FROM student WHERE id=?");
    xdb_res_t *pRes;
    xdb_row_t *pRow;

    id = 0;

    for (auto _: state) {
        id++;
        pRes = xdb_stmt_bexec (pStmt, id);
        // pRow = xdb_fetch_row (pRes);
        XDB_RESCHK(pRes, printf ("Can't select id %d data\n", id);continue;);
        xdb_free_result (pRes);
        // xdb_print_row (pRes->col_meta, pRow, 0);
        // printf ("\n");

    }
    xdb_stmt_close (pStmt);
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_DEFINE_F(CDF_DBBenchmark, selectRange)(benchmark::State &state) {
    xdb_stmt_t *pStmt = xdb_stmt_prepare (pConn, "SELECT * FROM student WHERE id < ?");
    xdb_res_t *pRes;
    xdb_row_t *pRow;

    id = 0;

    for (auto _: state) {
        id++;
        pRes = xdb_stmt_bexec (pStmt, 2);
        // pRow = xdb_fetch_row (pRes);
        XDB_RESCHK(pRes, printf ("Can't select id %d data\n", id);continue;);
        xdb_free_result (pRes);
        // xdb_print_row (pRes->col_meta, pRow, 0);
        // printf ("\n");

    }
    xdb_stmt_close (pStmt);
    state.SetItemsProcessed(state.iterations());
}


 BENCHMARK_REGISTER_F(CDF_DBBenchmark, PreparedInsertBenchmark)
 ->Iterations(100000)->Threads(1)->Repetitions(10);
BENCHMARK_REGISTER_F(CDF_DBBenchmark, InsertNorBenchmark)
->Iterations(100000)->Threads(1)->Repetitions(10);
BENCHMARK_REGISTER_F(CDF_DBBenchmark, PreparedBatchInsertBenchmark)
->Iterations(100000)->Threads(1)->Repetitions(10);
BENCHMARK_REGISTER_F(CDF_DBBenchmark, InsertSelfSTMTBenchmark)
->Iterations(100000)->Threads(1)->Repetitions(10);
BENCHMARK_REGISTER_F(CDF_DBBenchmark, selectEQ)
->Iterations(100000)->Threads(1)->Repetitions(10);


// BENCHMARK_REGISTER_F(CDF_DBBenchmark, selectRange)
// ->Iterations(100000)->Threads(1)->Repetitions(10);
BENCHMARK_MAIN();
