#include <benchmark/benchmark.h>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>

#include "../include/crossdb.h"


int id = 0;
bool init = false;
std::mutex ml;
xdb_conn_t *pConn = nullptr;

// 基准测试类
class CDF_DBBenchmark : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State &state) override {
        ml.lock();
        if (!init) {
            xdb_res_t *pRes;
            xdb_row_t *pRow;

            pConn = xdb_open("school");
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


BENCHMARK_DEFINE_F(CDF_DBBenchmark, InsertBenchmark)(benchmark::State &state) {
    xdb_stmt_t *pStmt = xdb_stmt_prepare (pConn, "INSERT INTO student (id,name,age,sex) VALUES (?,?,?,?)");
    xdb_res_t *pRes;
    for (auto _: state) {
        id++;
        pRes = xdb_stmt_bexec (pStmt, id, id, id, id);
        XDB_RESCHK(pRes, printf ("Can't insert data\n"););
        xdb_free_result (pRes);
    }
    xdb_stmt_close (pStmt);
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


 BENCHMARK_REGISTER_F(CDF_DBBenchmark, InsertBenchmark)
 ->Iterations(100000)->Threads(1)->Repetitions(10);

BENCHMARK_REGISTER_F(CDF_DBBenchmark, selectEQ)
->Iterations(100000)->Threads(1)->Repetitions(10);


// BENCHMARK_REGISTER_F(CDF_DBBenchmark, selectRange)
// ->Iterations(100000)->Threads(1)->Repetitions(10);
BENCHMARK_MAIN();
