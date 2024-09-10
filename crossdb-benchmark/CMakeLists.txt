cmake_minimum_required(VERSION 2.8.3)
project(crossdb_benchmark)
find_package(benchmark CONFIG REQUIRED)

add_executable(crossdb_benchmark CrossDBBenchMark.cpp)
set_target_properties(crossdb_benchmark PROPERTIES PUBLIC_HEADER "../include/crossdb.h")
target_link_libraries(crossdb_benchmark benchmark::benchmark benchmark::benchmark_main fork-em)