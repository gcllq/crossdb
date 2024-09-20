cmake_minimum_required(VERSION 2.8.3)
project(crossdb_example)
#add_compile_options(-O2)
#include_directories(../include)
add_executable(example c/example.c)
add_executable(singleTest c/singleTest.c)

target_link_libraries(example fork-em)
target_link_libraries(singleTest fork-em)