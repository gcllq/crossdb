cmake_minimum_required(VERSION 2.8.3)
project(crossdb)
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
#add_compile_options(-O2)
add_library(fork-em SHARED src/crossdb.c)
add_executable(xdb-cli src/xdb-cli.c)
target_link_libraries(xdb-cli pthread)
add_subdirectory(examples)
set_target_properties(fork-em PROPERTIES PUBLIC_HEADER "include/crossdb.h")
#add_subdirectory(test)
add_subdirectory(crossdb-benchmark)

#INSTALL(
#	TARGETS xdb-cli crossdb
#	RUNTIME DESTINATION bin
#	LIBRARY DESTINATION lib
#	PUBLIC_HEADER DESTINATION include
#)
#
#add_custom_target("uninstall" COMMENT "Uninstall installed files")
#add_custom_command(
#    TARGET "uninstall"
#    POST_BUILD
#    COMMENT "Uninstall files with install_manifest.txt"
#    COMMAND xargs rm -vf < install_manifest.txt || echo Nothing in
#            install_manifest.txt to be uninstalled!
#)

set(CPACK_PACKAGE_NAME crossdb)
set(CPACK_PACKAGE_VERSION_MAJOR 0)
set(CPACK_PACKAGE_VERSION_MINOR 8)
set(CPACK_PACKAGE_VERSION_PATCH 0)
set(CPACK_PACKAGE_CONTACT "crossdb <support@crossdb.org>")

#set(CPACK_GENERATOR DEB)
#set(CPACK_GENERATOR DEB NSIS RPM)
#include(CPack)
