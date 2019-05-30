cmake_minimum_required(VERSION 2.8)
project(RedisGraph)

add_compile_options(-g -Wall -Wextra -fPIC)

add_subdirectory(deps/GraphBLAS)
set(XXHASH_BUNDLED_MODE ON CACHE BOOL "" FORCE)
add_subdirectory(deps/xxhash/cmake_unofficial)
add_subdirectory(deps/libcypher-parser)

add_definitions(-D_GNU_SOURCE -DREDIS_MODULE_TARGET -DREDISMODULE_EXPERIMENTAL_API -DXXH_STATIC_LINKING_ONLY)

##
## Populate the names of all source and header files in the indicated paths in a designated variable.
##
## When RECURSIVE is specified, directories are traversed recursively.
##
## Use: scan_source_directories(VAR my_result_var [RECURSIVE] PATHS [path1 path2 ...])
##
function (scan_c_source_directories)
    set(options RECURSIVE)
    set(oneValueArgs VAR)
    set(multiValueArgs PATHS)
    cmake_parse_arguments(args "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")

    set(globs "")

    foreach (dir ${args_PATHS})
        list(APPEND globs "${dir}/*.c")
    endforeach()

    if (args_RECURSIVE)
        set(glob_kind GLOB_RECURSE)
    else()
        set(glob_kind GLOB)
    endif()

    file(${glob_kind} var
            ${globs})

    set(${args_VAR} ${var} PARENT_SCOPE)
endfunction()

scan_c_source_directories(
        VAR SOURCE_FILES
        PATHS
        src
        src/algorithms
        src/arithmetic
        src/ast
        src/bulk_insert
        src/commands
        src/execution_plan
        src/execution_plan/ops
        src/execution_plan/optimizations
        src/filter_tree
        src/graph
        src/graph/entities
        src/graph/serializers
        src/GraphBLASExt
        src/grouping
        src/index
        src/procedures
        src/record
        src/resultset
        src/schema
        src/util
        src/util/datablock
        src/util/thpool
        src/util/triemap)

foreach (f ${SOURCE_FILES})
    message(${f})
endforeach()

add_library(redis_graph MODULE ${SOURCE_FILES})

target_link_libraries(redis_graph
        c
        m
        pthread
        dl
        graphblas_static
        xxhash
        libcypher-parser
)
target_link_options(redis_graph PRIVATE -shared -Bsymbolic -Bsymbolic-functions)
target_include_directories(redis_graph PRIVATE src deps)