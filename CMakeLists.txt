cmake_minimum_required(VERSION 2.8)
project(RedisGraph)

add_compile_options(-g -Wall -Wextra -fPIC)

set ( BUILD_GRB_STATIC_LIBRARY true )
add_subdirectory(deps/GraphBLAS)
set(XXHASH_BUNDLED_MODE ON CACHE BOOL "" FORCE)
add_subdirectory(deps/xxhash/cmake_unofficial)
add_subdirectory(deps/libcypher-parser)
add_subdirectory(deps/RediSearch)

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
        src/arithmetic/aggregate_funcs
        src/arithmetic/algebraic_expression
        src/arithmetic/boolean_funcs
        src/arithmetic/comprehension_funcs
        src/arithmetic/conditional_funcs
        src/arithmetic/entity_funcs
        src/arithmetic/list_funcs
        src/arithmetic/numeric_funcs
        src/arithmetic/path_funcs
        src/arithmetic/placeholder_funcs
        src/arithmetic/string_funcs
        src/arithmetic/time_funcs
        src/bulk_insert
        src/commands
        src/datatypes
        src/datatypes/path
        src/execution_plan
        src/execution_plan/execution_plan_build
        src/execution_plan/ops
        src/execution_plan/ops/shared
        src/execution_plan/optimizations
        src/filter_tree
        src/graph
        src/graph/entities
        src/serializers
        src/serializers/encoder
        src/serializers/encoder/*
        src/serializers/decoders
        src/serializers/decoders/current
        src/serializers/decoders/current/*
        src/serializers/decoders/prev
        src/serializers/decoders/prev/*
        src/GraphBLASExt
        src/grouping
        src/index
        src/ast
        src/ast/enrichment
        src/resultset
        src/resultset/formatters
        src/schema
        src/slow_log
        src/procedures
        src/util
        src/util/datablock
        src/util/object_pool
        src/util/thpool
        src/util/range
        src/util/cache
)

foreach (f ${SOURCE_FILES})
    message(${f})
endforeach()

add_library(
        redisgraph
        MODULE
        ${SOURCE_FILES}
        deps/rax/rc4rand.c
        deps/rax/rax.c
        deps/rax/crc16.c
)

target_link_libraries(redisgraph
        c
        m
        pthread
        dl
        graphblas_static
        xxhash
        libcypher-parser
        redisearch
#        openmp
)
#target_link_options(redisgraph PRIVATE -shared -Bsymbolic -Bsymbolic-functions)
target_include_directories(redisgraph
        PRIVATE
        src
        deps/xxHash
        deps/RediSearch/src
        deps/rax
        deps/libcypher-parser/lib/src
        deps/GraphBLAS/Include)