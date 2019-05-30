cmake_minimum_required(VERSION 2.8)
project(libcypher-parser)

add_compile_options(-g -Wall -Wextra -fPIC)

add_custom_command(OUTPUT
        ${CMAKE_CURRENT_SOURCE_DIR}/config.h
        ${CMAKE_CURRENT_SOURCE_DIR}/lib/src/cypher-parser.h
        ${CMAKE_CURRENT_SOURCE_DIR}/lib/src/parser_leg.c
        ${CMAKE_CURRENT_SOURCE_DIR}/lib/src/quick_parser_leg.c
        COMMAND autoreconf --install
        COMMAND ./configure
        COMMAND leg -o lib/src/parser_leg.c lib/src/parser.leg
        COMMAND leg -o lib/src/quick_parser_leg.c lib/src/quick_parser.leg
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_library(libcypher-parser
        config.h
        lib/src/cypher-parser.h

        lib/src/ast_all.c
        lib/src/ast_all_nodes_scan.c
        lib/src/ast_all_rels_scan.c
        lib/src/ast_any.c
        lib/src/ast_apply_all_operator.c
        lib/src/ast_apply_operator.c
        lib/src/ast_binary_operator.c
        lib/src/ast_block_comment.c
        lib/src/ast_boolean.c
        lib/src/ast.c
        lib/src/ast_call.c
        lib/src/ast_case.c
        lib/src/ast_collection.c
        lib/src/ast_command.c
        lib/src/ast_comment.c
        lib/src/ast_comparison.c
        lib/src/ast_create.c
        lib/src/ast_create_node_prop_constraint.c
        lib/src/ast_create_node_prop_index.c
        lib/src/ast_create_rel_prop_constraint.c
        lib/src/ast_cypher_option.c
        lib/src/ast_cypher_option_param.c
        lib/src/ast_delete.c
        lib/src/ast_drop_node_prop_constraint.c
        lib/src/ast_drop_node_prop_index.c
        lib/src/ast_drop_rel_prop_constraint.c
        lib/src/ast_error.c
        lib/src/ast_explain_option.c
        lib/src/ast_expression.c
        lib/src/ast_extract.c
        lib/src/ast_false.c
        lib/src/ast_filter.c
        lib/src/ast_float.c
        lib/src/ast_foreach.c
        lib/src/ast_function_name.c
        lib/src/ast_identifier.c
        lib/src/ast_index_name.c
        lib/src/ast_integer.c
        lib/src/ast_label.c
        lib/src/ast_labels_operator.c
        lib/src/ast_line_comment.c
        lib/src/ast_list_comprehension.c
        lib/src/ast_load_csv.c
        lib/src/ast_map.c
        lib/src/ast_map_projection_all_properties.c
        lib/src/ast_map_projection.c
        lib/src/ast_map_projection_identifier.c
        lib/src/ast_map_projection_literal.c
        lib/src/ast_map_projection_property.c
        lib/src/ast_map_projection_selector.c
        lib/src/ast_match.c
        lib/src/ast_match_hint.c
        lib/src/ast_merge_action.c
        lib/src/ast_merge.c
        lib/src/ast_merge_properties.c
        lib/src/ast_named_path.c
        lib/src/ast_node_id_lookup.c
        lib/src/ast_node_index_lookup.c
        lib/src/ast_node_index_query.c
        lib/src/ast_node_pattern.c
        lib/src/ast_none.c
        lib/src/ast_null.c
        lib/src/ast_on_create.c
        lib/src/ast_on_match.c
        lib/src/ast_order_by.c
        lib/src/ast_parameter.c
        lib/src/ast_pattern.c
        lib/src/ast_pattern_comprehension.c
        lib/src/ast_pattern_path.c
        lib/src/ast_proc_name.c
        lib/src/ast_profile_option.c
        lib/src/ast_projection.c
        lib/src/ast_property_operator.c
        lib/src/ast_prop_name.c
        lib/src/ast_query.c
        lib/src/ast_query_clause.c
        lib/src/ast_query_option.c
        lib/src/ast_range.c
        lib/src/ast_reduce.c
        lib/src/ast_rel_id_lookup.c
        lib/src/ast_rel_index_lookup.c
        lib/src/ast_rel_index_query.c
        lib/src/ast_rel_pattern.c
        lib/src/ast_reltype.c
        lib/src/ast_remove.c
        lib/src/ast_remove_item.c
        lib/src/ast_remove_labels.c
        lib/src/ast_remove_property.c
        lib/src/ast_return.c
        lib/src/ast_schema_command.c
        lib/src/ast_set_all_properties.c
        lib/src/ast_set.c
        lib/src/ast_set_item.c
        lib/src/ast_set_labels.c
        lib/src/ast_set_property.c
        lib/src/ast_shortest_path.c
        lib/src/ast_single.c
        lib/src/ast_slice_operator.c
        lib/src/ast_sort_item.c
        lib/src/ast_start.c
        lib/src/ast_start_point.c
        lib/src/ast_statement.c
        lib/src/ast_statement_option.c
        lib/src/ast_string.c
        lib/src/ast_subscript_operator.c
        lib/src/ast_true.c
        lib/src/ast_unary_operator.c
        lib/src/ast_union.c
        lib/src/ast_unwind.c
        lib/src/ast_using_index.c
        lib/src/ast_using_join.c
        lib/src/ast_using_periodic_commit.c
        lib/src/ast_using_scan.c
        lib/src/ast_with.c
        lib/src/errors.c
        lib/src/operators.c
        lib/src/parser.c
        lib/src/parser_config.c
        lib/src/quick_parser.c
        lib/src/result.c
        lib/src/segment.c
        lib/src/string_buffer.c
        lib/src/util.c
        lib/src/vector.c)

target_link_libraries(libcypher-parser pthread m)

add_executable(cypher-linter linter/src/cypher-lint.c)
target_link_libraries(cypher-linter libcypher-parser)
target_include_directories(cypher-linter PRIVATE lib/src)