/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "ast.h"
#include <assert.h>

#include "../util/arr.h"
#include "../arithmetic/repository.h"
#include "../arithmetic/arithmetic_expression.h"

extern pthread_key_t _tlsASTKey;  // Thread local storage AST key.

void _AST_MapPath(AST *ast, const cypher_astnode_t *path) {
    uint nelems = cypher_ast_pattern_path_nelements(path);
    for (uint i = 0; i < nelems; i ++) {
        const cypher_astnode_t *ast_alias = NULL;
        const cypher_astnode_t *entity = cypher_ast_pattern_path_get_element(path, i);
        if (i % 2) {
            ast_alias = cypher_ast_rel_pattern_get_identifier(entity);
        } else {
            ast_alias = cypher_ast_node_pattern_get_identifier(entity);
        }

        // If the entity is aliased: (a:person)
        // the alias should be mapped as well as the entity.
        //  We may have already constructed a mapping
        // on a previous encounter: MATCH (a)-[]->(a)
        AR_ExpNode *exp = NULL;
        uint id = IDENTIFIER_NOT_FOUND;
        if (ast_alias) {
            // Add alias if it has not already been mapped.
            if (id == IDENTIFIER_NOT_FOUND) id = AST_MapAlias(ast, cypher_ast_identifier_get_name(ast_alias));
        }

        AST_MapEntity(ast, entity, id);
    }
}

void _AST_MapPattern(AST *ast, const cypher_astnode_t *pattern) {
    uint npaths = cypher_ast_pattern_npaths(pattern);
    for (uint i = 0; i < npaths; i ++) {
        const cypher_astnode_t *path = cypher_ast_pattern_get_path(pattern, i);
        _AST_MapPath(ast, path);
    }
}

void _AST_MapExpression(AST *ast, const cypher_astnode_t *expr) {
    // A CYPHER_AST_EXPRESSION is a generic type, including function calls,
    // scalars,and identifiers.
    // Any identifiers described within the expression or its children must
    // be represented in the AST mapping.
    const cypher_astnode_type_t type = cypher_astnode_type(expr);

    // Function invocations
    if (type == CYPHER_AST_APPLY_OPERATOR || type == CYPHER_AST_APPLY_ALL_OPERATOR) {
        uint arg_count = cypher_ast_apply_operator_narguments(expr);
        for (uint i = 0; i < arg_count; i ++) {
            const cypher_astnode_t *arg = cypher_ast_apply_operator_get_argument(expr, i);
            // Recursively map arguments
            _AST_MapExpression(ast, arg);
        }
        return;

    } else if (type == CYPHER_AST_IDENTIFIER) {
        /* Variables (full nodes and edges, UNWIND artifacts */
        const char *alias = cypher_ast_identifier_get_name(expr);
        AST_MapAlias(ast, alias);
    } else if (type == CYPHER_AST_PROPERTY_OPERATOR) {
        // Identifier and property pair
        // Extract the entity alias from the property. Currently, the embedded
        // expression should only refer to the IDENTIFIER type.
        const cypher_astnode_t *prop_expr = cypher_ast_property_operator_get_expression(expr);
        _AST_MapExpression(ast, prop_expr);
    } else if (type == CYPHER_AST_UNARY_OPERATOR) {
        /* Operator types (comparisons, filters, functions) */
        const cypher_astnode_t *unary_expr = cypher_ast_unary_operator_get_argument(expr);
        _AST_MapExpression(ast, unary_expr);
    } else if (type == CYPHER_AST_BINARY_OPERATOR) {
        // Arguments are of type CYPHER_AST_EXPRESSION
        _AST_MapExpression(ast, cypher_ast_binary_operator_get_argument1(expr));
        _AST_MapExpression(ast, cypher_ast_binary_operator_get_argument2(expr));
    } else if (type == CYPHER_AST_COMPARISON) {
        uint nchildren = cypher_ast_comparison_get_length(expr);
        for (uint i = 0; i < nchildren; i ++) {
            _AST_MapExpression(ast, cypher_ast_comparison_get_argument(expr, i));
        }
    } else {
        printf("Encountered unhandled type '%s'\n", cypher_astnode_typestr(type));
        assert(false);
    }
}

void _AST_MapProjection(AST *ast, const cypher_astnode_t *projection) {
    uint id = IDENTIFIER_NOT_FOUND;
    // A projection contains an expression and optionally an alias.
    const cypher_astnode_t *ast_alias = cypher_ast_projection_get_alias(projection);
    if (ast_alias) {
        id = AST_MapAlias(ast, cypher_ast_identifier_get_name(ast_alias));
    }
    const cypher_astnode_t *expr = cypher_ast_projection_get_expression(projection);
    AST_MapEntity(ast, expr, id);
    _AST_MapExpression(ast, expr);

}

void _AST_BuildEntityMap(AST *ast) {
    /* The AST->entity_map uses AST node pointers and string aliases as keys.
     * These keys resolve to integer IDs.
     * Not all keys have a unique ID, as multiple AST nodes might describe the same
     * entity (as will aliases).
     * The ExecutionPlanSegment will contain a mapping that converts these IDs
     * as well as other keys to Record IDs. */
    ast->entity_map = NewTrieMap();

    // Check every clause in this AST segment
    uint clause_count = cypher_ast_query_nclauses(ast->root);

    for (uint i = 0; i < clause_count; i ++) {
        const cypher_astnode_t *clause = cypher_ast_query_get_clause(ast->root, i);
        cypher_astnode_type_t type = cypher_astnode_type(clause);
        if (type == CYPHER_AST_MATCH || type == CYPHER_AST_CREATE) {
            // MATCH and CREATE clauses have 1 pattern which contains 1 or more paths.
            _AST_MapPattern(ast, cypher_ast_match_get_pattern(clause));
        } else if (type == CYPHER_AST_MERGE) {
            // MERGE clauses contain exactly one path.
            _AST_MapPath(ast, cypher_ast_merge_get_pattern_path(clause));
        } else if (type == CYPHER_AST_UNWIND) {
            // An UNWIND clause introduces 1 new alias.
            const char *alias = cypher_ast_identifier_get_name(cypher_ast_unwind_get_alias(clause));
            AST_MapAlias(ast, alias);
        } else if (type == CYPHER_AST_WITH) {
            // WITH introduces 1 or more aliases and refers to earlier entities.

        } else {
            // TODO probably unsafe
            uint child_count = cypher_astnode_nchildren(clause);
            for (uint j = 0; j < child_count; j ++) {
                const cypher_astnode_t *child = cypher_astnode_get_child(clause, j);
                _AST_MapExpression(ast, child);
            }
        }
        
            /*
        } else if (type == CYPHER_AST_SET) {
            // SET contains n SET_ITEMs
            // uint set_count = cypher_ast_set_nitems(clause);
            uint child_count = cypher_astnode_nchildren(clause);
            for (uint j = 0; j < set_count; j ++) {
                cypher_astnode_get_child
            }
        } else if (type == CYPHER_AST_DELETE) {
            // DELETE contains n AST_EXPRESSION nodes

        } else if (type == CYPHER_AST_CALL) {
            // CALL contains n AST_PROJECTION nodes as input arguments
            // and m AST_PROJECTION nodes as outputs.

        } else if (type == CYPHER_AST_RETURN) {
            // RETURN contains n AST_PROJECTION nodes.

        } else if (type == CYPHER_AST_ORDER_BY) {
            // ORDER BY contains n AST_SORT_ITEM nodes (which contain sort direction and an AST_EXPRESSION)

        }
        */
    }
}

// Note each function call within given expression
// Example: given the expression: "abs(max(min(a), abs(k)))"
// referred_funcs will include: "abs", "max" and "min".
void _consume_function_call_expression(const cypher_astnode_t *expression, TrieMap *referred_funcs) {
    // Value is an apply operator
    const cypher_astnode_t *func = cypher_ast_apply_operator_get_func_name(expression);
    const char *func_name = cypher_ast_function_name_get_value(func);
    TrieMap_Add(referred_funcs, (char*)func_name, strlen(func_name), NULL, TrieMap_DONT_CARE_REPLACE);

    unsigned int narguments = cypher_ast_apply_operator_narguments(expression);
    for(int i = 0; i < narguments; i++) {
        const cypher_astnode_t *child_exp = cypher_ast_apply_operator_get_argument(expression, i);
        cypher_astnode_type_t child_exp_type = cypher_astnode_type(child_exp);
        if(child_exp_type != CYPHER_AST_APPLY_OPERATOR) continue;
        _consume_function_call_expression(child_exp, referred_funcs);
    }
}

// void _mapUnwind(AST *ast, const cypher_astnode_t *unwind_clause) {
    // const cypher_astnode_t *alias_node = cypher_ast_unwind_get_alias(unwind_clause);
    // const char *alias = cypher_ast_identifier_get_name(cypher_ast_unwind_get_alias(unwind_clause));

    // const cypher_astnode_t *ast_exp = cypher_ast_unwind_get_expression(unwind_clause);
    // uint id = AST_AddRecordEntry(ast);
    // AR_ExpNode *exp = AR_EXP_NewReferenceNode(alias, id, false);
    // ast->defined_entities = array_append(ast->defined_entities, exp);
    // AST_MapAlias(ast, alias, id);

// }

bool AST_ReadOnly(const cypher_astnode_t *root) {
    // Iterate over children rather than clauses, as the root is not
    // guaranteed to be a query.
    uint num_children = cypher_astnode_nchildren(root);
    for (unsigned int i = 0; i < num_children; i ++) {
        const cypher_astnode_t *child = cypher_astnode_get_child(root, i);
        cypher_astnode_type_t type = cypher_astnode_type(child);
        if(type == CYPHER_AST_CREATE ||
           type == CYPHER_AST_MERGE ||
           type == CYPHER_AST_DELETE ||
           type == CYPHER_AST_SET ||
           type == CYPHER_AST_CREATE_NODE_PROP_INDEX ||
           type == CYPHER_AST_DROP_NODE_PROP_INDEX) {
            return false;
        }
    }

    return true;
}

bool AST_ContainsClause(const AST *ast, cypher_astnode_type_t clause) {
    return AST_GetClause(ast, clause) != NULL;
}

bool AST_ContainsErrors(const cypher_parse_result_t *result) {
    return cypher_parse_result_nerrors(result) > 0;
}

char* AST_ReportErrors(const cypher_parse_result_t *result) {
    char *errorMsg;
    unsigned int nerrors = cypher_parse_result_nerrors(result);
    for(unsigned int i = 0; i < nerrors; i++) {
        const cypher_parse_error_t *error = cypher_parse_result_get_error(result, i);

        // Get the position of an error.
        struct cypher_input_position errPos = cypher_parse_error_position(error);

        // Get the error message of an error.
        const char *errMsg = cypher_parse_error_message(error);

        // Get the error context of an error.
        // This returns a pointer to a null-terminated string, which contains a
        // section of the input around where the error occurred, that is limited
        // in length and suitable for presentation to a user.
        const char *errCtx = cypher_parse_error_context(error);

        // Get the offset into the context of an error.
        // Identifies the point of the error within the context string, allowing
        // this to be reported to the user, typically with an arrow pointing to the
        // invalid character.
        size_t errCtxOffset = cypher_parse_error_context_offset(error);

        asprintf(&errorMsg, "errMsg: %s line: %u, column: %u, offset: %zu errCtx: %s errCtxOffset: %zu", errMsg, errPos.line, errPos.column, errPos.offset, errCtx, errCtxOffset);
    }
    return errorMsg;
}

// Recursively collect the names of all function calls beneath a node
void AST_ReferredFunctions(const cypher_astnode_t *root, TrieMap *referred_funcs) {
    cypher_astnode_type_t root_type = cypher_astnode_type(root);
    if(root_type == CYPHER_AST_APPLY_OPERATOR) {
        _consume_function_call_expression(root, referred_funcs);
    } else {
        unsigned int child_count = cypher_astnode_nchildren(root);
        for(int i = 0; i < child_count; i++) {
            const cypher_astnode_t *child = cypher_astnode_get_child(root, i);
            AST_ReferredFunctions(child, referred_funcs);
        }
    }
}


// Retrieve the first instance of the specified clause in range, if any.
const cypher_astnode_t* AST_GetClause(const AST *ast, cypher_astnode_type_t clause_type) {
    uint clause_count = cypher_ast_query_nclauses(ast->root);
    for (uint i = 0; i < clause_count; i ++) {
        const cypher_astnode_t *child = cypher_ast_query_get_clause(ast->root, i);
        if (cypher_astnode_type(child) == clause_type) return child;
    }

    return NULL;
}

/* Collect references to all clauses of the specified type in the query. Since clauses
 * cannot be nested, we only need to check the immediate children of the query node. */
uint AST_GetTopLevelClauses(const AST *ast, cypher_astnode_type_t clause_type, const cypher_astnode_t **matches) {
    unsigned int num_found = 0;
    uint clause_count = cypher_ast_query_nclauses(ast->root);
    for (unsigned int i = 0; i < clause_count; i ++) {
        const cypher_astnode_t *child = cypher_ast_query_get_clause(ast->root, i);
        if (cypher_astnode_type(child) != clause_type) continue;

        matches[num_found] = child;
        num_found ++;
    }
    return num_found;
}

uint* AST_GetClauseIndices(const AST *ast, cypher_astnode_type_t clause_type) {
    uint *clause_indices = array_new(uint, 0);
    uint clause_count = cypher_ast_query_nclauses(ast->root);
    for (unsigned int i = 0; i < clause_count; i ++) {
        if (cypher_astnode_type(cypher_ast_query_get_clause(ast->root, i)) == clause_type) {
            clause_indices = array_append(clause_indices, i);
        }
    }
    return clause_indices;
}

uint AST_GetClauseCount(const AST *ast, cypher_astnode_type_t clause_type) {
    uint clause_count = cypher_ast_query_nclauses(ast->root);
    unsigned int num_found = 0;
    for (unsigned int i = 0; i < clause_count; i ++) {
        const cypher_astnode_t *child = cypher_ast_query_get_clause(ast->root, i);
        if (cypher_astnode_type(child) == clause_type) num_found ++;

    }
    return num_found;
}

uint AST_NumClauses(const AST *ast) {
    return cypher_astnode_nchildren(ast->root);
}

// TODO should just use this format or the TopLevel format
const cypher_astnode_t** AST_CollectReferencesInRange(const AST *ast, cypher_astnode_type_t type) {
    const cypher_astnode_t **found = array_new(const cypher_astnode_t *, 0);

    uint clause_count = cypher_ast_query_nclauses(ast->root);
    for (uint i = 0; i < clause_count; i ++) {
        const cypher_astnode_t *child = cypher_ast_query_get_clause(ast->root, i);
        if (cypher_astnode_type(child) != type) continue;

        found = array_append(found, child);
    }

    return found;
}

const cypher_astnode_t* AST_GetBody(const cypher_parse_result_t *result) {
    const cypher_astnode_t *statement = cypher_parse_result_get_root(result, 0);
    assert(statement && cypher_astnode_type(statement) == CYPHER_AST_STATEMENT);

    return cypher_ast_statement_get_body(statement);
}

AST* AST_Build(cypher_parse_result_t *parse_result) {
    AST *ast = rm_malloc(sizeof(AST));
    ast->root = AST_GetBody(parse_result);
    assert(ast->root);
    ast->entity_map = NULL;

    return ast;
}

AST* AST_NewSegment(AST *master_ast, uint start_offset, uint end_offset) {
    AST *ast = rm_malloc(sizeof(AST));

    uint n = end_offset - start_offset;

    cypher_astnode_t *clauses[n];
    for (uint i = 0; i < n; i ++) {
        clauses[i] = (cypher_astnode_t*)cypher_ast_query_get_clause(master_ast->root, i + start_offset);
    }
    // TODO warning - 'range' causes failures when 0-initialized.
    // Revisit to determine safety of this instantiation.
    struct cypher_input_range range;
    ast->root = cypher_ast_query(NULL, 0, (cypher_astnode_t *const *)clauses, n, NULL, 0, range);
    // ast->root = cypher_ast_query(NULL, 0, (cypher_astnode_t *const *)clauses, n, clauses, n, range);

    pthread_setspecific(_tlsASTKey, ast); // TODO I don't know if I like this
    _AST_BuildEntityMap(ast);

    return ast;
}

// TODO I find it so weird that this is necessary
long AST_ParseIntegerNode(const cypher_astnode_t *int_node) {
    assert(int_node);

    const char *value_str = cypher_ast_integer_get_valuestr(int_node);
    return strtol(value_str, NULL, 0);
}

/* TODO CYPHER_OP_IS_NULL and CYPHER_OP_IS_NOT_NULL are also unaries (though postfix).
 * Investigate this, also check CIPs because I think null handling may be different in
 * Cypher now than it is in the parser. */
/*
AST_Operator AST_ConvertUnaryOperatorNode(const cypher_astnode_t *expr) {
    // TODO All in AR_EXP_FromExpression right now
}
*/

bool AST_ClauseContainsAggregation(const cypher_astnode_t *clause) {
    assert(clause);

    bool aggregated = false;

    // Retrieve all user-specified functions in clause.
    TrieMap *referred_funcs = NewTrieMap();
    AST_ReferredFunctions(clause, referred_funcs);

    void *value;
    tm_len_t len;
    char *funcName;
    TrieMapIterator *it = TrieMap_Iterate(referred_funcs, "", 0);
    while(TrieMapIterator_Next(it, &funcName, &len, &value)) {
        if(Agg_FuncExists(funcName)) {
            aggregated = true;
            break;
        }
    }
    TrieMapIterator_Free(it);
    TrieMap_Free(referred_funcs, TrieMap_NOP_CB);

    return aggregated;
}

AST* AST_GetFromTLS(void) {
    AST *ast = pthread_getspecific(_tlsASTKey);
    assert(ast);
    return ast;
}

void AST_Free(AST *ast) {
    if (ast->entity_map) TrieMap_Free(ast->entity_map, TrieMap_NOP_CB);

    rm_free(ast);
}

