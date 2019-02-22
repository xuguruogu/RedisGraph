/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Apache License, Version 2.0,
* modified with the Commons Clause restriction.
*/

#ifndef NEW_AST_H
#define NEW_AST_H

#include "ast_common.h"
#include "../util/triemap/triemap.h"
#include "../../deps/libcypher-parser/lib/src/cypher-parser.h"

// #define IDENTIFIER_NOT_FOUND UINT_MAX

typedef enum {
	AST_VALID,
	AST_INVALID
} AST_Validation;

typedef struct AR_ExpNode AR_ExpNode;

typedef struct {
    const char *alias;    // Alias given to this return element (using the AS keyword)
    AR_ExpNode *exp;
} ReturnElementNode;

typedef struct {
    const cypher_astnode_t *root;
    // Extensible array of entities described in MATCH, MERGE, and CREATE clauses
    AR_ExpNode **defined_entities;
    TrieMap *identifier_map;
    ReturnElementNode **return_expressions;
    unsigned int order_expression_count; // TODO maybe use arr.h instead
    AR_ExpNode **order_expressions;
} NEWAST;

// AST clause validations.
AST_Validation NEWAST_Validate(const cypher_astnode_t *ast, char **reason);

// Checks if AST represent a read only query.
bool NEWAST_ReadOnly(const cypher_astnode_t *query);

// Checks to see if AST contains specified clause. 
bool NEWAST_ContainsClause(const cypher_astnode_t *ast, cypher_astnode_type_t clause);

// Checks to see if query contains any errors.
bool NEWAST_ContainsErrors(const cypher_parse_result_t *ast);

// Report encountered errors.
char* NEWAST_ReportErrors(const cypher_parse_result_t *ast);

// Returns all function (aggregated & none aggregated) mentioned in query.
void NEWAST_ReferredFunctions(const cypher_astnode_t *root, TrieMap *referred_funcs);

// Checks if RETURN clause contains collapsed entities.
int NEWAST_ReturnClause_ContainsCollapsedNodes(const cypher_astnode_t *ast);

// Returns specified clause or NULL.
const cypher_astnode_t* NEWAST_GetClause(const cypher_astnode_t *query, cypher_astnode_type_t clause_type);

unsigned int NewAST_GetTopLevelClauses(const cypher_astnode_t *query, cypher_astnode_type_t clause_type, const cypher_astnode_t **matches);

const cypher_astnode_t* NEWAST_GetBody(const cypher_parse_result_t *result);

NEWAST* NEWAST_Build(cypher_parse_result_t *parse_result);

void NEWAST_BuildAliasMap(NEWAST *ast);

unsigned int NEWAST_GetAliasID(const NEWAST *ast, char *alias);

AR_ExpNode* NEWAST_GetEntity(const NEWAST *ast, unsigned int id);

NEWAST* NEWAST_GetFromLTS(void);

#endif
