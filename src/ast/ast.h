/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#ifndef NEW_AST_H
#define NEW_AST_H

// #include "/ast_shared.h"
#include "../value.h"
#include "../util/triemap/triemap.h"
#include "../../deps/libcypher-parser/lib/src/cypher-parser.h"

typedef const cypher_astnode_t* AST_IDENTIFIER;

#define NOT_IN_RECORD UINT_MAX

#define IDENTIFIER_NOT_FOUND UINT_MAX

typedef enum {
    AST_VALID,
    AST_INVALID
} AST_Validation;

typedef struct AR_ExpNode AR_ExpNode;

typedef struct {
    const cypher_astnode_t *root;     // Root element of libcypher-parser AST
    // Extensible array of entities described in MATCH, MERGE, and CREATE clausesp
    AR_ExpNode **defined_entities;    // TODO delete if possible
    TrieMap *entity_map;              // Mapping of aliases and AST node pointers to AR_ExpNodes (TODO replace with record IDs)
} AST;

// AST clause validations.
AST_Validation AST_Validate(const AST *ast, char **reason);

// Checks if AST represents a read-only query.
bool AST_ReadOnly(const cypher_astnode_t *root);

// Checks to see if AST contains specified clause. 
bool AST_ContainsClause(const AST *ast, cypher_astnode_type_t clause);

// Checks to see if query contains any errors.
bool AST_ContainsErrors(const cypher_parse_result_t *result);

// Report encountered errors.
char* AST_ReportErrors(const cypher_parse_result_t *result);

// Returns all function (aggregated & none aggregated) mentioned in query.
void AST_ReferredFunctions(const cypher_astnode_t *root, TrieMap *referred_funcs);

// Returns specified clause or NULL.
const cypher_astnode_t* AST_GetClause(const AST *ast, cypher_astnode_type_t clause_type);

uint AST_GetTopLevelClauses(const AST *ast, cypher_astnode_type_t clause_type, const cypher_astnode_t **matches);

uint* AST_GetClauseIndices(const AST *ast, cypher_astnode_type_t clause_type);

uint AST_GetClauseCount(const AST *ast, cypher_astnode_type_t clause_type);

uint AST_NumClauses(const AST *ast);

const cypher_astnode_t** AST_CollectReferencesInRange(const AST *ast, cypher_astnode_type_t type);

const cypher_astnode_t* AST_GetBody(const cypher_parse_result_t *result);

AST* AST_Build(cypher_parse_result_t *parse_result);

AST* AST_NewSegment(AST *master_ast, uint start_offset, uint end_offset);

long AST_ParseIntegerNode(const cypher_astnode_t *int_node);

bool AST_ClauseContainsAggregation(const cypher_astnode_t *clause);

AR_ExpNode** AST_GetOrderExpressions(const cypher_astnode_t *order_clause);

void AST_BuildAliasMap(AST *ast);

// mapping functions

uint AST_GetEntityIDFromReference(const AST *ast, AST_IDENTIFIER entity);

uint AST_GetEntityIDFromAlias(const AST *ast, const char *alias);

uint AST_MapEntity(const AST *ast, AST_IDENTIFIER identifier, uint id);

uint AST_MapAlias(const AST *ast, const char *alias);

// void AST_AssociateAliasWithID(const AST *ast, const char *alias, uint id);

AST* AST_GetFromTLS(void);

void AST_Free(AST *ast);

#endif
