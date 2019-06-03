/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "ast.h"
#include "../util/arr.h"
#include "../util/rmalloc.h"
#include "../arithmetic/arithmetic_expression.h"
#include <assert.h>

uint AST_GetAliasID(const AST *ast, const char *alias) {
    AR_ExpNode *exp = TrieMap_Find(ast->entity_map, (char*)alias, strlen(alias));
    return exp->operand.variadic.record_idx;
}

uint AST_MapEntity(const AST *ast, AST_IDENTIFIER identifier) {
    uint *id_ptr = rm_malloc(sizeof(uint));
    *id_ptr = ast->entity_map->cardinality;
    TrieMap_Add(ast->entity_map, (char*)&identifier, sizeof(identifier), id_ptr, TrieMap_DONT_CARE_REPLACE);
    return *id_ptr;
}

uint AST_MapAlias(const AST *ast, const char *alias) {
    uint *id_ptr = rm_malloc(sizeof(uint));
    *id_ptr = ast->entity_map->cardinality;
    TrieMap_Add(ast->entity_map, (char*)alias, strlen(alias), id_ptr, TrieMap_DONT_CARE_REPLACE);
    return *id_ptr;
}

void AST_AssociateAliasWithID(const AST *ast, const char *alias, uint id) {
    uint *id_ptr = rm_malloc(sizeof(uint));
    *id_ptr = id;
    TrieMap_Add(ast->entity_map, (char*)alias, strlen(alias), id_ptr, TrieMap_DONT_CARE_REPLACE);
}

uint AST_GetEntity(const AST *ast, AST_IDENTIFIER entity) {
    AR_ExpNode *v = TrieMap_Find(ast->entity_map, (char*)&entity, sizeof(entity));
    if (v == TRIEMAP_NOTFOUND) return NOT_IN_RECORD;
    return *(uint*)v;
}

uint AST_GetEntityFromAlias(const AST *ast, const char *alias) {
    void *v = TrieMap_Find(ast->entity_map, (char*)alias, strlen(alias));
    if (v == TRIEMAP_NOTFOUND) return NOT_IN_RECORD;
    return *(uint*)v;
}

// TODO dup
uint AST_GetEntityRecordIdx(const AST *ast, const cypher_astnode_t *entity) {
    return AST_GetEntity(ast, entity);
}

uint AST_RecordLength(const AST *ast) {
    return ast->record_length;
}

uint AST_AddRecordEntry(AST *ast) {
    // Increment Record length and return a valid record index
    return ast->record_length ++;
}

void AST_RecordAccommodateExpression(AST *ast, AR_ExpNode *exp) {
    // Do nothing if expression already has a Record index
    if (exp->record_idx != NOT_IN_RECORD) return;

    // Register a new Record index
    exp->record_idx = ast->record_length ++;

    // Add entity to the set of entities to be populated
    ast->defined_entities = array_append(ast->defined_entities, exp);
}

uint AST_AddAnonymousRecordEntry(AST *ast) {
    uint id = ast->record_length ++;
    
    AR_ExpNode *exp = AR_EXP_NewAnonymousEntity(id);
    ast->defined_entities = array_append(ast->defined_entities, exp);

    return id;
}
