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

uint AST_GetEntityIDFromReference(const AST *ast, AST_IDENTIFIER entity) {
    uint *id = TrieMap_Find(ast->entity_map, (char*)&entity, sizeof(entity));
    if (id == TRIEMAP_NOTFOUND) return IDENTIFIER_NOT_FOUND;
    return *id;
}

uint AST_GetEntityIDFromAlias(const AST *ast, const char *alias) {
    void *v = TrieMap_Find(ast->entity_map, (char*)alias, strlen(alias));
    if (v == TRIEMAP_NOTFOUND) return NOT_IN_RECORD;
    return *(uint*)v;
}

uint AST_MapEntity(const AST *ast, AST_IDENTIFIER identifier, uint id) {
    if (id == IDENTIFIER_NOT_FOUND) {
        id = ast->entity_map->cardinality;
    }
    // TODO somewhat wasteful, do we need to allocate values?
    uint *id_ptr = rm_malloc(sizeof(uint));
    *id_ptr = id;
    TrieMap_Add(ast->entity_map, (char*)&identifier, sizeof(identifier), id_ptr, TrieMap_DONT_CARE_REPLACE);

    return *id_ptr;
}

// Add alias if it has not already been mapped and return ID
uint AST_MapAlias(const AST *ast, const char *alias) {
    uint *v = TrieMap_Find(ast->entity_map, (char*)alias, strlen(alias));
    if (v != TRIEMAP_NOTFOUND) return *v;

    uint *id_ptr = rm_malloc(sizeof(uint));
    *id_ptr = ast->entity_map->cardinality;
    TrieMap_Add(ast->entity_map, (char*)alias, strlen(alias), id_ptr, TrieMap_DONT_CARE_REPLACE);

    return *id_ptr;
}

// void AST_AssociateAliasWithID(const AST *ast, const char *alias, uint id) {
    // uint *id_ptr = rm_malloc(sizeof(uint));
    // *id_ptr = id;
    // TrieMap_Add(ast->entity_map, (char*)alias, strlen(alias), id_ptr, TrieMap_DONT_CARE_REPLACE);
// }

