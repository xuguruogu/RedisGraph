/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "ast.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../util/arr.h"
#include "../util/rmalloc.h"
#include "../procedures/procedure.h"
#include "../arithmetic/repository.h"
#include "./ast_arithmetic_expression.h"
#include "../graph/entities/graph_entity.h"
#include "../arithmetic/arithmetic_expression.h"

extern pthread_key_t _tlsASTKey;  // Thread local storage AST key.

static void _AST_MapAliasToID(AST *ast) {
  uint id = 0;  
  ast->_aliasIDMapping = NewTrieMap(); // Holds mapping between referred entities and IDs.
  
  // Get unique aliases, from clauses which can introduce entities.
  TrieMap *referredEntities = NewTrieMap();
  MatchClause_DefinedEntities(ast->matchNode, referredEntities);
  CreateClause_ReferredEntities(ast->createNode, referredEntities);
  UnwindClause_DefinedEntities(ast->unwindNode, referredEntities);
  ReturnClause_DefinedEntities(ast->returnNode, referredEntities);

  void *val;
  char *ptr;
  tm_len_t len;
  TrieMapIterator *it = TrieMap_Iterate(referredEntities, "", 0);
  // Scan through aliases and give each one an ID.
  while(TrieMapIterator_Next(it, &ptr, &len, &val)) {
    uint *entityID = malloc(sizeof(uint));
    *entityID = id++;
    TrieMap_Add(ast->_aliasIDMapping, ptr, len, entityID, TrieMap_NOP_REPLACE);
  }

  TrieMapIterator_Free(it);
  TrieMap_Free(referredEntities, TrieMap_NOP_CB);
}

static AST_Validation _Validate_CALL_Clause(const AST *ast, char **reason) {
  if (!ast->callNode) return AST_VALID;
  // Make sure refereed procedure exists.
  ProcedureCtx *proc = Proc_Get(ast->callNode->procedure);
  if(!proc) {
    asprintf(reason, "There is no procedure with the name `%s` registered for this database instance. Please ensure you've spelled the procedure name correctly.",
             ast->callNode->procedure);
    return AST_INVALID;
  }

  if(proc->argc != PROCEDURE_VARIABLE_ARG_COUNT && proc->argc != array_len(ast->callNode->arguments)) {
    asprintf(reason, "Procedure call does not provide the required number of arguments: got %d expected %d. ",
             array_len(ast->callNode->arguments),
             proc->argc);

    Proc_Free(proc);
    return AST_INVALID;
  }

  // Make sure yield doesn't refers to unknown output.
  if(ast->callNode->yield) {
    uint i = 0;
    uint j = 0;
    char *yield;
    uint call_yield_len = array_len(ast->callNode->yield);
    uint proc_output_len = array_len(proc->output);

    for(i = 0; i < call_yield_len; i++) {
      yield = ast->callNode->yield[i];
      for(j = 0; j < proc_output_len; j++) {
        if(strcmp(yield, proc->output[j]->name) == 0) break;
      }
      if(j == proc_output_len) {
        // Didn't managed to match current yield against procedure output.
        break;
      }
    }

    if(i < call_yield_len) {
      Proc_Free(proc);
      asprintf(reason, "Unknown procedure output: `%s`", yield);
      return AST_INVALID;
    }
  }

  Proc_Free(proc);
  return AST_VALID;
}

AST *AST_New(AST_MatchNode *matchNode, AST_WhereNode *whereNode,
                         AST_CreateNode *createNode, AST_MergeNode *mergeNode,
                         AST_SetNode *setNode, AST_DeleteNode *deleteNode,
                         AST_ReturnNode *returnNode, AST_OrderNode *orderNode,
                         AST_SkipNode *skipNode, AST_LimitNode *limitNode,
                         AST_IndexNode *indexNode, AST_UnwindNode *unwindNode,
                         AST_ProcedureCallNode *callNode) {
  AST *ast = rm_malloc(sizeof(AST));

  ast->matchNode = matchNode;
  ast->whereNode = whereNode;
  ast->createNode = createNode;
  ast->mergeNode = mergeNode;
  ast->setNode = setNode;
  ast->deleteNode = deleteNode;
  ast->returnNode = returnNode;
  ast->orderNode = orderNode;
  ast->skipNode = skipNode;
  ast->limitNode = limitNode;
  ast->indexNode = indexNode;
  ast->unwindNode = unwindNode;
  ast->callNode = callNode;
  ast->withNode = NULL;
  ast->_aliasIDMapping = NULL;
  return ast;
}

AST *AST_GetFromLTS() {
  AST* ast = pthread_getspecific(_tlsASTKey);
  assert(ast);
  return ast;
}

int AST_AliasCount(const AST *ast) {
  assert(ast);
  return ast->_aliasIDMapping->cardinality;
}

int AST_GetAliasID(const AST *ast, char *alias) {
  assert(ast->_aliasIDMapping);
  void *v = TrieMap_Find(ast->_aliasIDMapping, alias, strlen(alias));
  assert(v != TRIEMAP_NOTFOUND);
  int *id = (int*)v;
  return *id;
}

void AST_NameAnonymousNodes(AST *ast) {
  int entity_id = 0;

  if(ast->matchNode)
    MatchClause_NameAnonymousNodes(ast->matchNode, &entity_id);

  if(ast->createNode)
    CreateClause_NameAnonymousNodes(ast->createNode, &entity_id);
  
  if(ast->mergeNode)
    MergeClause_NameAnonymousNodes(ast->mergeNode, &entity_id);
}

void AST_MapAliasToID(AST *ast, AST_WithNode *prevWithClause) {
  uint id = 0;
  uint *entityID = NULL;
  ast->_aliasIDMapping = NewTrieMap(); // Holds mapping between referred entities and IDs.
  
  if(prevWithClause) {
    for(uint i = 0; i < array_len(prevWithClause->exps); i++) {
      entityID = malloc(sizeof(uint));
      *entityID = id++;
      AST_WithElementNode *exp = prevWithClause->exps[i];
      TrieMap_Add(ast->_aliasIDMapping,
                  exp->alias,
                  strlen(exp->alias),
                  entityID,
                  TrieMap_DONT_CARE_REPLACE);
    }
  }

  // Get unique aliases, from clauses which can introduce entities.
  TrieMap *definedEntities = AST_Identifiers(ast);
  void *val;
  char *ptr;
  tm_len_t len;
  TrieMapIterator *it = TrieMap_Iterate(definedEntities, "", 0);
  // Scan through aliases and give each one an ID.
  while(TrieMapIterator_Next(it, &ptr, &len, &val)) {
    entityID = malloc(sizeof(uint));
    *entityID = id++;
    TrieMap_Add(ast->_aliasIDMapping, ptr, len, entityID, TrieMap_DONT_CARE_REPLACE);
  }

  TrieMapIterator_Free(it);
  TrieMap_Free(definedEntities, TrieMap_NOP_CB);
}

TrieMap* AST_CollectEntityReferences(AST **ast_arr) {
  TrieMap *alias_references = NewTrieMap();

  uint ast_count = array_len(ast_arr);
  for (uint i = 0; i < ast_count; i ++) {
      const AST *ast = ast_arr[i];
      // Get unique aliases from clauses that can introduce nodes and edges.
      MatchClause_DefinedEntities(ast->matchNode, alias_references);
      CreateClause_DefinedEntities(ast->createNode, alias_references);
      ProcedureCallClause_DefinedEntities(ast->callNode, alias_references);

      // TODO May need to collect alias redefinitions from WITH clauses
  }
  return alias_references;
}

// Returns a triemap of all identifiers defined by ast.
TrieMap* AST_Identifiers(const AST *ast) {
  TrieMap *identifiers = NewTrieMap();
  MatchClause_DefinedEntities(ast->matchNode, identifiers);
  ReturnClause_DefinedEntities(ast->returnNode, identifiers);
  WithClause_DefinedEntities(ast->withNode, identifiers);
  CreateClause_DefinedEntities(ast->createNode, identifiers);
  UnwindClause_DefinedEntities(ast->unwindNode, identifiers);
  ProcedureCallClause_DefinedEntities(ast->callNode, identifiers);
  return identifiers;
}

bool AST_Projects(const AST *ast) {
  return (ast->returnNode || ast->withNode);
}

bool AST_ReadOnly(AST **ast) {
  for (uint i = 0; i < array_len(ast); i++) {
    bool write = (ast[i]->createNode ||
                  ast[i]->mergeNode ||
                  ast[i]->deleteNode ||
                  ast[i]->setNode ||
                  ast[i]->indexNode);
    if(write) return false;
  }
  return true;
}

void AST_Free(AST **ast) {
  for (uint i = 0; i < array_len(ast); i++) {
    Free_AST_MatchNode(ast[i]->matchNode);
    Free_AST_CreateNode(ast[i]->createNode);
    Free_AST_MergeNode(ast[i]->mergeNode);
    Free_AST_DeleteNode(ast[i]->deleteNode);
    Free_AST_SetNode(ast[i]->setNode);
    Free_AST_WhereNode(ast[i]->whereNode);
    Free_AST_ReturnNode(ast[i]->returnNode);
    Free_AST_SkipNode(ast[i]->skipNode);
    Free_AST_OrderNode(ast[i]->orderNode);
    Free_AST_UnwindNode(ast[i]->unwindNode);
    Free_AST_LimitNode(ast[i]->limitNode);
    Free_AST_ProcedureCallNode(ast[i]->callNode);
    Free_AST_WithNode(ast[i]->withNode);

    if(ast[i]->_aliasIDMapping) TrieMap_Free(ast[i]->_aliasIDMapping, NULL);
    rm_free(ast[i]);
  }
  array_free(ast);
}
