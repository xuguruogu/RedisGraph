/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef __QUERY_EXECUTOR_H
#define __QUERY_EXECUTOR_H

#include "redismodule.h"
#include "parser/ast.h"
#include "parser/newast.h"
#include "graph/query_graph.h"
#include "arithmetic/arithmetic_expression.h"

/* Create an AST from raw query. */
AST **ParseQuery(const char *query, size_t qLen, char **errMsg);

/* Make sure AST is valid. */
AST_Validation AST_PerformValidations(RedisModuleCtx *ctx, AST **ast);

/* Performs a number of adjustments to given AST. */
<<<<<<< HEAD
void ModifyAST(AST **ast);
=======
void ModifyAST(GraphContext *gc, AST *ast, const cypher_parse_result_t *new_ast);
>>>>>>> modified parser buffer size to handle large queries, flowtest will try to connect to a local redis before instantiating their own disposable redis server, need to make sure that while flow tests run redis would not try to save/load RDB.

#endif
