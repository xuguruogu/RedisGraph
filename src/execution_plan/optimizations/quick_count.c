/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Apache License, Version 2.0,
* modified with the Commons Clause restriction.
*/

#include "quick_count.h"
#include "../ops/op_aggregate.h"
#include "../ops/op_conditional_traverse.h"

// static int _aggregateFollowedByTraverse(OpBase *root) {
//     if(root)
// }

void skipCounting(ExecutionPlan *plan, AST_Query *ast) {
    // Scan for aggregate operation followed by traverse operation.
    if(plan->root->type == OPType_AGGREGATE) {
        if(plan->root->childCount == 1 && plan->root->children[0]->type == OPType_CONDITIONAL_TRAVERSE) {
            const AST_ReturnNode *return_node = ast->returnNode;
            
            CondTraverse *traverse = (CondTraverse *)plan->root->children[0];
            char *destNodeAlias = traverse->algebraic_expression->dest_node->alias;

            // We're only interested in a single return element.
            if(Vector_Size(return_node->returnElements) != 1) {
                return;
            }

            AST_ReturnElementNode *returnElement;
            Vector_Get(return_node->returnElements, 0, &returnElement);

            AST_ArithmeticExpressionNode *exp = returnElement->exp;
            if(exp->type != AST_AR_EXP_OP) {
                return;
            }

            if (strcasecmp(exp->op.function, "count")) {
                return;
            }

            if(Vector_Size(exp->op.args) != 1) {
                return;
            }

            AST_ArithmeticExpressionNode *arg;
            Vector_Get(exp->op.args, 0, &arg);
            
            if(arg->type != AST_AR_EXP_OPERAND) {
                return;
            }
            
            if(arg->operand.type != AST_AR_EXP_VARIADIC) {
                return;
            }
            
            if(strcmp(arg->operand.variadic.alias, destNodeAlias)) {
                return;
            }

            // We're counting Conditional traverse destination node
            // Switch to quick counting!

            // Traverse dose not need to iterate.
            traverse->iterate = false;

            // Swap aggregate clause count function with matrix count.
            exp->op.function = "matCount";
        }
    }
}
