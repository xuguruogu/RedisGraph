/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include <assert.h>

#include "execution_plan.h"
#include "./ops/ops.h"
#include "../util/arr.h"
#include "../util/vector.h"
#include "../query_executor.h"
#include "../graph/entities/edge.h"
#include "./optimizations/optimizer.h"
#include "./optimizations/optimizations.h"
#include "../arithmetic/algebraic_expression.h"

/* Checks if parent has given child, if so returns 1
 * otherwise returns 0 */
int _OpBase_ContainsChild(const OpBase *parent, const OpBase *child) {
    for(int i = 0; i < parent->childCount; i++) {
        if(parent->children[i] == child) {
            return 1;
        }
    }
    return 0;
}

void _OpBase_AddChild(OpBase *parent, OpBase *child) {
    // Add child to parent
    if(parent->children == NULL) {
        parent->children = malloc(sizeof(OpBase *));
    } else {
        parent->children = realloc(parent->children, sizeof(OpBase *) * (parent->childCount+1));
    }
    parent->children[parent->childCount++] = child;

    // Add parent to child
    child->parent = parent;
}

/* Removes node b from a and update child parent lists
 * Assuming B is a child of A. */
void _OpBase_RemoveNode(OpBase *parent, OpBase *child) {
    // Remove child from parent.
    int i = 0;
    for(; i < parent->childCount; i++) {
        if(parent->children[i] == child) break;
    }
    
    assert(i != parent->childCount);    

    // Uppdate child count.
    parent->childCount--;
    if(parent->childCount == 0) {
        free(parent->children);
        parent->children = NULL;
    } else {
        // Shift left children.
        for(int j = i; j < parent->childCount; j++) {
            parent->children[j] = parent->children[j+1];
        }
        parent->children = realloc(parent->children, sizeof(OpBase *) * parent->childCount);
    }

    // Remove parent from child.
    child->parent = NULL;
}

void _OpBase_RemoveChild(OpBase *parent, OpBase *child) {
    _OpBase_RemoveNode(parent, child);
}

Vector* _ExecutionPlan_Locate_References(OpBase *root, OpBase **op, Vector *references) {
    /* List of entities which had their ID resolved
     * at this point of execution, should include all
     * previously modified entities (up the execution plan). */
    Vector *seen = NewVector(char*, 0);
    char *modifiedEntity;

    /* Append current op modified entities. */
    if(root->modifies) {
        for(int i = 0; i < Vector_Size(root->modifies); i++) {
            Vector_Get(root->modifies, i, &modifiedEntity);
            Vector_Push(seen, modifiedEntity);
        }
    }

     /* Traverse execution plan, upwards. */
    for(int i = 0; i < root->childCount; i++) {
        Vector *saw = _ExecutionPlan_Locate_References(root->children[i], op, references);

        /* Quick return if op was located. */
        if(*op) {
            Vector_Free(saw);
            return seen;
        }

        /* Add modified entities from previous operation. */
        for(int i = 0; i < Vector_Size(saw); i++) {
            Vector_Get(saw, i, &modifiedEntity);
            Vector_Push(seen, modifiedEntity);
        }
        Vector_Free(saw);
    }

    /* See if all references been resolved.
     * TODO: create a vector compare function
     * which checks if the content of one is in the other. */
    int match = Vector_Size(references);
    size_t ref_count = Vector_Size(references);
    size_t seen_count = Vector_Size(seen);

    for(int i = 0; i < ref_count; i++) {
        char *reference;        
        Vector_Get(references, i, &reference);

        int j = 0;
        for(; j < seen_count; j++) {
            char *resolved;
            Vector_Get(seen, j, &resolved);
            if(!strcmp(reference, resolved)) {
                /* Match! */
                break;
            }
        }
        
        /* no match, quick break, */
        if (j == seen_count) break;
        else match--;
    }

    if(!match) *op = root;
    return seen;
}

// TODO
// void _Count_Graph_Entities(const Vector *entities, size_t *node_count, size_t *edge_count) {
    // for(int i = 0; i < Vector_Size(entities); i++) {
        // AST_GraphEntity *entity;
        // Vector_Get(entities, i, &entity);

        // if(entity->t == N_ENTITY) {
            // (*node_count)++;
        // } else if(entity->t == N_LINK) {
            // (*edge_count)++;
        // }
    // }
// }

// TODO is this necessary?
// void _Determine_Graph_Size(const AST *ast, size_t *node_count, size_t *edge_count) {
    // *edge_count = 0;
    // *node_count = 0;
    // Vector *entities;
    
    // if(ast->matchNode) {
        // entities = ast->matchNode->_mergedPatterns;
        // _Count_Graph_Entities(entities, node_count, edge_count);
    // }

    // if(ast->createNode) {
        // entities = ast->createNode->graphEntities;
        // _Count_Graph_Entities(entities, node_count, edge_count);
    // }
// }

void ExecutionPlan_AddOp(OpBase *parent, OpBase *newOp) {
    _OpBase_AddChild(parent, newOp);
}

void ExecutionPlan_PushBelow(OpBase *a, OpBase *b) {
    /* B is a new operation. */
    assert(!(b->parent || b->children));
    assert(a->parent);

    /* Replace A's former parent. */
    OpBase *a_former_parent = a->parent;

    /* Disconnect A from its former parent. */
    _OpBase_RemoveChild(a_former_parent, a);

    /* Add A's former parent as parent of B. */
    _OpBase_AddChild(a_former_parent, b);

    /* Add A as a child of B. */
    _OpBase_AddChild(b, a);
}

void ExecutionPlan_ReplaceOp(ExecutionPlan *plan, OpBase *a, OpBase *b) {
    // Insert the new operation between the original and its parent.
    ExecutionPlan_PushBelow(a, b);
    // Delete the original operation.
    ExecutionPlan_RemoveOp(plan, a);
}

void ExecutionPlan_RemoveOp(ExecutionPlan *plan, OpBase *op) {
    if(op->parent == NULL) {
        // Removing execution plan root.
        assert(op->childCount == 1);
        plan->root = op->children[0];
    } else {
        // Remove op from its parent.
        OpBase* parent = op->parent;
        _OpBase_RemoveChild(op->parent, op);

        // Add each of op's children as a child of op's parent.
        for(int i = 0; i < op->childCount; i++) {
            _OpBase_AddChild(parent, op->children[i]);
        }
    }

    // Clear op.
    op->parent = NULL;
    free(op->children);
    op->children = NULL;
    op->childCount = 0;
}

static bool inline _TapOperation(const OpBase *op) {
    return (op->type == OPType_ALL_NODE_SCAN ||
       op->type == OPType_NODE_BY_LABEL_SCAN ||
       op->type == OPType_INDEX_SCAN ||
       op->type == OPType_CREATE ||
       op->type == OPType_UNWIND ||
       op->type == OPType_PROC_CALL);
}

void ExecutionPlan_LocateTaps(OpBase *root, OpBase ***taps) {
    if(root == NULL) return;
    if(_TapOperation(root)) *taps = array_append(*taps, root);
    for(int i = 0; i < root->childCount; i++) {
        ExecutionPlan_LocateTaps(root->children[i], taps);
    }
}

OpBase* ExecutionPlan_LocateOp(OpBase *root, OPType type) {
    if(!root) return NULL;

    if(root->type == type) {
        return root;
    }

    for(int i = 0; i < root->childCount; i++) {
        OpBase *op = ExecutionPlan_LocateOp(root->children[i], type);
        if(op) return op;
    }

    return NULL;
}

void ExecutionPlan_Taps(OpBase *root, OpBase ***taps) {
    if(root == NULL) return;
    if(root->type & OP_SCAN) *taps = array_append(*taps, root);

    for(int i = 0; i < root->childCount; i++) {
        ExecutionPlan_Taps(root->children[i], taps);
    }
}

OpBase* ExecutionPlan_Locate_References(OpBase *root, Vector *references) {
    OpBase *op = NULL;    
    Vector *temp = _ExecutionPlan_Locate_References(root, &op, references);
    Vector_Free(temp);
    return op;
}

/* Given an AST path, construct a series of scans and traversals to model it. */
void _ExecutionPlan_BuildTraversalOps(QueryGraph *qg, FT_FilterNode *ft, const cypher_astnode_t *path, Vector *traversals) {
    GraphContext *gc = GraphContext_GetFromTLS();
    NEWAST *ast = NEWAST_GetFromTLS();
    OpBase *op;

    uint nelems = cypher_ast_pattern_path_nelements(path);
    if (nelems == 1) {
        // Only one entity is specified - build a node scan.
        const cypher_astnode_t *ast_node = cypher_ast_pattern_path_get_element(path, 0);
        AR_ExpNode *ar_exp = NEWAST_GetEntity(ast, ast_node);
        if (ar_exp->record_idx == NOT_IN_RECORD) {
            // Anonymous node - make space for it in the Record
            ar_exp->record_idx = NEWAST_AddAnonymousRecordEntry(ast);
        }
        uint rec_idx = ar_exp->record_idx; 
        Node *n = QueryGraph_GetEntityByASTRef(qg, ast_node);
        if(cypher_ast_node_pattern_nlabels(ast_node) > 0) {
            op = NewNodeByLabelScanOp(gc, n, rec_idx);
        } else {
            op = NewAllNodeScanOp(gc->g, n, rec_idx);
        }
        Vector_Push(traversals, op);
        return;
    }

    // This path must be expressed with one or more traversals.
    size_t expCount = 0;
    AlgebraicExpression **exps = AlgebraicExpression_FromPath(ast, qg, path, &expCount);

    TRAVERSE_ORDER order = determineTraverseOrder(ft, exps, expCount);
    if(order == TRAVERSE_ORDER_FIRST) {
        AlgebraicExpression *exp = exps[0];
        selectEntryPoint(exp, ft);

        if (exp->src_node_idx == NOT_IN_RECORD) {
            // Anonymous node - make space for it in the Record
            exp->src_node_idx = NEWAST_AddAnonymousRecordEntry(ast);
        }

        // Create SCAN operation.
        if(exp->src_node->label) {
            /* There's no longer need for the last matrix operand
             * as it's been replaced by label scan. */
            AlgebraicExpression_RemoveTerm(exp, exp->operand_count-1, NULL);
            op = NewNodeByLabelScanOp(gc, exp->src_node, exp->src_node_idx);
            Vector_Push(traversals, op);
        } else {
            op = NewAllNodeScanOp(gc->g, exp->src_node, exp->src_node_idx);
            Vector_Push(traversals, op);
        }
        for(int i = 0; i < expCount; i++) {
            if(exps[i]->operand_count == 0) continue;
            if(exps[i]->minHops != 1 || exps[i]->maxHops != 1) {
                op = NewCondVarLenTraverseOp(exps[i],
                                             exps[i]->minHops,
                                             exps[i]->maxHops,
                                             gc->g);
            } else {
                op = NewCondTraverseOp(gc->g, exps[i]);
            }
            Vector_Push(traversals, op);
        }
    } else {
        AlgebraicExpression *exp = exps[expCount-1];
        selectEntryPoint(exp, ft);

        if (exp->dest_node_idx == NOT_IN_RECORD) {
            // Anonymous node - make space for it in the Record
            exp->dest_node_idx = NEWAST_AddAnonymousRecordEntry(ast);
        }

        // Create SCAN operation.
        if(exp->dest_node->label) {
            /* There's no longer need for the last matrix operand
             * as it's been replaced by label scan. */
            AlgebraicExpression_RemoveTerm(exp, exp->operand_count-1, NULL);
            op = NewNodeByLabelScanOp(gc, exp->dest_node, exp->dest_node_idx);
            Vector_Push(traversals, op);
        } else {
            op = NewAllNodeScanOp(gc->g, exp->dest_node, exp->dest_node_idx);
            Vector_Push(traversals, op);
        }

        for(int i = expCount-1; i >= 0; i--) {
            if(exps[i]->operand_count == 0) continue;
            AlgebraicExpression_Transpose(exps[i]);
            if(exps[i]->minHops != 1 || exps[i]->maxHops != 1) {
                op = NewCondVarLenTraverseOp(exps[i],
                                             exps[i]->minHops,
                                             exps[i]->maxHops,
                                             gc->g);
            }
            else {
                op = NewCondTraverseOp(gc->g, exps[i]);
            }
            Vector_Push(traversals, op);
        }
    }
    // Free the expressions array, as its parts have been converted into operations
    free(exps);
}

void _ExecutionPlan_AddTraversalOps(Vector *ops, OpBase *cartesian_root, Vector *traversals) {
    if(cartesian_root) {
        // If we're traversing multiple disjoint paths, the new traversal
        // should be connected uner a Cartesian product.
        OpBase *childOp;
        OpBase *parentOp;
        Vector_Pop(traversals, &parentOp);
        // Connect cartesian product to the root of traversal.
        _OpBase_AddChild(cartesian_root, parentOp);
        while(Vector_Pop(traversals, &childOp)) {
            _OpBase_AddChild(parentOp, childOp);
            parentOp = childOp;
        }
    } else {
        // Otherwise, the traversals can be added sequentially to the overall ops chain
        OpBase *op;
        for(int traversalIdx = 0; traversalIdx < Vector_Size(traversals); traversalIdx++) {
            Vector_Get(traversals, traversalIdx, &op);
            Vector_Push(ops, op);
        }
    }
}

// TODO I don't like the way this function is separate from its caller right now
// (this had been the setup to handle multiple ASTs). Depending on how the WITH clause
// implementation works, consider folding back into caller.
ExecutionPlan* _NewExecutionPlan(RedisModuleCtx *ctx, ResultSet *result_set) {
    ExecutionPlan *execution_plan = (ExecutionPlan*)calloc(1, sizeof(ExecutionPlan));    
    execution_plan->result_set = result_set;
    Vector *ops = NewVector(OpBase*, 1);

    GraphContext *gc = GraphContext_GetFromTLS();
    NEWAST *ast = NEWAST_GetFromTLS();

    // Build query graph
    QueryGraph *qg = BuildQueryGraph(gc, ast);
    execution_plan->query_graph = qg;

    // Build filter tree
    FT_FilterNode *filter_tree = BuildFiltersTree(ast);
    execution_plan->filter_tree = filter_tree;

    if(ast->callNode) {        
        OpBase *opProcCall = NewProcCallOp(ast->callNode->procedure, ast->callNode->arguments, ast->callNode->yield, ast);
        Vector_Push(ops, opProcCall);
    }

    unsigned int clause_count = cypher_astnode_nchildren(ast->root);
    const cypher_astnode_t *match_clauses[clause_count];
    unsigned int match_count = NewAST_GetTopLevelClauses(ast->root, CYPHER_AST_MATCH, match_clauses);
    // Build traversal operations for every MATCH clause
    for (uint i = 0; i < match_count; i ++) {
        // Each MATCH clause has a pattern that consists of 1 or more paths
        const cypher_astnode_t *ast_pattern = cypher_ast_match_get_pattern(match_clauses[i]);
        uint npaths = cypher_ast_pattern_npaths(ast_pattern);

        /* If we're dealing with multiple paths (which our validations have guaranteed
         * are disjoint), we'll join them all together with a Cartesian product (full join). */
        OpBase *cartesianProduct = NULL;
        if (cypher_ast_pattern_npaths(ast_pattern) > 1) {
            cartesianProduct = NewCartesianProductOp(NEWAST_RecordLength(ast));
            Vector_Push(ops, cartesianProduct);
        }
        
        Vector *path_traversal = NewVector(OpBase*, 1);
        for (uint j = 0; j < npaths; j ++) {
            // Convert each path into the appropriate traversal operation(s).
            const cypher_astnode_t *path = cypher_ast_pattern_get_path(ast_pattern, j);
            _ExecutionPlan_BuildTraversalOps(qg, filter_tree, path, path_traversal);
            _ExecutionPlan_AddTraversalOps(ops, cartesianProduct, path_traversal);
            Vector_Clear(path_traversal);
        }
        Vector_Free(path_traversal);
    }

    // Set root operation
    const cypher_astnode_t *unwind_clause = NEWAST_GetClause(ast->root, CYPHER_AST_UNWIND);
    if(unwind_clause) {
        OpBase *opUnwind = NewUnwindOp(ast, unwind_clause);
        Vector_Push(ops, opUnwind);
    }

    const cypher_astnode_t *create_clause = NEWAST_GetClause(ast->root, CYPHER_AST_CREATE);
    if(create_clause) {
        OpBase *opCreate = NewCreateOp(ctx, qg, execution_plan->result_set);
        Vector_Push(ops, opCreate);
    }

    const cypher_astnode_t *merge_clause = NEWAST_GetClause(ast->root, CYPHER_AST_MERGE);
    if(merge_clause) {
        // A merge clause provides a single path that must exist or be created.
        // As with paths in a MATCH query, build the appropriate traversal operations
        // and append them to the set of ops.
        const cypher_astnode_t *path = cypher_ast_merge_get_pattern_path(merge_clause);
        Vector *path_traversal = NewVector(OpBase*, 1);
        _ExecutionPlan_BuildTraversalOps(qg, filter_tree, path, path_traversal);
        _ExecutionPlan_AddTraversalOps(ops, NULL, path_traversal);
        Vector_Free(path_traversal);

        // Append a merge operation
        OpBase *opMerge = NewMergeOp(gc, merge_clause, execution_plan->result_set);
        Vector_Push(ops, opMerge);
    }

    const cypher_astnode_t *delete_clause = NEWAST_GetClause(ast->root, CYPHER_AST_DELETE);
    if(delete_clause) {
        OpBase *opDelete = NewDeleteOp(delete_clause, execution_plan->result_set);
        Vector_Push(ops, opDelete);
    }

    const cypher_astnode_t *set_clause = NEWAST_GetClause(ast->root, CYPHER_AST_SET);
    if(set_clause) {
        OpBase *op_update = NewUpdateOp(gc, execution_plan->result_set);
        Vector_Push(ops, op_update);
    }

    char **aliases = NULL;  // Array of aliases RETURN n.v as V
    AR_ExpNode **exps = NULL;
    bool aggregate = false;

    // TODO with clauses, separate handling for their distinct/limit/etc
    const cypher_astnode_t *with_clause = NEWAST_GetClause(ast->root, CYPHER_AST_WITH);
    if(with_clause) {
        assert(false);
    }

    const cypher_astnode_t *ret_clause = NEWAST_GetClause(ast->root, CYPHER_AST_RETURN);
    if(ret_clause) {
        uint exp_count = array_len(ast->return_expressions);
        // TODO exps and aliases just separate the elements of ast->return_expressions,
        // which is dumb - change signatures to take ReturnElementNodes

        exps = array_new(AR_ExpNode*, exp_count);
        aliases = array_new(char*, exp_count);
        for (uint i = 0; i < exp_count; i ++) {
            exps = array_append(exps, ast->return_expressions[i]->exp);
            aliases = array_append(aliases, (char*)ast->return_expressions[i]->alias);
        }
        // TODO We've already determined this during AST validations, could refactor to make
        // this call unnecessary.
        aggregate = NEWAST_ClauseContainsAggregation(ret_clause);
    }


    OpBase *op;
    if(ret_clause || with_clause) {
        if(aggregate) op = NewAggregateOp(exps, aliases);
        else op = NewProjectOp(exps, aliases);
        Vector_Push(ops, op);
    }

    
    if (ret_clause) {
        if (cypher_ast_return_is_distinct(ret_clause)) {
            op = NewDistinctOp();
            Vector_Push(ops, op);
        }

        const cypher_astnode_t *order_clause = cypher_ast_return_get_order_by(ret_clause);
        const cypher_astnode_t *skip_clause = cypher_ast_return_get_skip(ret_clause);
        const cypher_astnode_t *limit_clause = cypher_ast_return_get_limit(ret_clause);

        uint skip = 0;
        uint limit = 0;
        if (skip_clause) skip = NEWAST_ParseIntegerNode(skip_clause);
        if (limit_clause) limit = skip + NEWAST_ParseIntegerNode(limit_clause);

        if (order_clause) {
            op = NewSortOp(order_clause, limit);
            Vector_Push(ops, op);
        }

        if (skip_clause) {
            OpBase *op_skip = NewSkipOp(skip);
            Vector_Push(ops, op_skip);
        }

        if (limit_clause) {
            OpBase *op_limit = NewLimitOp(limit);
            Vector_Push(ops, op_limit);
        }

        op = NewResultsOp(execution_plan->result_set, qg);
        Vector_Push(ops, op);
    }

    OpBase *parent_op;
    OpBase *child_op;
    Vector_Pop(ops, &parent_op);
    execution_plan->root = parent_op;

    while(Vector_Pop(ops, &child_op)) {
        _OpBase_AddChild(parent_op, child_op);
        parent_op = child_op;
    }

    Vector_Free(ops);
    return execution_plan;
}

// Locates all "taps" (entry points) of root.
static void _ExecutionPlan_StreamTaps(OpBase *root, OpBase ***taps) {    
    if(root->childCount) {
        for(int i = 0; i < root->childCount; i++) {
            OpBase *child = root->children[i];
            _ExecutionPlan_StreamTaps(child, taps);
        }
    } else {
        *taps = array_append(*taps, root);
    }
}

static ExecutionPlan *_ExecutionPlan_Connect(ExecutionPlan *a, ExecutionPlan *b) {
    assert(false);
    NEWAST *ast = NEWAST_GetFromTLS();
    assert(a &&
           b &&
           (a->root->type == OPType_PROJECT || a->root->type == OPType_AGGREGATE));

    OpBase *tap;
    OpBase **taps = array_new(OpBase*, 1);
    _ExecutionPlan_StreamTaps(b->root, &taps);

    unsigned short tap_count = array_len(taps);
    if(tap_count == 1 && !(taps[0]->type & OP_SCAN)) {
        /* Single tap, entry point isn't a SCAN operation, e.g.
         * MATCH (b) WITH b.v AS V RETURN V
         * MATCH (b) WITH b.v+1 AS V CREATE (n {v:V}) */
        _OpBase_AddChild(taps[0], a->root);
    } else {
        /* Multiple taps or a single SCAN tap, e.g. 
         * MATCH (b) WITH b.v AS V MATCH (c) return V,c
         * MATCH (b) WITH b.v AS V MATCH (c),(d) return c, V, d */
        for(int i = 0; i < tap_count; i++) {
            tap = taps[i];
            if(tap->type & OP_SCAN) {
                // Connect via cartesian product
                OpBase *cartesianProduct = NewCartesianProductOp(NEWAST_RecordLength(ast));
                ExecutionPlan_PushBelow(tap, cartesianProduct);
                _OpBase_AddChild(cartesianProduct, a->root);
                break;
            }
        }
    }

    array_free(taps);
    // null root to avoid freeing connected operations.
    a->root = NULL;
    // null query graph to avoid freeing entities referenced in both graphs
    // TODO memory leak on entities that exclusively appear in this graph
    free(a->query_graph);
    a->query_graph = NULL;
    ExecutionPlanFree(a);
    return b;
}

ExecutionPlan* NewExecutionPlan(RedisModuleCtx *ctx, GraphContext *gc, bool explain) {
    NEWAST *ast = NEWAST_GetFromTLS();

    ExecutionPlan *plan = NULL;
    ExecutionPlan *curr_plan;

    ResultSet *result_set = NULL;
    if(!explain) {
        result_set = NewResultSet(ast, ctx);
        ResultSet_CreateHeader(result_set);
    }

    // for(unsigned int i = 0; i < array_len(ast); i++) {
    for(unsigned int i = 0; i < 1; i++) { // TODO WITH
        curr_plan = _NewExecutionPlan(ctx, result_set);
        if(i == 0) plan = curr_plan;
        else plan = _ExecutionPlan_Connect(plan, curr_plan);
        // else plan = _ExecutionPlan_Connect(plan, curr_plan, ast[i]);

        if(curr_plan->filter_tree) {
            Vector *sub_trees = FilterTree_SubTrees(curr_plan->filter_tree);

            /* For each filter tree find the earliest position along the execution
             * after which the filter tree can be applied. */
            for(int i = 0; i < Vector_Size(sub_trees); i++) {
                FT_FilterNode *tree;
                Vector_Get(sub_trees, j, &tree);

                Vector *references = FilterTree_CollectAliases(tree);

                /* Scan execution plan, locate the earliest position where all
                 * references been resolved. */
                OpBase *op = ExecutionPlan_Locate_References(curr_plan->root, references);
                assert(op);

                /* Create filter node.
                 * Introduce filter op right below located op. */
                OpBase *filter_op = NewFilterOp(tree);
                ExecutionPlan_PushBelow(op, filter_op);
                for(int k = 0; k < Vector_Size(references); k++) {
                    char *ref;
                    Vector_Get(references, k, &ref);
                    free(ref);
                }
                Vector_Free(references);
            }
            Vector_Free(sub_trees);
        }
        optimizePlan(gc, plan);
    }

    return plan;
}

void _ExecutionPlanPrint(const OpBase *op, char **strPlan, int ident) {
    char strOp[512] = {0};
    sprintf(strOp, "%*s%s\n", ident, "", op->name);
    
    if(*strPlan == NULL) {
        *strPlan = calloc(strlen(strOp) + 1, sizeof(char));
    } else {
        *strPlan = realloc(*strPlan, sizeof(char) * (strlen(*strPlan) + strlen(strOp) + 2));
    }
    strcat(*strPlan, strOp);

    for(int i = 0; i < op->childCount; i++) {
        _ExecutionPlanPrint(op->children[i], strPlan, ident + 4);
    }
}

char* ExecutionPlanPrint(const ExecutionPlan *plan) {
    char *strPlan = NULL;
    _ExecutionPlanPrint(plan->root, &strPlan, 0);
    return strPlan;
}

void _ExecutionPlanInit(OpBase *root) {
    if(root->init) root->init(root);
    for(int i = 0; i < root->childCount; i++) {
        _ExecutionPlanInit(root->children[i]);
    }
}

void ExecutionPlanInit(ExecutionPlan *plan) {
    if(!plan) return;
    _ExecutionPlanInit(plan->root);
}

ResultSet* ExecutionPlan_Execute(ExecutionPlan *plan) {
    Record r;
    OpBase *op = plan->root;
    
    ExecutionPlanInit(plan);
    while((r = op->consume(op)) != NULL) Record_Free(r);
    return plan->result_set;
}

void _ExecutionPlanFreeRecursive(OpBase* op) {
    for(int i = 0; i < op->childCount; i++) {
        _ExecutionPlanFreeRecursive(op->children[i]);
    }
    OpBase_Free(op);
}

void ExecutionPlanFree(ExecutionPlan *plan) {
    if(plan == NULL) return;
    if(plan->root) _ExecutionPlanFreeRecursive(plan->root);

    QueryGraph_Free(plan->query_graph);
    free(plan);
}
