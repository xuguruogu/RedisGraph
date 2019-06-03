/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include <assert.h>

#include "execution_plan.h"
#include "./ops/ops.h"
#include "../util/rmalloc.h"
#include "../util/arr.h"
#include "../util/vector.h"
#include "../query_executor.h"
#include "../graph/entities/edge.h"
#include "./optimizations/optimizer.h"
#include "./optimizations/optimizations.h"
#include "../arithmetic/algebraic_expression.h"
#include "../ast/ast_build_op_contexts.h"
#include "../ast/ast_build_filter_tree.h"

static ResultSet* _prepare_resultset(RedisModuleCtx *ctx, AST *ast, bool compact) {
    const cypher_astnode_t *ret_clause = AST_GetClause(ast, CYPHER_AST_RETURN);
    bool distinct = false;
    if (ret_clause) {
        distinct = cypher_ast_return_is_distinct(ret_clause);
    }
    ResultSet *set = NewResultSet(ctx, distinct, compact);
    return set;
}

/* Given an AST path, construct a series of scans and traversals to model it. */
void _ExecutionPlanSegment_BuildTraversalOps(QueryGraph *qg, FT_FilterNode *ft, const cypher_astnode_t *path, Vector *traversals) {
    GraphContext *gc = GraphContext_GetFromTLS();
    AST *ast = AST_GetFromTLS();
    OpBase *op = NULL;

    uint nelems = cypher_ast_pattern_path_nelements(path);
    if (nelems == 1) {
        // Only one entity is specified - build a node scan.
        const cypher_astnode_t *ast_node = cypher_ast_pattern_path_get_element(path, 0);
        uint rec_idx = AST_GetEntityRecordIdx(ast, ast_node);
        Node *n = QueryGraph_GetEntityByASTRef(qg, ast_node);
        if(cypher_ast_node_pattern_nlabels(ast_node) > 0) {
            op = NewNodeByLabelScanOp(n, rec_idx);
        } else {
            op = NewAllNodeScanOp(gc->g, n, rec_idx);
        }
        Vector_Push(traversals, op);
        return;
    }

    // This path must be expressed with one or more traversals.
    size_t expCount = 0;
    AlgebraicExpression **exps = AlgebraicExpression_FromPath(ast, qg, path, &expCount);

    TRAVERSE_ORDER order;
    if (exps[0]->op == AL_EXP_UNARY) {
        // If either the first or last expression simply specifies a node, it should
        // be replaced by a label scan. (This can be the case after building a
        // variable-length traversal like MATCH (a)-[*]->(b:labeled)
        AlgebraicExpression *to_replace = exps[0];
        if (to_replace->src_node_idx == NOT_IN_RECORD) {
            // Anonymous node - make space for it in the Record
            to_replace->src_node_idx = AST_AddAnonymousRecordEntry(ast);
        }
        op = NewNodeByLabelScanOp(to_replace->src_node, to_replace->src_node_idx);
        Vector_Push(traversals, op);
        AlgebraicExpression_Free(to_replace);
        for (uint q = 1; q < expCount; q ++) {
            exps[q-1] = exps[q];
        }
        expCount --;
        order = TRAVERSE_ORDER_FIRST;
    } else if (exps[expCount - 1]->op == AL_EXP_UNARY) {
        AlgebraicExpression *to_replace = exps[expCount - 1];
        if (to_replace->src_node_idx == NOT_IN_RECORD) {
            // Anonymous node - make space for it in the Record
            to_replace->src_node_idx = AST_AddAnonymousRecordEntry(ast);
        }
        op = NewNodeByLabelScanOp(to_replace->src_node, to_replace->src_node_idx);
        Vector_Push(traversals, op);
        AlgebraicExpression_Free(to_replace);
        expCount --;
        order = TRAVERSE_ORDER_LAST;
    } else {
        order = determineTraverseOrder(ft, exps, expCount);
    }

    if(order == TRAVERSE_ORDER_FIRST) {
        if (op == NULL) {
            // We haven't already built the appropriate label scan
            AlgebraicExpression *exp = exps[0];
            selectEntryPoint(exp, ft);
            if (exp->src_node_idx == NOT_IN_RECORD) {
                // Anonymous node - make space for it in the Record
                exp->src_node_idx = AST_AddAnonymousRecordEntry(ast);
            }

            // Create SCAN operation.
            if(exp->src_node->label) {
                /* There's no longer need for the last matrix operand
                 * as it's been replaced by label scan. */
                AlgebraicExpression_RemoveTerm(exp, exp->operand_count-1, NULL);
                op = NewNodeByLabelScanOp(exp->src_node, exp->src_node_idx);
                Vector_Push(traversals, op);
            } else {
                op = NewAllNodeScanOp(gc->g, exp->src_node, exp->src_node_idx);
                Vector_Push(traversals, op);
            }
        }
        for(int i = 0; i < expCount; i++) {
            if(exps[i]->operand_count == 0) continue;
            // TODO tmp
            if (exps[i]->op == AL_EXP_UNARY) {
                 exps[i]->dest_node_idx = exps[i]->src_node_idx;
            } else {
                AlgebraicExpression_ExtendRecord(exps[i]); // TODO should come before scans are built
            }
            if(exps[i]->minHops != 1 || exps[i]->maxHops != 1) {
                op = NewCondVarLenTraverseOp(exps[i],
                                             exps[i]->minHops,
                                             exps[i]->maxHops,
                                             gc->g);
            } else {
                op = NewCondTraverseOp(gc->g, exps[i], TraverseRecordCap(ast));
            }
            Vector_Push(traversals, op);
        }
    } else {
        if (op == NULL) {
            // We haven't already built the appropriate label scan
            AlgebraicExpression *exp = exps[expCount-1];
            selectEntryPoint(exp, ft);

            if (exp->dest_node_idx == NOT_IN_RECORD) {
                // Anonymous node - make space for it in the Record
                exp->dest_node_idx = AST_AddAnonymousRecordEntry(ast);
            }

            // Create SCAN operation.
            if(exp->dest_node->label) {
                /* There's no longer need for the last matrix operand
                 * as it's been replaced by label scan. */
                AlgebraicExpression_RemoveTerm(exp, exp->operand_count-1, NULL);
                op = NewNodeByLabelScanOp(exp->dest_node, exp->dest_node_idx);
                Vector_Push(traversals, op);
            } else {
                op = NewAllNodeScanOp(gc->g, exp->dest_node, exp->dest_node_idx);
                Vector_Push(traversals, op);
            }
        }

        for(int i = expCount-1; i >= 0; i--) {
            if(exps[i]->operand_count == 0) continue;
            AlgebraicExpression_Transpose(exps[i]);
            // TODO tmp
            if (exps[i]->op == AL_EXP_UNARY) {
                exps[i]->src_node_idx = exps[i]->dest_node_idx;
            } else {
                AlgebraicExpression_ExtendRecord(exps[i]);
            }
            if(exps[i]->minHops != 1 || exps[i]->maxHops != 1) {
                op = NewCondVarLenTraverseOp(exps[i],
                                             exps[i]->minHops,
                                             exps[i]->maxHops,
                                             gc->g);
            } else {
                op = NewCondTraverseOp(gc->g, exps[i], TraverseRecordCap(ast));
            }
            Vector_Push(traversals, op);
        }
    }
    // Free the expressions array, as its parts have been converted into operations
    free(exps);
}

void _ExecutionPlanSegment_AddTraversalOps(Vector *ops, OpBase *cartesian_root, Vector *traversals) {
    if(cartesian_root) {
        // If we're traversing multiple disjoint paths, the new traversal
        // should be connected uner a Cartesian product.
        OpBase *childOp;
        OpBase *parentOp;
        Vector_Pop(traversals, &parentOp);
        // Connect cartesian product to the root of traversal.
        ExecutionPlan_AddOp(cartesian_root, parentOp);
        while(Vector_Pop(traversals, &childOp)) {
            ExecutionPlan_AddOp(parentOp, childOp);
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

ExecutionPlanSegment* _NewExecutionPlanSegment(RedisModuleCtx *ctx, GraphContext *gc, AST *ast, ResultSet *result_set, ExecutionPlanSegment *segment, OpBase *prev_op) {
    Vector *ops = NewVector(OpBase*, 1);

    // Initialize map of Record IDs
    TrieMap *record_map = NewTrieMap();
    segment->record_map = record_map;

    // Build query graph
    QueryGraph *qg = BuildQueryGraph(gc, ast);
    segment->query_graph = qg;

    // Build filter tree
    FT_FilterNode *filter_tree = AST_BuildFilterTree(ast);
    segment->filter_tree = filter_tree;


    const cypher_astnode_t *call_clause = AST_GetClause(ast, CYPHER_AST_CALL);
    if(call_clause) {
        // A call clause has a procedure name, 0+ arguments (parenthesized expressions), and a projection if YIELD is included
        const char *proc_name = cypher_ast_proc_name_get_value(cypher_ast_call_get_proc_name(call_clause));
        uint arg_count = cypher_ast_call_narguments(call_clause);
        char **arguments = array_new(char*, arg_count);
        for (uint i = 0; i < arg_count; i ++) {
            const cypher_astnode_t *ast_arg = cypher_ast_call_get_argument(call_clause, i);
            AR_ExpNode *arg = AR_EXP_FromExpression(ast, ast_arg);
            char *arg_str;
            // TODO tmp, leak
            AR_EXP_ToString(arg, &arg_str);
            AST_RecordAccommodateExpression(ast, arg);
            AST_MapEntity(ast, ast_arg, arg);
            AST_MapAlias(ast, arg_str, arg);
            arguments = array_append(arguments, arg_str);
        }

        uint yield_count = cypher_ast_call_nprojections(call_clause);
        char **yields = array_new(char*, yield_count);
        uint *modified = array_new(char*, yield_count);
        // if (segment->projections == NULL) segment->projections = array_new(AR_ExpNode*, yield_count);
        AR_ExpNode *yield;
        char *yield_str;
        for (uint i = 0; i < yield_count; i ++) {
            // type == CYPHER_AST_PROJECTION
            const cypher_astnode_t *ast_yield = cypher_ast_call_get_projection(call_clause, i);
            const cypher_astnode_t *yield_alias = cypher_ast_projection_get_alias(ast_yield);

            // TODO all tmp
            if (yield_alias == NULL) {
                const cypher_astnode_t *ast_yield_exp = cypher_ast_projection_get_expression(ast_yield);
                yield = AST_GetEntity(ast, ast_yield_exp);
                // yield = AR_EXP_FromExpression(ast, ast_yield_exp);
                AR_EXP_ToString(yield, &yield_str);
                // AST_RecordAccommodateExpression(ast, yield);
                // AST_MapEntity(ast, ast_yield_exp, yield);
                // AST_MapAlias(ast, yield_str, yield);
                // yield->record_idx = AST_AddRecordEntry(ast);
            } else {
                yield_str = rm_strdup(cypher_ast_identifier_get_name(yield_alias));
                yield = AST_GetEntityFromAlias(ast, yield_str);
                AST_RecordAccommodateExpression(ast, yield);
            }
            // segment->projections = array_append(segment->projections, yield); // TODO tmp, adds yield to return exps
            yields = array_append(yields, yield_str);
            modified = array_append(modified, yield->record_idx);
        }

        /* Incase procedure call is missing its yield part
         * include procedure outputs. */
        if (yield_count == 0) {
            ProcedureCtx *proc = Proc_Get(proc_name);
            unsigned int output_count = array_len(proc->output);
            for(int i = 0; i < output_count; i++) {
                char *output_name = proc->output[i]->name;
                yield = AST_GetEntityFromAlias(ast, output_name);
                AR_EXP_ToString(yield, &yield_str);
                yields = array_append(yields, yield_str);
                modified = array_append(modified, yield->record_idx);
            }
        }
        OpBase *opProcCall = NewProcCallOp(proc_name, arguments, yields, modified, ast);
        Vector_Push(ops, opProcCall);
    }

    const cypher_astnode_t **match_clauses = AST_CollectReferencesInRange(ast, CYPHER_AST_MATCH);
    uint match_count = array_len(match_clauses);

    /* TODO Currently, we don't differentiate between:
     * MATCH (a) MATCH (b)
     * and
     * MATCH (a), (b)
     * Introduce this distinction. */
    OpBase *cartesianProduct = NULL;
    if (match_count > 1) {
        cartesianProduct = NewCartesianProductOp();
        Vector_Push(ops, cartesianProduct);
    }

    // Build traversal operations for every MATCH clause
    for (uint i = 0; i < match_count; i ++) {
        // Each MATCH clause has a pattern that consists of 1 or more paths
        const cypher_astnode_t *ast_pattern = cypher_ast_match_get_pattern(match_clauses[i]);
        uint npaths = cypher_ast_pattern_npaths(ast_pattern);

        /* If we're dealing with multiple paths (which our validations have guaranteed
         * are disjoint), we'll join them all together with a Cartesian product (full join). */
        if ((cartesianProduct == NULL) && (cypher_ast_pattern_npaths(ast_pattern) > 1)) {
            cartesianProduct = NewCartesianProductOp();
            Vector_Push(ops, cartesianProduct);
        }

        Vector *path_traversal = NewVector(OpBase*, 1);
        for (uint j = 0; j < npaths; j ++) {
            // Convert each path into the appropriate traversal operation(s).
            const cypher_astnode_t *path = cypher_ast_pattern_get_path(ast_pattern, j);
            _ExecutionPlanSegment_BuildTraversalOps(qg, filter_tree, path, path_traversal);
            _ExecutionPlanSegment_AddTraversalOps(ops, cartesianProduct, path_traversal);
            Vector_Clear(path_traversal);
        }
        Vector_Free(path_traversal);
    }

    array_free(match_clauses);

    // Set root operation
    const cypher_astnode_t *unwind_clause = AST_GetClause(ast, CYPHER_AST_UNWIND);
    if(unwind_clause) {
        AST_UnwindContext unwind_ast_ctx = AST_PrepareUnwindOp(ast, unwind_clause);

        OpBase *opUnwind = NewUnwindOp(unwind_ast_ctx.record_idx, unwind_ast_ctx.exps);
        Vector_Push(ops, opUnwind);
    }

    bool create_clause = AST_ContainsClause(ast, CYPHER_AST_CREATE);
    if(create_clause) {
        AST_CreateContext create_ast_ctx = AST_PrepareCreateOp(ast, qg);
        OpBase *opCreate = NewCreateOp(&result_set->stats,
                                       create_ast_ctx.nodes_to_create,
                                       create_ast_ctx.edges_to_create);
        Vector_Push(ops, opCreate);
    }

    const cypher_astnode_t *merge_clause = AST_GetClause(ast, CYPHER_AST_MERGE);
    if(merge_clause) {
        // A merge clause provides a single path that must exist or be created.
        // As with paths in a MATCH query, build the appropriate traversal operations
        // and append them to the set of ops.
        const cypher_astnode_t *path = cypher_ast_merge_get_pattern_path(merge_clause);
        Vector *path_traversal = NewVector(OpBase*, 1);
        _ExecutionPlanSegment_BuildTraversalOps(qg, filter_tree, path, path_traversal);
        _ExecutionPlanSegment_AddTraversalOps(ops, NULL, path_traversal);
        Vector_Free(path_traversal);

        // Append a merge operation
        AST_MergeContext merge_ast_ctx = AST_PrepareMergeOp(ast, merge_clause, qg);
        OpBase *opMerge = NewMergeOp(&result_set->stats,
                                     merge_ast_ctx.nodes_to_merge,
                                     merge_ast_ctx.edges_to_merge);
        Vector_Push(ops, opMerge);
    }

    const cypher_astnode_t *delete_clause = AST_GetClause(ast, CYPHER_AST_DELETE);
    if(delete_clause) {
        uint *nodes_ref;
        uint *edges_ref;
        AST_PrepareDeleteOp(delete_clause, &nodes_ref, &edges_ref);
        OpBase *opDelete = NewDeleteOp(nodes_ref, edges_ref, &result_set->stats);
        Vector_Push(ops, opDelete);
    }

    const cypher_astnode_t *set_clause = AST_GetClause(ast, CYPHER_AST_SET);
    if(set_clause) {
        // Create a context for each update expression.
        uint nitems;
        EntityUpdateEvalCtx *update_exps = AST_PrepareUpdateOp(set_clause, &nitems);
        OpBase *op_update = NewUpdateOp(gc, update_exps, nitems, &result_set->stats);
        Vector_Push(ops, op_update);
    }

    const cypher_astnode_t *with_clause = AST_GetClause(ast, CYPHER_AST_WITH);
    const cypher_astnode_t *ret_clause = AST_GetClause(ast, CYPHER_AST_RETURN);

    assert(!(with_clause && ret_clause));

    uint *modifies = NULL;

    // WITH/RETURN projections have already been constructed from the AST
    AR_ExpNode **projections = segment->projections;

    if (with_clause || ret_clause || call_clause) {
        uint exp_count = array_len(projections);
        modifies = array_new(uint, exp_count);
        for (uint i = 0; i < exp_count; i ++) {
            AR_ExpNode *exp = projections[i];
            modifies = array_append(modifies, exp->record_idx);
        }
    }

    OpBase *op;

    if(with_clause) {
        // uint *with_projections = AST_WithClauseModifies(ast, with_clause);
        if (AST_ClauseContainsAggregation(with_clause)) {
            op = NewAggregateOp(projections, modifies);
        } else {
            op = NewProjectOp(projections, modifies);
        }
        Vector_Push(ops, op);

        if (cypher_ast_with_is_distinct(with_clause)) {
            op = NewDistinctOp();
            Vector_Push(ops, op);
        }

        const cypher_astnode_t *skip_clause = cypher_ast_with_get_skip(with_clause);
        const cypher_astnode_t *limit_clause = cypher_ast_with_get_limit(with_clause);

        uint skip = 0;
        uint limit = 0;
        if (skip_clause) skip = AST_ParseIntegerNode(skip_clause);
        if (limit_clause) limit = AST_ParseIntegerNode(limit_clause);

        if (segment->order_expressions) {
            const cypher_astnode_t *order_clause = cypher_ast_with_get_order_by(with_clause);
            int direction = AST_PrepareSortOp(order_clause);
            // The sort operation will obey a specified limit, but must account for skipped records
            uint sort_limit = (limit > 0) ? limit + skip : 0;
            op = NewSortOp(segment->order_expressions, direction, sort_limit);
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
    } else if (ret_clause) {

        // TODO we may not need a new project op if the query is something like:
        // MATCH (a) WITH a.val AS val RETURN val
        // Though we would still need a new projection (barring later optimizations) for:
        // MATCH (a) WITH a.val AS val RETURN val AS e
        if (AST_ClauseContainsAggregation(ret_clause)) {
            op = NewAggregateOp(projections, modifies);
        } else {
            op = NewProjectOp(projections, modifies);
        }
        Vector_Push(ops, op);

        if (cypher_ast_return_is_distinct(ret_clause)) {
            op = NewDistinctOp();
            Vector_Push(ops, op);
        }

        const cypher_astnode_t *order_clause = cypher_ast_return_get_order_by(ret_clause);
        const cypher_astnode_t *skip_clause = cypher_ast_return_get_skip(ret_clause);
        const cypher_astnode_t *limit_clause = cypher_ast_return_get_limit(ret_clause);

        uint skip = 0;
        uint limit = 0;
        if (skip_clause) skip = AST_ParseIntegerNode(skip_clause);
        if (limit_clause) limit = AST_ParseIntegerNode(limit_clause);

        if (segment->order_expressions) {
            int direction = AST_PrepareSortOp(order_clause);
            // The sort operation will obey a specified limit, but must account for skipped records
            uint sort_limit = (limit > 0) ? limit + skip : 0;
            op = NewSortOp(segment->order_expressions, direction, sort_limit);
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

        op = NewResultsOp(result_set, qg);
        Vector_Push(ops, op);
    } else if (call_clause) {
        op = NewResultsOp(result_set, qg);
        Vector_Push(ops, op);
    }

    OpBase *parent_op;
    OpBase *child_op;
    Vector_Pop(ops, &parent_op);
    segment->root = parent_op;

    while(Vector_Pop(ops, &child_op)) {
        ExecutionPlan_AddOp(parent_op, child_op);
        parent_op = child_op;
    }

    Vector_Free(ops);

    if (prev_op) {
        // Need to connect this segment to the previous one.
        // If the last operation of this segment is a potential data producer, join them
        // under an Apply operation.
        if (parent_op->type & OP_TAPS) {
            OpBase *op_apply = NewApplyOp();
            ExecutionPlan_PushBelow(parent_op, op_apply);
            ExecutionPlan_AddOp(op_apply, prev_op);
        } else {
            // All operations can be connected in a single chain.
            ExecutionPlan_AddOp(parent_op, prev_op);
        }
    }

    if(segment->filter_tree) {
        Vector *sub_trees = FilterTree_SubTrees(segment->filter_tree);

        /* For each filter tree find the earliest position along the execution
         * after which the filter tree can be applied. */
        for(int i = 0; i < Vector_Size(sub_trees); i++) {
            FT_FilterNode *tree;
            Vector_Get(sub_trees, i, &tree);

            uint *references = FilterTree_CollectModified(tree);

            /* Scan execution segment, locate the earliest position where all
             * references been resolved. */
            OpBase *op = ExecutionPlan_LocateReferences(segment->root, references);
            assert(op);

            /* Create filter node.
             * Introduce filter op right below located op. */
            OpBase *filter_op = NewFilterOp(tree);
            ExecutionPlan_PushBelow(op, filter_op);
            array_free(references);
        }
        Vector_Free(sub_trees);
    }

    segment->record_len = AST_RecordLength(ast);

    return segment;
}

// Map the required AST entities and build expressions to match
// the AST slice's WITH, RETURN, and ORDER clauses
ExecutionPlanSegment* _PrepareSegment(AST *ast, AR_ExpNode **projections) {
    // Allocate a new segment
    ExecutionPlanSegment *segment = rm_malloc(sizeof(ExecutionPlanSegment));

    if (projections) {
        // We have an array of identifiers provided by a prior WITH clause -
        // these will correspond to our first Record entities
        uint projection_count = array_len(projections);
        for (uint i = 0; i < projection_count; i++) {
            // TODO add interface
            AR_ExpNode *projection = projections[i];
            uint record_idx = AST_AddRecordEntry(ast);
            AST_MapAlias(ast, projection->alias, record_idx);
        }
    }

    AST_BuildAliasMap(ast);

    // Retrieve a RETURN clause if one is specified in this AST's range
    const cypher_astnode_t *ret_clause = AST_GetClause(ast, CYPHER_AST_RETURN);
    // Retrieve a WITH clause if one is specified in this AST's range
    const cypher_astnode_t *with_clause = AST_GetClause(ast, CYPHER_AST_WITH);

    // We cannot have both a RETURN and WITH clause
    assert(!(ret_clause && with_clause));
    segment->projections = NULL;
    segment->order_expressions = NULL;

    if (ret_clause) {
        segment->projections = AST_BuildReturnExpressions(ast, ret_clause);
        const cypher_astnode_t *order_clause = cypher_ast_return_get_order_by(ret_clause);
        if (order_clause) segment->order_expressions = AST_BuildOrderExpressions(ast, order_clause);
    } else if (with_clause) {
        segment->projections = AST_BuildWithExpressions(ast, with_clause);
        const cypher_astnode_t *order_clause = cypher_ast_with_get_order_by(with_clause);
        if (order_clause) segment->order_expressions = AST_BuildOrderExpressions(ast, order_clause);
    }
    // TODO tmp
    const cypher_astnode_t *call_clause = AST_GetClause(ast, CYPHER_AST_CALL);
    if(call_clause) {
        uint yield_count = cypher_ast_call_nprojections(call_clause);
        if (segment->projections == NULL) segment->projections = array_new(AR_ExpNode*, yield_count);
        for (uint i = 0; i < yield_count; i ++) {
            // type == CYPHER_AST_PROJECTION
            const cypher_astnode_t *ast_yield = cypher_ast_call_get_projection(call_clause, i);
            const cypher_astnode_t *yield_alias = cypher_ast_projection_get_alias(ast_yield);

            AR_ExpNode *yield;
            char *yield_str;
            // TODO all tmp
            if (yield_alias == NULL) {
                const cypher_astnode_t *ast_yield_exp = cypher_ast_projection_get_expression(ast_yield);
                yield = AR_EXP_FromExpression(ast, ast_yield_exp);
                AR_EXP_ToString(yield, &yield_str);
                AST_RecordAccommodateExpression(ast, yield);
                AST_MapEntity(ast, ast_yield_exp, yield);
                AST_MapAlias(ast, yield_str, yield);
                yield->record_idx = AST_AddRecordEntry(ast);
            } else {
                yield_str = rm_strdup(cypher_ast_identifier_get_name(yield_alias));
                yield = AST_GetEntityFromAlias(ast, yield_str);
                AST_RecordAccommodateExpression(ast, yield);
            }
            segment->projections = array_append(segment->projections, yield); // TODO tmp, adds yield to return exps
        }

        /* Incase procedure call is missing its yield part
         * include procedure outputs. */
        if (yield_count == 0) {
            const char *proc_name = cypher_ast_proc_name_get_value(cypher_ast_call_get_proc_name(call_clause));
            ProcedureCtx *proc = Proc_Get(proc_name);
            assert(proc);

            unsigned int output_count = array_len(proc->output);
            for(int i = 0; i < output_count; i++) {
                AR_ExpNode *exp = AR_EXP_NewAnonymousEntity(AST_AddRecordEntry(ast));
                exp->operand.variadic.entity_alias = strdup(proc->output[i]->name);
                exp->operand.variadic.entity_alias_idx = exp->record_idx;
                AST_RecordAccommodateExpression(ast, exp);
                // AST_MapEntity(ast, ast_yield_exp, yield);
                AST_MapAlias(ast, proc->output[i]->name, exp);
                // yield->record_idx = AST_AddRecordEntry(ast);
                segment->projections = array_append(segment->projections, exp);
            }
        }
    }

    return segment;
}

// TODO same as AST_Free, but doesn't free defined entities
void _AST_Free(AST *ast) {
    if (ast->defined_entities) {
        uint len = array_len(ast->defined_entities);
        for (uint i = 0; i < len; i ++) {
            // TODO leaks on entities that are not handed off
            // AR_EXP_Free(ast->defined_entities[i]);
        }
        array_free(ast->defined_entities);
    } else {
        ast->defined_entities = array_new(AR_ExpNode*, 1);
    }

    if (ast->entity_map) TrieMap_Free(ast->entity_map, TrieMap_NOP_CB);
    // TODO probably a memory leak on ast->root
    rm_free(ast);
}


ExecutionPlan* NewExecutionPlan(RedisModuleCtx *ctx, GraphContext *gc, bool compact, bool explain) {
    AST *ast = AST_GetFromTLS();

    ExecutionPlan *plan = rm_malloc(sizeof(ExecutionPlan));

    plan->result_set = _prepare_resultset(ctx, ast, compact);

    uint with_clause_count = AST_GetClauseCount(ast, CYPHER_AST_WITH);
    plan->segment_count = with_clause_count + 1;

    plan->segments = rm_malloc(plan->segment_count * sizeof(ExecutionPlanSegment));

    uint *segment_indices = NULL;
    if (with_clause_count > 0) segment_indices = AST_GetClauseIndices(ast, CYPHER_AST_WITH);

    uint i = 0;
    uint end_offset;
    uint start_offset = 0;
    OpBase *prev_op = NULL;
    ExecutionPlanSegment *segment;
    AR_ExpNode **input_projections = NULL;

    // The original AST does not need to be modified if our query only has one segment
    AST *ast_segment = ast;
    if (with_clause_count > 0) {
        for (i = 0; i < with_clause_count; i++) {
            end_offset = segment_indices[i] + 1; // Switching from index to bound, so add 1
            ast_segment = AST_NewSegment(ast, start_offset, end_offset);
            segment = _PrepareSegment(ast_segment, input_projections);
            plan->segments[i] = _NewExecutionPlanSegment(ctx, gc, ast_segment, plan->result_set, segment, prev_op);
            _AST_Free(ast_segment); // Free all AST constructions scoped to this segment
            // Store the expressions constructed by this segment's WITH projection to pass into the *next* segment
            prev_op = segment->root;
            input_projections = plan->segments[i]->projections;
            start_offset = end_offset;
        }
        // Prepare the last AST segment
        end_offset = cypher_astnode_nchildren(ast->root);
        ast_segment = AST_NewSegment(ast, start_offset, end_offset);
    }

    segment = _PrepareSegment(ast_segment, input_projections);
    const cypher_astnode_t *ret_clause = AST_GetClause(ast, CYPHER_AST_RETURN);
    AR_ExpNode **return_columns = NULL;
    if (segment->projections) {
        return_columns = segment->projections; // TODO kludge
    }
    if (explain == false) ResultSet_ReplyWithPreamble(plan->result_set, return_columns);

    plan->segments[i] = _NewExecutionPlanSegment(ctx, gc, ast_segment, plan->result_set, segment, prev_op);

    plan->root = segment->root;

    optimizePlan(gc, plan);

    if (ast_segment != ast) {
        _AST_Free(ast_segment);
    }
    // _AST_Free(ast);

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

char* ExecutionPlan_Print(const ExecutionPlan *plan) {
    char *strPlan = NULL;
    _ExecutionPlanPrint(plan->root, &strPlan, 0);
    return strPlan;
}

void _ExecutionPlanSegmentInit(OpBase *root, uint record_len) {
    // If the operation's record length has already been set, it and all subsequent
    // operations have been initialized by an earlier segment.
    if (root->record_len > 0) return;

    root->record_len = record_len;
    if(root->init) root->init(root);
    for(int i = 0; i < root->childCount; i++) {
        _ExecutionPlanSegmentInit(root->children[i], record_len);
    }
}

ResultSet* ExecutionPlan_Execute(ExecutionPlan *plan) {
    for (uint i = 0; i < plan->segment_count; i ++) {
        ExecutionPlanSegment *segment = plan->segments[i];
        _ExecutionPlanSegmentInit(segment->root, segment->record_len);
    }

    Record r;
    OpBase *op = plan->root;

    while((r = op->consume(op)) != NULL) Record_Free(r);
    return plan->result_set;
}



void _ExecutionPlan_FreeOperations(OpBase* op) {
    for(int i = 0; i < op->childCount; i++) {
        _ExecutionPlan_FreeOperations(op->children[i]);
    }
    OpBase_Free(op);
}

void ExecutionPlan_Free(ExecutionPlan *plan) {
    if(plan == NULL) return;
    _ExecutionPlan_FreeOperations(plan->root);

    for (uint i = 0; i < plan->segment_count; i ++) {
        ExecutionPlanSegment *segment = plan->segments[i];
        QueryGraph_Free(segment->query_graph);
        rm_free(segment);
    }
    rm_free(plan->segments);

    rm_free(plan);
}
