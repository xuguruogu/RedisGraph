/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "algebraic_expression.h"
#include "../util/arr.h"
#include "../util/rmalloc.h"
#include "../algorithms/algorithms.h"

#include <assert.h>

AlgebraicExpression *_AE_MUL(size_t operand_cap) {
    AlgebraicExpression *ae = malloc(sizeof(AlgebraicExpression));
    ae->op = AL_EXP_MUL;
    ae->operand_cap = operand_cap;
    ae->operand_count = 0;
    ae->operands = malloc(sizeof(AlgebraicExpressionOperand) * ae->operand_cap);
    ae->edge = NULL;
    ae->edgeLength = NULL;
    return ae;
}

static int _intermidate_node(const Node *n) {
    /* ->()<- 
     * <-()->
     * ->()->
     * <-()<- */
    return ((array_len(n->incoming_edges) > 1) ||
            (array_len(n->outgoing_edges) > 1) ||
            ((array_len(n->incoming_edges) > 0) && (array_len(n->outgoing_edges) > 0)));
}

static int _referred_entity(char *alias, TrieMap *ref_entities) {
    return TRIEMAP_NOTFOUND != TrieMap_Find(ref_entities, alias, strlen(alias));
}

static int _referred_node(const Node *n, TrieMap *ref_entities) {
    return _referred_entity(n->alias, ref_entities);
}

/* For every referenced edge, add edge source and destination nodes
 * as referenced entities. */
static void _referred_edge_ends(const QueryGraph *q, TrieMap *ref_entities) {
    for(int i = 0; i < q->edge_count; i++) {
        Edge *e = q->edges[i];
        if(TrieMap_Find(ref_entities, e->alias, strlen(e->alias)) != TRIEMAP_NOTFOUND) {
            // Edge is referenced, add ends as referenced entities.
            char *src = e->src->alias;
            char *dest = e->dest->alias;
            TrieMap_Add(ref_entities, src, strlen(src), NULL, TrieMap_DONT_CARE_REPLACE);
            TrieMap_Add(ref_entities, dest, strlen(dest), NULL, TrieMap_DONT_CARE_REPLACE);
        }
    }
}

/* Variable length edges require their own algebraic expression,
 * therefor mark both variable length edge ends as referenced. */
static void _referred_variable_length_edges(TrieMap *ref_entities, Vector *matchPattern, const QueryGraph *q) {
    Edge *e;
    AST_GraphEntity *match_element;

    for(int i = 0; i < Vector_Size(matchPattern); i++) {
        Vector_Get(matchPattern, i, &match_element);
        if(match_element->t != N_LINK) continue;
        
        AST_LinkEntity *edge = (AST_LinkEntity*)match_element;
        if(!edge->length) continue;

        e = QueryGraph_GetEdgeByAlias(q, edge->ge.alias);
        TrieMap_Add(ref_entities, e->src->alias, strlen(e->src->alias), NULL, TrieMap_DONT_CARE_REPLACE);
        TrieMap_Add(ref_entities, e->dest->alias, strlen(e->dest->alias), NULL, TrieMap_DONT_CARE_REPLACE);
    }
}

/* Variable length edges require their own algebraic expression,
 * therefor mark both variable length edge ends as referenced. */
static void _referred_variable_length_edges_(const QueryGraph *q, TrieMap *ref_entities) {
    Edge *e;
    char *src;
    char *dest;

    for(int i = 0; i < q->edge_count; i++) {
        e = q->edges[i];
        if(Edge_VariableLength(e)) {
            src = e->src->alias;
            dest = e->dest->alias;
            TrieMap_Add(ref_entities, src, strlen(src), NULL, TrieMap_DONT_CARE_REPLACE);
            TrieMap_Add(ref_entities, dest, strlen(dest), NULL, TrieMap_DONT_CARE_REPLACE);
        }
    }
}

/* Checks if given expression contains a variable length edge. */
static bool _AlgebraicExpression_ContainsVariableLengthEdge(const AlgebraicExpression *exp) {
    return (exp->edge && Edge_VariableLength(exp->edge));
}

/* Variable length expression must contain only a single operand, the edge being 
 * traversed multiple times, in cases such as (:labelA)-[e*]->(:labelB) both label A and B
 * are applied via a label matrix operand, this function migrates A and B from a
 * variable length expression to other expressions. */
static AlgebraicExpression** _AlgebraicExpression_IsolateVariableLenExps(AlgebraicExpression **expressions, size_t *expCount) {
    /* Return value is a new set of expressions, where each variable length expression
     * is guaranteed to have a single operand, as such in the worst case the number of
     * expressions doubles + 1. */
    AlgebraicExpression **res = malloc(sizeof(AlgebraicExpression*) * (*expCount * 2 + 1));
    size_t newExpCount = 0;

    /* Scan through each expression, locate expression which 
     * have a variable length edge in them. */
    for(size_t expIdx = 0; expIdx < *expCount; expIdx++) {
        AlgebraicExpression *exp = expressions[expIdx];
        if(!_AlgebraicExpression_ContainsVariableLengthEdge(exp)) {
            res[newExpCount++] = exp;
            continue;
        }

        Edge *e = exp->edge;
        Node *src = exp->src_node;
        Node *dest = exp->dest_node;
        
        // A variable length expression with a labeled source node
        // We only care about the source label matrix, when it comes to
        // the first expression, as in the following expressions
        // src is the destination of the previous expression.
        if(src->mat && expIdx == 0) {
            // Remove src node matrix from expression.
            AlgebraicExpressionOperand op;
            AlgebraicExpression_RemoveTerm(exp, 0, &op);

            /* Create a new expression. */
            AlgebraicExpression *newExp = _AE_MUL(1);
            newExp->src_node = exp->src_node;
            newExp->dest_node = exp->src_node;
            AlgebraicExpression_PrependTerm(newExp, op.operand, op.transpose, op.free);
            res[newExpCount++] = newExp;
        }

        res[newExpCount++] = exp;

        // A variable length expression with a labeled destination node.
        if(dest->mat) {
            // Remove dest node matrix from expression.
            AlgebraicExpressionOperand op;
            AlgebraicExpression_RemoveTerm(exp, exp->operand_count-1, &op);

            /* See if dest mat can be prepended to the following expression.
             * If not create a new expression. */            
            if(expIdx < *expCount-1 &&
                !_AlgebraicExpression_ContainsVariableLengthEdge(expressions[expIdx+1])) {
                AlgebraicExpression_PrependTerm(expressions[expIdx+1], op.operand, op.transpose, op.free);
            } else {
                AlgebraicExpression *newExp = _AE_MUL(1);
                newExp->src_node = exp->dest_node;
                newExp->dest_node = exp->dest_node;
                AlgebraicExpression_PrependTerm(newExp, op.operand, op.transpose, op.free);
                res[newExpCount++] = newExp;
            }
        }
    }

    *expCount = newExpCount;
    free(expressions);
    return res;
}

/* Variable length expression must contain only a single operand, the edge being 
 * traversed multiple times, in cases such as (:labelA)-[e*]->(:labelB) both label A and B
 * are applied via a label matrix operand, this function migrates A and B from a
 * variable length expression to other expressions. */
static AlgebraicExpression** _AlgebraicExpression_IsolateVariableLenExps_(AlgebraicExpression **expressions) {
    /* Return value is a new set of expressions, where each variable length expression
     * is guaranteed to have a single operand, as such in the worst case the number of
     * expressions doubles + 1. */
    // AlgebraicExpression **res = malloc(sizeof(AlgebraicExpression*) * (*expCount * 2 + 1));

    size_t expCount = array_len(expressions);
    AlgebraicExpression **res = array_new(AlgebraicExpression*, expCount * 2 + 1);

    /* Scan through each expression, locate expression which 
     * have a variable length edge in them. */
    for(size_t expIdx = 0; expIdx < expCount; expIdx++) {
        AlgebraicExpression *exp = expressions[expIdx];
        if(!_AlgebraicExpression_ContainsVariableLengthEdge(exp)) {
            res = array_append(res, exp);
            continue;
        }

        Edge *e = exp->edge;
        Node *src = exp->src_node;
        Node *dest = exp->dest_node;
        
        // A variable length expression with a labeled source node
        // We only care about the source label matrix, when it comes to
        // the first expression, as in the following expressions
        // src is the destination of the previous expression.
        if(src->mat && expIdx == 0) {
            // Remove src node matrix from expression.
            AlgebraicExpressionOperand op;
            AlgebraicExpression_RemoveTerm(exp, 0, &op);

            /* Create a new expression. */
            AlgebraicExpression *newExp = _AE_MUL(1);
            newExp->src_node = exp->src_node;
            newExp->dest_node = exp->src_node;
            AlgebraicExpression_PrependTerm(newExp, op.operand, op.transpose, op.free);
            res = array_append(res, newExp);
        }

        res = array_append(res, exp);

        // A variable length expression with a labeled destination node.
        if(dest->mat) {
            // Remove dest node matrix from expression.
            AlgebraicExpressionOperand op;
            AlgebraicExpression_RemoveTerm(exp, exp->operand_count-1, &op);

            /* See if dest mat can be prepended to the following expression.
             * If not create a new expression. */            
            if(expIdx < expCount-1 &&
                !_AlgebraicExpression_ContainsVariableLengthEdge(expressions[expIdx+1])) {
                AlgebraicExpression_PrependTerm(expressions[expIdx+1], op.operand, op.transpose, op.free);
            } else {
                AlgebraicExpression *newExp = _AE_MUL(1);
                newExp->src_node = exp->dest_node;
                newExp->dest_node = exp->dest_node;
                AlgebraicExpression_PrependTerm(newExp, op.operand, op.transpose, op.free);
                res = array_append(res, newExp);
            }
        }
    }

    array_free(expressions);
    return res;
}


/* Break down expression into sub expressions.
 * considering referenced intermidate nodes and edges. */
static AlgebraicExpression** _AlgebraicExpression_Intermidate_Expressions(AlgebraicExpression *exp, const AST *ast, Vector *matchPattern, const QueryGraph *q, size_t *exp_count) {
    /* Allocating maximum number of expression possible. */
    AlgebraicExpression **expressions = malloc(sizeof(AlgebraicExpression *) * exp->operand_count);
    int expIdx = 0;     // Sub expression index.
    int operandIdx = 0; // Index to currently inspected operand.
    int transpose;      // Indicate if matrix operand needs to be transposed.
    Edge *e = NULL;
    Node *src = NULL;
    Node *dest = NULL;

    TrieMap *ref_entities = NewTrieMap();
    SetClause_ReferredEntities(ast->setNode, ref_entities);
    WithClause_ReferredEntities(ast->withNode, ref_entities);
    WhereClause_ReferredEntities(ast->whereNode, ref_entities);
    ReturnClause_ReferredEntities(ast->returnNode, ref_entities);
    CreateClause_ReferredEntities(ast->createNode, ref_entities);
    DeleteClause_ReferredEntities(ast->deleteNode, ref_entities);
    _referred_edge_ends(q, ref_entities);
    _referred_variable_length_edges(ref_entities, matchPattern, q);

    AlgebraicExpression *iexp = _AE_MUL(exp->operand_count);
    iexp->src_node = exp->src_node;
    iexp->dest_node = exp->dest_node;
    iexp->operand_count = 0;
    expressions[expIdx++] = iexp;

    for(int i = 0; i < Vector_Size(matchPattern); i++) {
        AST_GraphEntity *match_element;
        Vector_Get(matchPattern, i, &match_element);

        if(match_element->t != N_LINK) continue;

        AST_LinkEntity *edge = (AST_LinkEntity*)match_element;
        transpose = (edge->direction == N_RIGHT_TO_LEFT);
        e = QueryGraph_GetEdgeByAlias(q, edge->ge.alias);

        /* If edge is referenced, set expression edge pointer. */
        if(_referred_entity(edge->ge.alias, ref_entities))
            iexp->edge = e;

        /* If this is a variable length edge, which is not fixed in length
         * remember edge length. */
        if(edge->length && !AST_LinkEntity_FixedLengthEdge(edge)) {
            iexp->edgeLength = edge->length;
            iexp->edge = e;
        }

        dest = e->dest;
        src = e->src;
        if(transpose) {
            dest = e->src;
            src = e->dest;
        }        

        if(operandIdx == 0 && src->mat) {
            iexp->operands[iexp->operand_count++] = exp->operands[operandIdx++];
        }

        unsigned int hops = 1;
        /* Expand fixed variable length edge */
        if(edge->length && AST_LinkEntity_FixedLengthEdge(edge)) {
            hops = edge->length->minHops;
        }

        for(int j = 0; j < hops; j++) {
            iexp->operands[iexp->operand_count++] = exp->operands[operandIdx++];
        }

        if(dest->mat) {
            iexp->operands[iexp->operand_count++] = exp->operands[operandIdx++];
        }

        /* If intermidate node is referenced, create a new algebraic expression. */
        if(_intermidate_node(dest) && _referred_node(dest, ref_entities)) {
            // Finalize current expression.
            iexp->dest_node = dest;

            /* Create a new algebraic expression. */
            iexp = _AE_MUL(exp->operand_count - operandIdx);
            iexp->operand_count = 0;
            iexp->src_node = expressions[expIdx-1]->dest_node;
            iexp->dest_node = exp->dest_node;
            expressions[expIdx++] = iexp;
        }
    }
    
    *exp_count = expIdx;
    TrieMap_Free(ref_entities, TrieMap_NOP_CB);
    return expressions;
}

static inline void _AlgebraicExpression_Execute_MUL(GrB_Matrix C, GrB_Matrix A, GrB_Matrix B, GrB_Descriptor desc) {
    // Using our own compile-time, user defined semiring see rg_structured_bool.m4
    // A,B,C must be boolean matrices.
    GrB_Info res = GrB_mxm(
        C,                  // Output
        GrB_NULL,           // Mask
        GrB_NULL,           // Accumulator
        Rg_structured_bool, // Semiring
        A,                  // First matrix
        B,                  // Second matrix
        desc                // Descriptor        
    );
    assert(res == GrB_SUCCESS);
}

// Reverse order of operand within expression,
// A*B*C will become C*B*A. 
static void _AlgebraicExpression_ReverseOperandOrder(AlgebraicExpression *exp) {
    int right = exp->operand_count-1;
    int left = 0;
    while(right > left) {
        AlgebraicExpressionOperand leftOp = exp->operands[left];
        exp->operands[left] = exp->operands[right];
        exp->operands[right] = leftOp;
        right--;
        left++;
    }
}

void AlgebraicExpression_AppendTerm(AlgebraicExpression *ae, GrB_Matrix m, bool transposeOp, bool freeOp) {
    assert(ae);    
    if(ae->operand_count+1 > ae->operand_cap) {
        ae->operand_cap += 4;
        ae->operands = realloc(ae->operands, sizeof(AlgebraicExpressionOperand) * ae->operand_cap);
    }

    ae->operands[ae->operand_count].transpose = transposeOp;
    ae->operands[ae->operand_count].free = freeOp;
    ae->operands[ae->operand_count].operand = m;
    ae->operand_count++;
}

void AlgebraicExpression_PrependTerm(AlgebraicExpression *ae, GrB_Matrix m, bool transposeOp, bool freeOp) {
    assert(ae);

    ae->operand_count++;
    if(ae->operand_count+1 > ae->operand_cap) {
        ae->operand_cap += 4;
        ae->operands = realloc(ae->operands, sizeof(AlgebraicExpressionOperand) * ae->operand_cap);
    }

    // TODO: might be optimized with memcpy.
    // Shift operands to the right, making room at the begining.
    for(int i = ae->operand_count-1; i > 0 ; i--) {
        ae->operands[i] = ae->operands[i-1];
    }

    ae->operands[0].transpose = transposeOp;
    ae->operands[0].free = freeOp;
    ae->operands[0].operand = m;
}

void AlgebraicExpression_AppendOperand(AlgebraicExpression *ae, AlgebraicExpressionOperand op) {
    assert(ae);

    // Make sure expression has enough room for the new operand.
    if(ae->operand_count+1 > ae->operand_cap) {
        ae->operand_cap += 4;
        ae->operands = realloc(ae->operands, sizeof(AlgebraicExpressionOperand) * ae->operand_cap);
    }
    
    ae->operands[ae->operand_count] = op;
    ae->operand_count++;
}

void AlgebraicExpression_PrependOperand(AlgebraicExpression *ae, AlgebraicExpressionOperand op) {
    assert(ae);

    // Make sure expression has enough room for the new operand.
    ae->operand_count++;
    if(ae->operand_count+1 > ae->operand_cap) {
        ae->operand_cap += 4;
        ae->operands = realloc(ae->operands, sizeof(AlgebraicExpressionOperand) * ae->operand_cap);
    }

    // TODO: might be optimized with memcpy.
    // Shift operands to the right, making room at the begining.
    for(int i = ae->operand_count-1; i > 0 ; i--) {
        ae->operands[i] = ae->operands[i-1];
    }

    ae->operands[0] = op;
}

static void _RemovePathFromGraph(QueryGraph *g, Edge **path) {
    uint edge_count = array_len(path);
    for(uint i = 0; i < edge_count; i++) {
        Edge *e = path[i];
        Node *src = e->src;
        Node *dest = e->dest;

        QueryGraph_RemoveEdge(g, e);
        if(Node_EdgeCount(src) == 0) QueryGraph_RemoveNode(g, src);
        if(Node_EdgeCount(dest) == 0) QueryGraph_RemoveNode(g, dest);
    }
}

/* Determin the length of the longest path in the graph.
 * Returns a list residing on the edge of the longest path. */
static Node** _DeepestLevel(const QueryGraph *g, int *level) {
    Node *s = g->nodes[0];
    int l = BFS_LOWEST_LEVEL;
    Node **leafs = BFS(g->nodes[0], &l);
    Node *leaf = leafs[0];
    array_free(leafs);

    l = BFS_LOWEST_LEVEL;
    leafs = BFS(leaf, &l);
    array_free(leafs);

    *level = l;
    return leafs;
}

/* Break down expression into sub expressions.
 * considering referenced intermidate nodes and edges. */
static AlgebraicExpression** __AlgebraicExpression_Intermidate_Expressions__(
    AlgebraicExpression *exp, Edge **path, const QueryGraph *g,
    TrieMap *ref_entities) {

    /* Allocating maximum number of expression possible. */
    int expIdx = 0;         // Sub expression index.
    int operandIdx = 0;     // Index to currently inspected operand.
    bool transpose;         // Indicate if matrix operand needs to be transposed.
    Edge *e = NULL;
    Node *src = NULL;
    Node *dest = NULL;
    AlgebraicExpression **expressions;

    expressions = array_new(AlgebraicExpression*, exp->operand_count);
    AlgebraicExpression *iexp = _AE_MUL(exp->operand_count);
    iexp->operand_count = 0;
    iexp->src_node = exp->src_node;
    iexp->dest_node = exp->dest_node;
    expressions = array_append(expressions, iexp);
    expIdx++;

    for(int i = 0; i < array_len(path); i++) {
        e = path[i];
        src = e->src;
        dest = e->dest;

        transpose = false;
        if(i > 0) transpose = (path[i-1]->dest != e->src);
        if(transpose) {
            dest = e->src;
            src = e->dest;
        }

        /* If edge is referenced, set expression edge pointer. */
        if(_referred_entity(e->alias, ref_entities)) iexp->edge = e;

        /* If this is a variable length edge, which is not fixed in length
         * remember edge length. */
        if(Edge_VariableLength(e)) {
            iexp->edge = e;
        }

        if(i == 0 && src->mat) {
            iexp->operands[iexp->operand_count++] = exp->operands[operandIdx++];
        }

        for(int j = 0; j < e->minHops; j++) {
            iexp->operands[iexp->operand_count++] = exp->operands[operandIdx++];
        }

        if(dest->mat) {
            iexp->operands[iexp->operand_count++] = exp->operands[operandIdx++];
        }

        /* If intermidate node is referenced, create a new algebraic expression. */
        if(_intermidate_node(dest) && _referred_node(dest, ref_entities)) {
            // Finalize current expression.
            iexp->dest_node = dest;

            /* Create a new algebraic expression. */
            iexp = _AE_MUL(exp->operand_count - operandIdx);
            iexp->operand_count = 0;
            iexp->src_node = expressions[expIdx-1]->dest_node;
            iexp->dest_node = exp->dest_node;
            expressions = array_append(expressions, iexp);
            expIdx++;
        }
    }

    return expressions;
}

static AlgebraicExpressionOperand _AlgebraicExpression_OperandFromNode(Node *n) {
    AlgebraicExpressionOperand op;   
    op.free = false;
    op.transpose = false;
    op.operand = Node_GetMatrix(n);
    return op;
}

static AlgebraicExpressionOperand _AlgebraicExpression_OperandFromEdge(
    Edge *e,
    bool transpose,
    const AST* ast
) {
    AlgebraicExpressionOperand op;

    // TODO: find a better way for retriving AST graph entity.
    GrB_Matrix mat = Edge_GetMatrix(e);
    AST_LinkEntity *astEdge = (AST_LinkEntity*)MatchClause_GetEntity(ast->matchNode, e->alias);
    assert(astEdge);

    int labelCount = AST_LinkEntity_LabelCount(astEdge);
    bool freeMatrix = (labelCount > 1);
    // [:A|:B]
    // Create matrix M = A+B.
    if(labelCount > 1) {
        GraphContext *gc = GraphContext_GetFromTLS();
        Graph *g = gc->g;

        GrB_Matrix m;
        GrB_Matrix_new(&m, GrB_BOOL, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g));

        for(int i = 0; i < labelCount; i++) {
            char *label = astEdge->labels[i];
            Schema *s = GraphContext_GetSchema(gc, label, SCHEMA_EDGE);
            if(!s) continue;
            GrB_Matrix l = Graph_GetRelationMatrix(g, s->id);
            GrB_Info info = GrB_eWiseAdd_Matrix_Semiring(m, NULL, NULL, Rg_structured_bool, m, l, NULL);
        }
        mat = m;
    }

    op.operand = mat;
    op.free = freeMatrix;
    op.transpose = transpose;
    return op;
}

AlgebraicExpression **AlgebraicExpression_From_QueryGraph(const QueryGraph *qg, const AST *ast) {
    /* Construct algebraic expression(s) from query graph 
     * trying to take advantage of long multiplications with as few 
     * transpose as possible we'll transform paths crossing the graph 
     * "diameter", these are guarantee to be the longest, although
     * there might be situations in which these are not the most optimal paths
     * to explore.
     * 
     * Once a path been transformed it's removed from the graph and the process
     * repeat itself. */

    assert(qg);
    
    Node *src;
    Node *dest;
    size_t exp_count;
    AlgebraicExpression *exp;
    AlgebraicExpressionOperand op;
    AlgebraicExpression **exps = array_new(AlgebraicExpression*, 1);
    QueryGraph *g = QueryGraph_Clone(qg);

    // TODO: Have a single AST function which computes its referred entities.
    TrieMap *ref_entities = NewTrieMap();
    SetClause_ReferredEntities(ast->setNode, ref_entities);
    WithClause_ReferredEntities(ast->withNode, ref_entities);
    WhereClause_ReferredEntities(ast->whereNode, ref_entities);
    ReturnClause_ReferredEntities(ast->returnNode, ref_entities);
    CreateClause_ReferredEntities(ast->createNode, ref_entities);
    DeleteClause_ReferredEntities(ast->deleteNode, ref_entities);
    _referred_edge_ends(g, ref_entities);
    _referred_variable_length_edges_(g, ref_entities);

    // As long as there are nodes to process.
    while(g->node_count) {
        // Get leaf nodes at the deepest level.
        int level;
        Node **leafs = _DeepestLevel(g, &level);
        assert(array_len(leafs) > 0 && level >= 0);

        // Get a path of length level.
        Edge **path = DFS(leafs[0], level);
        assert(array_len(path) == level);

        // Construct expression.
        exp = _AE_MUL(level * 3 - 1);  // (3) Node Edge Node.

        // Scan path.
        for(int i = 0; i < level; i++) {
            Edge *e = path[i];
            src = e->src;
            dest = e->dest;

            // Determin if edge is transposed.
            bool transpose = false;
            if(i > 0) transpose = (path[i-1]->dest != e->src);
            if(transpose) {
                dest = e->src;
                src = e->dest;
            }

            // If src node has a label, multiple by label matrix.
            if(src->label) {
                op = _AlgebraicExpression_OperandFromNode(src);
                AlgebraicExpression_AppendOperand(exp, op);
                printf("%s ", src->label);
            }

            // Add Edge matrix.
            op = _AlgebraicExpression_OperandFromEdge(e, transpose, ast);
            for(int i = 0; i < e->minHops; i++) {
                AlgebraicExpression_AppendOperand(exp, op);
            }
            printf("%s ", e->relationship);
        }   // End of path traversal.

        // If last node on path has a label, multiply by label matrix.
        if(dest->label) {
            op = _AlgebraicExpression_OperandFromNode(dest);
            AlgebraicExpression_AppendOperand(exp, op);
            printf("%s ", dest->label);
        }

        // Set expression source and destination nodes.
        exp->src_node = path[0]->src;
        exp->dest_node = dest;

        // Split constructed expression into sub expressions.
        AlgebraicExpression **sub_exps = __AlgebraicExpression_Intermidate_Expressions__(exp, path, g, ref_entities);
        exp_count = array_len(sub_exps);
        // Frees sub_exps.
        sub_exps = _AlgebraicExpression_IsolateVariableLenExps_(sub_exps);

        // Remove path from graph.
        _RemovePathFromGraph(g, path);

        // Add constructed expression to return value.
        for(uint j = 0; j < array_len(sub_exps); j++) {
            exps = array_append(exps, sub_exps[j]);
        }
        
        // Clean up
        array_free(sub_exps);
        // TODO memory leak (fails on [a|b] relations?)
        // AlgebraicExpression_Free(exp);
        free(exp->operands);
        free(exp);
    }   // while(g->node_count)

    /* Because matrices are column ordered, when multiplying A*B
     * we need to reverse the order: B*A. */
    exp_count = array_len(exps);
    for(size_t i = 0; i < exp_count; i++) _AlgebraicExpression_ReverseOperandOrder(exps[i]);

    TrieMap_Free(ref_entities, TrieMap_NOP_CB);
    return exps;
}

AlgebraicExpression **AlgebraicExpression_From_Query(const AST *ast, Vector *matchPattern, const QueryGraph *q, size_t *exp_count) {
    assert(q->edge_count != 0);

    AlgebraicExpression *exp = _AE_MUL(q->edge_count + q->node_count);
    int transpose; // Indicate if matrix operand needs to be transposed.    
    Node *dest = NULL;
    Node *src = NULL;
    Edge *e = NULL;
    
    // Scan MATCH clause from left to right.
    for(int i = 0; i < Vector_Size(matchPattern); i++) {
        AST_GraphEntity *match_element;
        Vector_Get(matchPattern, i, &match_element);

        if(match_element->t != N_LINK) continue;
        AST_LinkEntity *astEdge = (AST_LinkEntity*)match_element;
        transpose = (astEdge->direction == N_RIGHT_TO_LEFT);
        e = QueryGraph_GetEdgeByAlias(q, astEdge->ge.alias);
        assert(e);

        // TODO: we might want to delay matrix retrieval even further.
        GrB_Matrix mat = Edge_GetMatrix(e);
        dest = e->dest;
        src = e->src;

        if(transpose) {
            dest = e->src;
            src = e->dest;
        }

        if(exp->operand_count == 0) {
            exp->src_node = src;
            if(src->label) {
                GrB_Matrix srcMat = Node_GetMatrix(src);
                AlgebraicExpression_AppendTerm(exp, srcMat, false, false);
            }
        }

        // ()-[:A|:B.]->()
        // Create matrix M, where M = A+B+...
        int labelCount = AST_LinkEntity_LabelCount(astEdge);
        if(labelCount > 1) {
            GraphContext *gc = GraphContext_GetFromTLS();
            Graph *g = gc->g;

            GrB_Matrix m;
            GrB_Matrix_new(&m, GrB_BOOL, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g));

            for(int i = 0; i < labelCount; i++) {
                char *label = astEdge->labels[i];
                Schema *s = GraphContext_GetSchema(gc, label, SCHEMA_EDGE);
                if(!s) continue;
                GrB_Matrix l = Graph_GetRelationMatrix(g, s->id);
                GrB_Info info = GrB_eWiseAdd_Matrix_Semiring(m, NULL, NULL, Rg_structured_bool, m, l, NULL);                
            }
            mat = m;
        }

        unsigned int hops = 1;
        /* Expand fixed variable length edge */
        if(astEdge->length && AST_LinkEntity_FixedLengthEdge(astEdge)) {
            hops = astEdge->length->minHops;
        }

        for(int i = 0; i < hops; i++) {
            bool freeMatrix = (labelCount > 1);
            AlgebraicExpression_AppendTerm(exp, mat, transpose, freeMatrix);
        }

        if(dest->label) {
            GrB_Matrix destMat = Node_GetMatrix(dest);
            AlgebraicExpression_AppendTerm(exp, destMat, false, false);
        }
    }

    exp->dest_node = dest;
    AlgebraicExpression **expressions = _AlgebraicExpression_Intermidate_Expressions(exp, ast, matchPattern, q, exp_count);
    expressions = _AlgebraicExpression_IsolateVariableLenExps(expressions, exp_count);
    // TODO memory leak (fails on [a|b] relations?)
    // AlgebraicExpression_Free(exp);
    free(exp->operands);
    free(exp);

    /* Because matrices are column ordered, when multiplying A*B
     * we need to reverse the order: B*A. */
    for(int i = 0; i < *exp_count; i++) _AlgebraicExpression_ReverseOperandOrder(expressions[i]);
    return expressions;
}

/* Evaluates an algebraic expression,
 * evaluation is done right to left due to matrix CSC representation 
 * the right most operand in the expression is a tiny extremely sparse matrix
 * this allows us to avoid computing multiplications of large matrices.
 * In the case an operand is marked for transpose, we will perform
 * the transpose once and update the expression. */
void AlgebraicExpression_Execute(AlgebraicExpression *ae, GrB_Matrix res) {
    assert(ae && res);
    size_t operand_count = ae->operand_count;
    assert(operand_count > 1);

    AlgebraicExpressionOperand leftTerm;
    AlgebraicExpressionOperand rightTerm;
    // Operate on a clone of the expression.
    AlgebraicExpressionOperand operands[operand_count];
    memcpy(operands, ae->operands, sizeof(AlgebraicExpressionOperand) * operand_count);

    /* Multiply right to left
     * A*B*C*D
     * X = C*D
     * Y = B*X
     * Z = A*Y */
    while(operand_count > 1) {
        // Multiply and reduce.
        rightTerm = operands[operand_count-1];
        leftTerm = operands[operand_count-2];

        /* Incase we're required to transpose left hand side operand 
         * perform transpose once and update original expression. */
        if(leftTerm.transpose) {
            GrB_Matrix t = leftTerm.operand;
            /* Graph matrices are immutable, create a new matrix. 
             * and transpose. */
            if(!leftTerm.free) {
                GrB_Index cols;
                GrB_Matrix_ncols(&cols, leftTerm.operand);
                GrB_Matrix_new(&t, GrB_BOOL, cols, cols);
            }
            GrB_transpose(t, GrB_NULL, GrB_NULL, leftTerm.operand, GrB_NULL);

            // Update local and original expressions.
            leftTerm.free = true;
            leftTerm.operand = t;
            leftTerm.transpose = false;
            ae->operands[operand_count-2].free = leftTerm.free;
            ae->operands[operand_count-2].operand = leftTerm.operand;
            ae->operands[operand_count-2].transpose = leftTerm.transpose;
        }

        _AlgebraicExpression_Execute_MUL(res, leftTerm.operand, rightTerm.operand, GrB_NULL);

        // Quick return if C is ZERO, there's no way to make progress.
        GrB_Index nvals = 0;
        GrB_Matrix_nvals(&nvals, res);
        if(nvals == 0) break;

        // Assign result and update operands count.
        operands[operand_count-2].operand = res;
        operand_count--;
    }
}

void AlgebraicExpression_RemoveTerm(AlgebraicExpression *ae, int idx, AlgebraicExpressionOperand *operand) {
    assert(idx >= 0 && idx < ae->operand_count);
    if(operand) *operand = ae->operands[idx];

    // Shift left.
    for(int i = idx; i < ae->operand_count-1; i++) {
        ae->operands[i] = ae->operands[i+1];
    }

    ae->operand_count--;
}

void AlgebraicExpression_Free(AlgebraicExpression* ae) {
    for(int i = 0; i < ae->operand_count; i++) {
        if(ae->operands[i].free) {
            GrB_Matrix_free(&ae->operands[i].operand);
        }
    }

    free(ae->operands);
    free(ae);
}

void AlgebraicExpression_Transpose(AlgebraicExpression *ae) {
    /* Actually transpose expression:
     * E = A*B*C 
     * Transpose(E) = 
     * = Transpose(A*B*C) = 
     * = CT*BT*AT */
    
    // Switch expression src and dest nodes.
    Node *n = ae->src_node;
    ae->src_node = ae->dest_node;
    ae->dest_node = n;

    _AlgebraicExpression_ReverseOperandOrder(ae);

    for(int i = 0; i < ae->operand_count; i++)
        ae->operands[i].transpose = !ae->operands[i].transpose;
}

AlgebraicExpressionNode *AlgebraicExpressionNode_NewOperationNode(AL_EXP_OP op) {
    AlgebraicExpressionNode *node = rm_calloc(1, sizeof(AlgebraicExpressionNode));
    node->type = AL_OPERATION;
    node->operation.op = op;
    node->operation.reusable = false;
    node->operation.v = NULL;
    node->operation.l = NULL;
    node->operation.r = NULL;
    return node;
}

AlgebraicExpressionNode *AlgebraicExpressionNode_NewOperandNode(GrB_Matrix operand) {
    AlgebraicExpressionNode *node = rm_calloc(1, sizeof(AlgebraicExpressionNode));
    node->type = AL_OPERAND;
    node->operand = operand;
    return node;
}

void AlgebraicExpressionNode_AppendLeftChild(AlgebraicExpressionNode *root, AlgebraicExpressionNode *child) {
    assert(root && root->type == AL_OPERATION && root->operation.l == NULL);
    root->operation.l = child;
}

void AlgebraicExpressionNode_AppendRightChild(AlgebraicExpressionNode *root, AlgebraicExpressionNode *child) {
    assert(root && root->type == AL_OPERATION && root->operation.r == NULL);
    root->operation.r = child;
}

// restructure tree
//              (*)
//      (*)               (+)
// (a)       (b)    (e0)       (e1)

// To
//               (+)
//       (*)                (*)  
// (ab)       (e0)    (ab)       (e1)

// Whenever we encounter a multiplication operation
// where one child is an addition operation and the other child
// is a multiplication operation, we'll replace root multiplication
// operation with an addition operation with two multiplication operations
// one for each child of the original addition operation, as can be seen above.
// we'll want to reuse the left handside of the multiplication.
void AlgebraicExpression_SumOfMul(AlgebraicExpressionNode **root) {
    if((*root)->type == AL_OPERATION && (*root)->operation.op == AL_EXP_MUL) {
        AlgebraicExpressionNode *l = (*root)->operation.l;
        AlgebraicExpressionNode *r = (*root)->operation.r;

        if((l->type == AL_OPERATION && l->operation.op == AL_EXP_ADD &&
            !(r->type == AL_OPERATION && r->operation.op == AL_EXP_ADD)) ||
            (r->type == AL_OPERATION && r->operation.op == AL_EXP_ADD &&
            !(l->type == AL_OPERATION && l->operation.op == AL_EXP_ADD))) {
            
            AlgebraicExpressionNode *add = AlgebraicExpressionNode_NewOperationNode(AL_EXP_ADD);
            AlgebraicExpressionNode *lMul = AlgebraicExpressionNode_NewOperationNode(AL_EXP_MUL);
            AlgebraicExpressionNode *rMul = AlgebraicExpressionNode_NewOperationNode(AL_EXP_MUL);

            AlgebraicExpressionNode_AppendLeftChild(add, lMul);
            AlgebraicExpressionNode_AppendRightChild(add, rMul);
            
            if(l->operation.op == AL_EXP_ADD) {
                // Lefthand side is addition, righthand side is multiplication.
                AlgebraicExpressionNode_AppendLeftChild(lMul, r);
                AlgebraicExpressionNode_AppendRightChild(lMul, l->operation.l);
                AlgebraicExpressionNode_AppendLeftChild(rMul, r);
                AlgebraicExpressionNode_AppendRightChild(rMul, l->operation.r);

                // Mark r as reusable.
                if(r->type == AL_OPERATION) r->operation.reusable = true;
            } else {
                // Righthand side is addition, lefthand side is multiplication.
                AlgebraicExpressionNode_AppendLeftChild(lMul, l);
                AlgebraicExpressionNode_AppendRightChild(lMul, r->operation.l);
                AlgebraicExpressionNode_AppendLeftChild(rMul, l);
                AlgebraicExpressionNode_AppendRightChild(rMul, r->operation.r);

                // Mark r as reusable.
                if(l->type == AL_OPERATION) l->operation.reusable = true;
            }

            // TODO: free old root.
            *root = add;
            AlgebraicExpression_SumOfMul(root);
        } else {
            AlgebraicExpression_SumOfMul(&l);
            AlgebraicExpression_SumOfMul(&r);
        }
    }
}

// Forward declaration.
static GrB_Matrix _AlgebraicExpression_Eval(AlgebraicExpressionNode *exp, GrB_Matrix res);

static GrB_Matrix _AlgebraicExpression_Eval_ADD(AlgebraicExpressionNode *exp, GrB_Matrix res) {
    // Expression already evaluated.
    if(exp->operation.v != NULL) return exp->operation.v;

    GrB_Index nrows;
    GrB_Index ncols;
    GrB_Matrix r = NULL;
    GrB_Matrix l = NULL;
    GrB_Matrix inter = NULL;
    GrB_Descriptor desc = NULL; // Descriptor used for transposing.
    AlgebraicExpressionNode *rightHand = exp->operation.r;
    AlgebraicExpressionNode *leftHand = exp->operation.l;

    // Determine if left or right expression needs to be transposed.
    if(leftHand && leftHand->type == AL_OPERATION && leftHand->operation.op == AL_EXP_TRANSPOSE) {
        if(!desc) GrB_Descriptor_new(&desc);
        GrB_Descriptor_set(desc, GrB_INP0, GrB_TRAN);
    }
    
    if(rightHand && rightHand->type == AL_OPERATION && rightHand->operation.op == AL_EXP_TRANSPOSE) {
        if(!desc) GrB_Descriptor_new(&desc);
        GrB_Descriptor_set(desc, GrB_INP1, GrB_TRAN);
    }

    // Evaluate right expressions.
    r = _AlgebraicExpression_Eval(exp->operation.r, res);

    // Evaluate left expressions,
    // if lefthandside expression requires a matrix
    // to hold its intermidate value allocate one here.
    if(leftHand->type == AL_OPERATION) {
        GrB_Matrix_nrows(&nrows, r);
        GrB_Matrix_ncols(&ncols, r);
        GrB_Matrix_new(&inter, GrB_BOOL, nrows, ncols);
        l = _AlgebraicExpression_Eval(exp->operation.l, inter);
    } else {
        l = _AlgebraicExpression_Eval(exp->operation.l, NULL);
    }

    // Perform addition.
    assert(GrB_eWiseAdd_Matrix_Semiring(res, NULL, NULL, Rg_structured_bool, l, r, desc) == GrB_SUCCESS);
    if(inter) GrB_Matrix_free(&inter);

    // Store intermidate if expression is marked for reuse.
    // TODO: might want to use inter if available.
    if(exp->operation.reusable) {
        assert(exp->operation.v == NULL);
        GrB_Matrix_dup(&exp->operation.v, res);
    }

    if(desc) GrB_Descriptor_free(&desc);
    return res;
}

static GrB_Matrix _AlgebraicExpression_Eval_MUL(AlgebraicExpressionNode *exp, GrB_Matrix res) {
    // Expression already evaluated.
    if(exp->operation.v != NULL) return exp->operation.v;

    GrB_Descriptor desc = NULL; // Descriptor used for transposing.
    AlgebraicExpressionNode *rightHand = exp->operation.r;
    AlgebraicExpressionNode *leftHand = exp->operation.l;

    // Determine if left or right expression needs to be transposed.
    if(leftHand && leftHand->type == AL_OPERATION && leftHand->operation.op == AL_EXP_TRANSPOSE) {
        if(!desc) GrB_Descriptor_new(&desc);
        GrB_Descriptor_set(desc, GrB_INP0, GrB_TRAN);
    }
    
    if(rightHand && rightHand->type == AL_OPERATION && rightHand->operation.op == AL_EXP_TRANSPOSE) {
        if(!desc) GrB_Descriptor_new(&desc);
        GrB_Descriptor_set(desc, GrB_INP1, GrB_TRAN);
    }

    // Evaluate right left expressions.
    GrB_Matrix r = _AlgebraicExpression_Eval(exp->operation.r, res);
    GrB_Matrix l = _AlgebraicExpression_Eval(exp->operation.l, res);

    // Perform multiplication.
    assert(GrB_mxm(res, NULL, NULL, Rg_structured_bool, l, r, desc) == GrB_SUCCESS);

    // Store intermidate if expression is marked for reuse.
    if(exp->operation.reusable) {
        assert(exp->operation.v == NULL);
        GrB_Matrix_dup(&exp->operation.v, res);
    }

    if(desc) GrB_Descriptor_free(&desc);
    return res;
}

static GrB_Matrix _AlgebraicExpression_Eval_TRANSPOSE(AlgebraicExpressionNode *exp, GrB_Matrix res) {
    // Transpose is an unary operation which gets delayed.
    AlgebraicExpressionNode *rightHand = exp->operation.r;
    AlgebraicExpressionNode *leftHand = exp->operation.l;

    assert( !(leftHand && rightHand) && (leftHand || rightHand) ); // Verify unary.
    if(leftHand) return _AlgebraicExpression_Eval(leftHand, res);
    else return _AlgebraicExpression_Eval(rightHand, res);
}

static GrB_Matrix _AlgebraicExpression_Eval(AlgebraicExpressionNode *exp, GrB_Matrix res) {
    if(exp == NULL) return NULL;
    if(exp->type == AL_OPERAND) return exp->operand;

    // Perform operation.
    switch(exp->operation.op) {
        case AL_EXP_MUL:
            return _AlgebraicExpression_Eval_MUL(exp, res);

        case AL_EXP_ADD:
            return _AlgebraicExpression_Eval_ADD(exp, res);

        case AL_EXP_TRANSPOSE:
            return _AlgebraicExpression_Eval_TRANSPOSE(exp, res);

        default:
            assert(false);
    }    
    return res;
}

void AlgebraicExpression_Eval(AlgebraicExpressionNode *exp, GrB_Matrix res) {
    _AlgebraicExpression_Eval(exp, res);
}

static void _AlgebraicExpressionNode_UniqueNodes(AlgebraicExpressionNode *root, AlgebraicExpressionNode ***uniqueNodes) {
    if(!root) return;

    // Have we seen this node before?
    int nodeCount = array_len(*uniqueNodes);
    for(int i = 0; i < nodeCount; i++) if(root == (*uniqueNodes)[i]) return;

    *uniqueNodes = array_append((*uniqueNodes), root);

    if(root->type != AL_OPERATION) return;

    _AlgebraicExpressionNode_UniqueNodes(root->operation.r, uniqueNodes);
    _AlgebraicExpressionNode_UniqueNodes(root->operation.l, uniqueNodes);
}

void AlgebraicExpressionNode_Free(AlgebraicExpressionNode *root) {
    if(!root) return;

    // Delay free for nodes which are referred from multiple points.
    AlgebraicExpressionNode **uniqueNodes = array_new(AlgebraicExpressionNode*, 1);
    _AlgebraicExpressionNode_UniqueNodes(root, &uniqueNodes);

    // Free unique nodes.
    AlgebraicExpressionNode *node;
    int nodesCount = array_len(uniqueNodes);
    for(int i = 0; i < nodesCount; i++) {
        node = array_pop(uniqueNodes);
        if(node->operation.v) GrB_Matrix_free(&node->operation.v);
        rm_free(node);
    }
    array_free(uniqueNodes);
}
