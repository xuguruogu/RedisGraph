#include "newast_filter_tree.h"
#include "../util/arr.h"
#include "../arithmetic/arithmetic_expression.h"

TMP_FT_FilterNode* TMP__CreatePredicateFilterNode(int op, AR_ExpNode *lhs, AR_ExpNode *rhs) {
    TMP_FT_FilterNode *filterNode = malloc(sizeof(TMP_FT_FilterNode));
    filterNode->t= TMP_FT_N_PRED;
    filterNode->pred.op = op;
    filterNode->pred.lhs = lhs;
    filterNode->pred.rhs = rhs;
    return filterNode;
}

TMP_FT_FilterNode *TMP_AppendLeftChild(TMP_FT_FilterNode *root, TMP_FT_FilterNode *child) {
    root->cond.left = child;
    return root->cond.left;
}

TMP_FT_FilterNode *TMP_AppendRightChild(TMP_FT_FilterNode *root, TMP_FT_FilterNode *child) {
    root->cond.right = child;
    return root->cond.right;
}

TMP_FT_FilterNode* TMP_CreateCondFilterNode(int op) {
    TMP_FT_FilterNode* filterNode = malloc(sizeof(TMP_FT_FilterNode));
    filterNode->t = TMP_FT_N_COND;
    filterNode->cond.op = op;
    return filterNode;
}

void TMP_FT_Append(TMP_FT_FilterNode **root_ptr, TMP_FT_FilterNode *child) {
    assert(child);

    TMP_FT_FilterNode *root = *root_ptr;
    // If the tree is uninitialized, its root is the child
    if (root == NULL) {
        root_ptr = &child;
        return;
    }

    // Promote predicate node to AND condition filter
    if (root->t == TMP_FT_N_PRED) {
        TMP_FT_FilterNode *new_root = TMP_CreateCondFilterNode(AND);
        TMP_AppendLeftChild(new_root, root);
        TMP_AppendRightChild(root, child);
        root_ptr = &new_root;
    }

    if (root->cond.left == NULL) {
        TMP_AppendLeftChild(root, child);
    } else if (root->cond.right == NULL) {
        TMP_AppendRightChild(root, child);
    } else {
        TMP_FT_FilterNode *new_cond = TMP_CreateCondFilterNode(AND);
        TMP_AppendLeftChild(new_cond, root->cond.right);
        TMP_AppendRightChild(new_cond, child);
    }
}

// TODO unary operators (especially NOT)

int _convertOp(const cypher_operator_t *op) {
    // TODO ordered by precedence, which I don't know if we're managing properly right now
    if (op == CYPHER_OP_OR) {
        return OR;
    } else if (op == CYPHER_OP_XOR) {

    } else if (op == CYPHER_OP_AND) {
        return AND;
    } else if (op == CYPHER_OP_NOT) {
        // return NOT;
    } else if (op == CYPHER_OP_EQUAL) {
        return EQ;
    } else if (op == CYPHER_OP_NEQUAL) {
        return NE;
    } else if (op == CYPHER_OP_LT) {
        return LT;
    } else if (op == CYPHER_OP_GT) {
        return GT;
    } else if (op == CYPHER_OP_LTE) {
        return LE;
    } else if (op == CYPHER_OP_GTE) {
        return GE;
    } else if (op == CYPHER_OP_PLUS) {
        return ADD;
    } // TODO continue

    return -1;
}

// AND, OR, XOR,
TMP_FT_FilterNode* _convertBinaryOperator(const NEWAST *ast, const cypher_astnode_t *op_node) {
    const cypher_operator_t *operator = cypher_ast_binary_operator_get_operator(op_node);
    const cypher_astnode_t *lhs_node = cypher_ast_binary_operator_get_argument1(op_node);
    const cypher_astnode_t *rhs_node = cypher_ast_binary_operator_get_argument2(op_node);


    int op = _convertOp(operator);
    TMP_FT_FilterNode *filter = TMP_CreateCondFilterNode(OR);
    // switch (op) {
        // case OR:
            // filter = TMP_CreateCondFilterNode(OR);
            // break;
        // case AND:
    // }

    AR_ExpNode *lhs = AR_EXP_FromExpression(ast, lhs_node);
    AR_ExpNode *rhs = AR_EXP_FromExpression(ast, rhs_node);
    return TMP__CreatePredicateFilterNode(op, lhs, rhs);
    // TMP_AppendLeftChild(filter, lhs);
    // TMP_AppendRightChild(filter, rhs);
    // return filter;
}

/* A comparison node contains two arrays - one of operators, and one of expressions.
 * Most comparisons will only have one operator and two expressions, but Cypher
 * allows more complex formulations like "x < y <= z". */
TMP_FT_FilterNode* _convertComparison(const NEWAST *ast, const cypher_astnode_t *comparison_node) {
    unsigned int nelems = cypher_ast_comparison_get_length(comparison_node);
    assert(nelems == 1); // TODO tmp, but may require modifying tree formation.

    const cypher_operator_t *operator = cypher_ast_comparison_get_operator(comparison_node, 0);
    int op = _convertOp(operator);

    // All arguments are of type CYPHER_AST_EXPRESSION
    const cypher_astnode_t *lhs_node = cypher_ast_comparison_get_argument(comparison_node, 0);
    const cypher_astnode_t *rhs_node = cypher_ast_comparison_get_argument(comparison_node, 1);
    AR_ExpNode *lhs = AR_EXP_FromExpression(ast, lhs_node);
    AR_ExpNode *rhs = AR_EXP_FromExpression(ast, rhs_node);

    return TMP__CreatePredicateFilterNode(op, lhs, rhs);
}

// TODO unused
TMP_FT_FilterNode* FilterNode_FromAST(const NEWAST *ast, const cypher_astnode_t *expr) {
    assert(expr);
    cypher_astnode_type_t type = cypher_astnode_type(expr);
    if (type == CYPHER_AST_BINARY_OPERATOR) {
        return _convertBinaryOperator(ast, expr);
    } else if (type == CYPHER_AST_COMPARISON) {
        return _convertComparison(ast, expr);
    } else {
        // AR_ExpNode *lhs = AR_EXP_FromExpression(ast, expr);
    }
    assert(false);
    return NULL;
}

// void _convertInlinedProperties(const NEWAST *ast, const char *alias, const cypher_astnode_t *map) {
TMP_FT_FilterNode* _convertInlinedProperties(const NEWAST *ast, const cypher_astnode_t *entity, int type) {
    const cypher_astnode_t *props = NULL;
    const cypher_astnode_t *alias_node = NULL;

    if (type == SCHEMA_NODE) {
        props = cypher_ast_node_pattern_get_properties(entity);
        alias_node = cypher_ast_node_pattern_get_identifier(entity);
    } else { // relation
        props = cypher_ast_rel_pattern_get_properties(entity);
        alias_node =  cypher_ast_rel_pattern_get_identifier(entity);
    }

    if (!props) return NULL;
    assert(alias_node); // TODO valid?
    const char *alias = cypher_ast_identifier_get_name(alias_node);

    TMP_FT_FilterNode *root = NULL;
    unsigned int nelems = cypher_ast_map_nentries(props);
    for (unsigned int i = 0; i < nelems; i ++) {
        // key is of type CYPHER_AST_PROP_NAME
        const cypher_astnode_t *key = cypher_ast_map_get_key(props, i);
        const char *prop = cypher_ast_prop_name_get_value(key); // TODO can inline with above
        // TODO passing NULL entity, maybe inappropriate?
        // Might not even want a variable like this.
        AR_ExpNode *lhs = AR_EXP_NewVariableOperandNode(ast, NULL, alias, prop);
        // val is of type CYPHER_AST_EXPRESSION
        const cypher_astnode_t *val = cypher_ast_map_get_value(props, i);
        AR_ExpNode *rhs = AR_EXP_FromExpression(ast, val);
        TMP_FT_FilterNode *t = TMP__CreatePredicateFilterNode(EQ, lhs, rhs);
        TMP_FT_Append(&root, t);
    }
    return root;
}

void _collectFilters(const NEWAST *ast, TMP_FT_FilterNode *root, const cypher_astnode_t *entity) {
    if (!entity) return;

    cypher_astnode_type_t type = cypher_astnode_type(entity);

    TMP_FT_FilterNode *node = NULL;
    // If the current entity is a node or edge pattern, capture its properties map (if any)
    if (type == CYPHER_AST_NODE_PATTERN) {
        node = _convertInlinedProperties(ast, entity, SCHEMA_NODE); // TODO choose better type argument
    } else if (type == CYPHER_AST_REL_PATTERN) {
        node = _convertInlinedProperties(ast, entity, SCHEMA_EDGE); // TODO choose better type argument
    } else if (type == CYPHER_AST_COMPARISON) {
        node = _convertComparison(ast, entity);
    } else if (type == CYPHER_AST_BINARY_OPERATOR) {
        node = _convertBinaryOperator(ast, entity);
    } else if (type == CYPHER_AST_UNARY_OPERATOR) {
        // TODO, also n-ary maybe
    } else {
        unsigned int child_count = cypher_astnode_nchildren(entity);
        for(unsigned int i = 0; i < child_count; i++) {
            const cypher_astnode_t *child = cypher_astnode_get_child(entity, i);
            // Recursively continue searching
            _collectFilters(ast, root, child);
        }
    }
    if (node) TMP_FT_Append(&root, node);
}

TMP_FT_FilterNode* NEW_BuildFiltersTree(const NEWAST *ast) {
    // TODO should build array with one tree (or NULL) per MATCH clause
    TMP_FT_FilterNode *filter_tree = NULL;

    unsigned int clause_count = cypher_astnode_nchildren(ast->root);
    const cypher_astnode_t *match_clauses[clause_count];
    unsigned int match_count = NewAST_GetTopLevelClauses(ast->root, CYPHER_AST_MATCH, match_clauses);
    for (unsigned int i = 0; i < match_count; i ++) {
        _collectFilters(ast, filter_tree, match_clauses[i]);
    }

    return filter_tree;
}

