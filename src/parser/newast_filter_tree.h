/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Apache License, Version 2.0,
 * modified with the Commons Clause restriction.
 */

#ifndef NEW_AST_FILTER_TREEH
#define NEW_AST_FILTER_TREEH
#include "newast.h"

/* filter_tree.h variants */
/* Nodes within the filter tree are one of two types
 * Either a predicate node or a condition node. */
typedef enum {
  TMP_FT_N_PRED,
  TMP_FT_N_COND,
} TMP_FT_FilterNodeType;

struct TMP_FT_FilterNode;

typedef struct {
	AR_ExpNode *lhs;
	AR_ExpNode *rhs;
	int op;					/* Operation (<, <=, =, =>, >, !). */
} TMP_FT_PredicateNode;

typedef struct {
	struct TMP_FT_FilterNode *left;
	struct TMP_FT_FilterNode *right;
	int op;	/* OR, AND */
} TMP_FT_ConditionNode;

/* All nodes within the filter tree are of type TMP_FT_FilterNode. */
struct TMP_FT_FilterNode {
  union {
    TMP_FT_PredicateNode pred;
    TMP_FT_ConditionNode cond;
  };
  TMP_FT_FilterNodeType t;	/* Determines actual type of this node. */
};

typedef struct TMP_FT_FilterNode TMP_FT_FilterNode;

TMP_FT_FilterNode* NEW_BuildFiltersTree(const NEWAST *ast);

#endif