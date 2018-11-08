/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Apache License, Version 2.0,
* modified with the Commons Clause restriction.
*/

#ifndef __OPTIMIZE_QUICK_COUNT__
#define __OPTIMIZE_QUICK_COUNT__

#include "../execution_plan.h"
#include "../../parser/ast.h"

void skipCounting(ExecutionPlan *plan, AST_Query *ast);

#endif
