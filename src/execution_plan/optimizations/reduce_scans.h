/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef __REDUCE_SCANS_H__
#define __REDUCE_SCANS_H__

#include "../execution_plan.h"

/* The reduce scans optimizer searches the execution plans for 
<<<<<<< HEAD
 * SCAN operations which set node N, in-case there's an earlier
 * operation within the execution plan e.g. PROCEDURE-CALL which sets N 
 * then omit SCAN. */
=======
 * consecutive traversal and scan operations, in such cases
 * performing SCAN, will only slow us down, and so this optimization
 * will remove such SCAN operations. */

/* TODO: Once we'll have statistics regarding the number of different types
 * using a relation we'll be able to drop SCAN and additional typed matrix 
 * multiplication. */
>>>>>>> Rename ExecutionPlan functions
void reduceScans(ExecutionPlan *plan);

#endif
