/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "execution_plan.h"

// TODO can possibly be deleted
uint ExecutionPlanSegment_GetRecordIDFromReference(ExecutionPlanSegment *segment, AST_IDENTIFIER entity) {
    uint *id = TrieMap_Find(segment->record_map, (char*)&entity, sizeof(entity));
    if (id == TRIEMAP_NOTFOUND) return IDENTIFIER_NOT_FOUND;
    return *id;
}

uint ExecutionPlanSegment_ReferenceToRecordID(ExecutionPlanSegment *segment, AST_IDENTIFIER identifier) {
    uint *id_ptr = TrieMap_Find(segment->record_map, (char*)&identifier, sizeof(identifier));
    if (id_ptr != TRIEMAP_NOTFOUND) return *id_ptr;

    id_ptr = rm_malloc(sizeof(uint));
    *id_ptr = segment->record_map->cardinality;
    TrieMap_Add(segment->record_map, (char*)&identifier, sizeof(identifier), id_ptr, TrieMap_DONT_CARE_REPLACE);

    return *id_ptr;
}

uint ExecutionPlanSegment_ExpressionToRecordID(ExecutionPlanSegment *segment, AR_ExpNode *exp) {
    uint *id_ptr = TrieMap_Find(segment->record_map, (char*)&exp, sizeof(AR_ExpNode));
    if (id_ptr != TRIEMAP_NOTFOUND) return *id_ptr;

    uint id = IDENTIFIER_NOT_FOUND;
    // If the expression contains an alias, map it first, and re-use its Record ID if one is already assigned
    if (exp->type == AR_EXP_OPERAND && exp->operand.type == AR_EXP_VARIADIC && exp->operand.variadic.entity_alias) {
        id = ExecutionPlanSegment_AliasToRecordID(segment, exp->operand.variadic.entity_alias, id);
    }

    if (id == IDENTIFIER_NOT_FOUND) id = segment->record_map->cardinality;

    id_ptr = rm_malloc(sizeof(uint));
    *id_ptr = id;
    TrieMap_Add(segment->record_map, (char*)&exp, sizeof(exp), id_ptr, TrieMap_DONT_CARE_REPLACE);

    return *id_ptr;
}

uint ExecutionPlanSegment_AliasToRecordID(ExecutionPlanSegment *segment, const char *alias, uint id) {
    if (id == IDENTIFIER_NOT_FOUND) {
        id = segment->record_map->cardinality;
    }

    uint *id_ptr = TrieMap_Find(segment->record_map, (char*)alias, strlen(alias));
    if (id_ptr != TRIEMAP_NOTFOUND) return *id_ptr;

    id_ptr = rm_malloc(sizeof(uint));
    *id_ptr = id;
    TrieMap_Add(segment->record_map, (char*)alias, strlen(alias), id_ptr, TrieMap_DONT_CARE_REPLACE);

    return *id_ptr;
}

uint ExecutionPlanSegment_RecordLength(ExecutionPlanSegment *segment) {
    return segment->record_len;
}

