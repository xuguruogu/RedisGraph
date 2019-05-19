#include "rax_cache_storage.h"
#include "../../util/rmalloc.h"

void insertToCache(RaxCacheStorage *raxCacheStorage, char *hashKey, CacheData *cacheData)
{
    raxInsert(raxCacheStorage->rt, (unsigned char *)hashKey, HASH_KEY_LENGTH, cacheData, NULL);
}

void removeFromCache(RaxCacheStorage *raxCacheStorage, char *hashKey)
{
    raxRemove(raxCacheStorage->rt, (unsigned char *)hashKey, HASH_KEY_LENGTH, NULL);
}

ResultSet *getFromCache(RaxCacheStorage *raxCacheStorage, char *hashKey)
{
    CacheData *data = raxFind(raxCacheStorage->rt, (unsigned char *)hashKey, HASH_KEY_LENGTH);
    if (data == raxNotFound)
    {
        return NULL;
    }
    return data->resultSet;
}

void RaxCacheStorage_Free(RaxCacheStorage *raxCacheStorage)
{
    raxFree(raxCacheStorage->rt);
    rm_free(raxCacheStorage);
}

RaxCacheStorage *RaxCacheStorage_New()
{
    RaxCacheStorage *raxCacheStorage = rm_malloc(sizeof(RaxCacheStorage));
    raxCacheStorage->rt = raxNew();
    return raxCacheStorage;
}