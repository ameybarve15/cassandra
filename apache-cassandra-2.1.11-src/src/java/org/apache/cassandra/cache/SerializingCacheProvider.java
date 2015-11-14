

public class SerializingCacheProvider
{
    public ICache<RowCacheKey, IRowCacheEntry> create(long capacity)
    {
        return SerializingCache.create(capacity, new RowCacheSerializer());
    }
}
