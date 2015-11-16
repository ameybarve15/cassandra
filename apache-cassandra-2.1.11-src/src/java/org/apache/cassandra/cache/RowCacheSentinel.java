

/**
 * A sentinel object for row caches.  See comments to getThroughCache and CASSANDRA-3862.
 */
public class RowCacheSentinel implements IRowCacheEntry
{
    private static final AtomicLong generator = new AtomicLong();

    final long sentinelId;

    public RowCacheSentinel()
    {
        sentinelId = generator.getAndIncrement();
    }
}
