

public interface CacheServiceMBean
{
    public int getRowCacheSavePeriodInSeconds();
    public void setRowCacheSavePeriodInSeconds(int rcspis);

    public int getKeyCacheSavePeriodInSeconds();
    public void setKeyCacheSavePeriodInSeconds(int kcspis);

    public int getCounterCacheSavePeriodInSeconds();
    public void setCounterCacheSavePeriodInSeconds(int ccspis);

    public int getRowCacheKeysToSave();
    public void setRowCacheKeysToSave(int rckts);

    public int getKeyCacheKeysToSave();
    public void setKeyCacheKeysToSave(int kckts);

    public int getCounterCacheKeysToSave();
    public void setCounterCacheKeysToSave(int cckts);

    /**
     * invalidate the key cache; for use after invalidating row cache
     */
    public void invalidateKeyCache();

    /**
     * invalidate the row cache; for use after bulk loading via BinaryMemtable
     */
    public void invalidateRowCache();

    public void invalidateCounterCache();

    public void setRowCacheCapacityInMB(long capacity);

    public void setKeyCacheCapacityInMB(long capacity);

    public void setCounterCacheCapacityInMB(long capacity);

    /**
     * save row and key caches
     *
     * @throws ExecutionException when attempting to retrieve the result of a task that aborted by throwing an exception
     * @throws InterruptedException when a thread is waiting, sleeping, or otherwise occupied, and the thread is interrupted, either before or during the activity.
     */
    public void saveCaches() throws ExecutionException, InterruptedException;

    //
    // remaining methods are provided for backwards compatibility; modern clients should use CacheMetrics instead
    //

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#hits
     */
    @Deprecated
    public long getKeyCacheHits();

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#hits
     */
    @Deprecated
    public long getRowCacheHits();

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#requests
     */
    @Deprecated
    public long getKeyCacheRequests();

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#requests
     */
    @Deprecated
    public long getRowCacheRequests();

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#hitRate
     */
    @Deprecated
    public double getKeyCacheRecentHitRate();

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#hitRate
     */
    @Deprecated
    public double getRowCacheRecentHitRate();

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#capacity
     */
    @Deprecated
    public long getRowCacheCapacityInMB();
    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#capacity
     */
    @Deprecated
    public long getRowCacheCapacityInBytes();

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#capacity
     */
    @Deprecated
    public long getKeyCacheCapacityInMB();
    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#capacity
     */
    @Deprecated
    public long getKeyCacheCapacityInBytes();

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#size
     */
    @Deprecated
    public long getRowCacheSize();

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#entries
     */
    @Deprecated
    public long getRowCacheEntries();

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#size
     */
    @Deprecated
    public long getKeyCacheSize();

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#entries
     */
    @Deprecated
    public long getKeyCacheEntries();
}
