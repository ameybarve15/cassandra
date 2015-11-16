

public class CacheService implements CacheServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Caches";

    public static enum CacheType
    {
        KEY_CACHE("KeyCache"),
        ROW_CACHE("RowCache"),
        COUNTER_CACHE("CounterCache");

        private final String name;

        private CacheType(String typeName)
        {
            name = typeName;
        }

        public String toString()
        {
            return name;
        }
    }

    public final static CacheService instance = new CacheService();

    public final AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache;
    public final AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache;
    public final AutoSavingCache<CounterCacheKey, ClockAndCount> counterCache;

    private CacheService()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        mbs.registerMBean(this, new ObjectName(MBEAN_NAME));

        keyCache = initKeyCache();
        rowCache = initRowCache();
        counterCache = initCounterCache();
    }

    /**
     * @return auto saving cache object
     */
    private AutoSavingCache<KeyCacheKey, RowIndexEntry> initKeyCache()
    {
        long keyCacheInMemoryCapacity = DatabaseDescriptor.getKeyCacheSizeInMB() * 1024 * 1024;

        // as values are constant size we can use singleton weigher
        // where 48 = 40 bytes (average size of the key) + 8 bytes (size of value)
        ICache<KeyCacheKey, RowIndexEntry> kc;
        kc = ConcurrentLinkedHashCache.create(keyCacheInMemoryCapacity);
        AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache = new AutoSavingCache<>(kc, CacheType.KEY_CACHE, new KeyCacheSerializer());

        int keyCacheKeysToSave = DatabaseDescriptor.getKeyCacheKeysToSave();

        keyCache.scheduleSaving(DatabaseDescriptor.getKeyCacheSavePeriod(), keyCacheKeysToSave);

        return keyCache;
    }

    /**
     * @return initialized row cache
     */
    private AutoSavingCache<RowCacheKey, IRowCacheEntry> initRowCache()
    {
        long rowCacheInMemoryCapacity = DatabaseDescriptor.getRowCacheSizeInMB() * 1024 * 1024;

        // cache object
        ICache<RowCacheKey, IRowCacheEntry> rc = new SerializingCacheProvider().create(rowCacheInMemoryCapacity);
        AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = new AutoSavingCache<>(rc, CacheType.ROW_CACHE, new RowCacheSerializer());

        int rowCacheKeysToSave = DatabaseDescriptor.getRowCacheKeysToSave();

        rowCache.scheduleSaving(DatabaseDescriptor.getRowCacheSavePeriod(), rowCacheKeysToSave);

        return rowCache;
    }

    private AutoSavingCache<CounterCacheKey, ClockAndCount> initCounterCache()
    {
        long capacity = DatabaseDescriptor.getCounterCacheSizeInMB() * 1024 * 1024;

        AutoSavingCache<CounterCacheKey, ClockAndCount> cache =
            new AutoSavingCache<>(ConcurrentLinkedHashCache.<CounterCacheKey, ClockAndCount>create(capacity),
                                  CacheType.COUNTER_CACHE,
                                  new CounterCacheSerializer());

        int keysToSave = DatabaseDescriptor.getCounterCacheKeysToSave();

        cache.scheduleSaving(DatabaseDescriptor.getCounterCacheSavePeriod(), keysToSave);

        return cache;
    }

    public long getKeyCacheHits()
    {
        return keyCache.getMetrics().hits.count();
    }

    public long getRowCacheHits()
    {
        return rowCache.getMetrics().hits.count();
    }

    public long getKeyCacheRequests()
    {
        return keyCache.getMetrics().requests.count();
    }

    public long getRowCacheRequests()
    {
        return rowCache.getMetrics().requests.count();
    }

    public double getKeyCacheRecentHitRate()
    {
        return keyCache.getMetrics().getRecentHitRate();
    }

    public double getRowCacheRecentHitRate()
    {
        return rowCache.getMetrics().getRecentHitRate();
    }

    public int getRowCacheSavePeriodInSeconds()
    {
        return DatabaseDescriptor.getRowCacheSavePeriod();
    }

    public void setRowCacheSavePeriodInSeconds(int seconds)
    {
        DatabaseDescriptor.setRowCacheSavePeriod(seconds);
        rowCache.scheduleSaving(seconds, DatabaseDescriptor.getRowCacheKeysToSave());
    }

    public int getKeyCacheSavePeriodInSeconds()
    {
        return DatabaseDescriptor.getKeyCacheSavePeriod();
    }

    public void setKeyCacheSavePeriodInSeconds(int seconds)
    {
        DatabaseDescriptor.setKeyCacheSavePeriod(seconds);
        keyCache.scheduleSaving(seconds, DatabaseDescriptor.getKeyCacheKeysToSave());
    }

    public int getCounterCacheSavePeriodInSeconds()
    {
        return DatabaseDescriptor.getCounterCacheSavePeriod();
    }

    public void setCounterCacheSavePeriodInSeconds(int seconds)
    {
        DatabaseDescriptor.setCounterCacheSavePeriod(seconds);
        counterCache.scheduleSaving(seconds, DatabaseDescriptor.getCounterCacheKeysToSave());
    }

    public int getRowCacheKeysToSave()
    {
        return DatabaseDescriptor.getRowCacheKeysToSave();
    }

    public void setRowCacheKeysToSave(int count)
    {
        DatabaseDescriptor.setRowCacheKeysToSave(count);
        rowCache.scheduleSaving(getRowCacheSavePeriodInSeconds(), count);
    }

    public int getKeyCacheKeysToSave()
    {
        return DatabaseDescriptor.getKeyCacheKeysToSave();
    }

    public void setKeyCacheKeysToSave(int count)
    {
        DatabaseDescriptor.setKeyCacheKeysToSave(count);
        keyCache.scheduleSaving(getKeyCacheSavePeriodInSeconds(), count);
    }

    public int getCounterCacheKeysToSave()
    {
        return DatabaseDescriptor.getCounterCacheKeysToSave();
    }

    public void setCounterCacheKeysToSave(int count)
    {
        DatabaseDescriptor.setCounterCacheKeysToSave(count);
        counterCache.scheduleSaving(getCounterCacheSavePeriodInSeconds(), count);
    }

    public void invalidateKeyCache()
    {
        keyCache.clear();
    }

    public void invalidateKeyCacheForCf(Pair<String, String> ksAndCFName)
    {
        Iterator<KeyCacheKey> keyCacheIterator = keyCache.getKeySet().iterator();
        while (keyCacheIterator.hasNext())
        {
            KeyCacheKey key = keyCacheIterator.next();
            if (key.ksAndCFName.equals(ksAndCFName))
                keyCacheIterator.remove();
        }
    }

    public void invalidateRowCache()
    {
        rowCache.clear();
    }

    public void invalidateRowCacheForCf(Pair<String, String> ksAndCFName)
    {
        Iterator<RowCacheKey> rowCacheIterator = rowCache.getKeySet().iterator();
        while (rowCacheIterator.hasNext())
        {
            RowCacheKey rowCacheKey = rowCacheIterator.next();
            if (rowCacheKey.ksAndCFName.equals(ksAndCFName))
                rowCacheIterator.remove();
        }
    }

    public void invalidateCounterCacheForCf(Pair<String, String> ksAndCFName)
    {
        Iterator<CounterCacheKey> counterCacheIterator = counterCache.getKeySet().iterator();
        while (counterCacheIterator.hasNext())
        {
            CounterCacheKey counterCacheKey = counterCacheIterator.next();
            if (counterCacheKey.ksAndCFName.equals(ksAndCFName))
                counterCacheIterator.remove();
        }
    }

    public void invalidateCounterCache()
    {
        counterCache.clear();
    }

    public long getRowCacheCapacityInBytes()
    {
        return rowCache.getMetrics().capacity.value();
    }

    public long getRowCacheCapacityInMB()
    {
        return getRowCacheCapacityInBytes() / 1024 / 1024;
    }

    public void setRowCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        rowCache.setCapacity(capacity * 1024 * 1024);
    }

    public long getKeyCacheCapacityInBytes()
    {
        return keyCache.getMetrics().capacity.value();
    }

    public long getKeyCacheCapacityInMB()
    {
        return getKeyCacheCapacityInBytes() / 1024 / 1024;
    }

    public void setKeyCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        keyCache.setCapacity(capacity * 1024 * 1024);
    }

    public void setCounterCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        counterCache.setCapacity(capacity * 1024 * 1024);
    }

    public long getRowCacheSize()
    {
        return rowCache.getMetrics().size.value();
    }

    public long getRowCacheEntries()
    {
        return rowCache.size();
    }

    public long getKeyCacheSize()
    {
        return keyCache.getMetrics().size.value();
    }

    public long getKeyCacheEntries()
    {
        return keyCache.size();
    }

    public void saveCaches()
    {
        List<Future<?>> futures = new ArrayList<>(3);

        futures.add(keyCache.submitWrite(DatabaseDescriptor.getKeyCacheKeysToSave()));
        futures.add(rowCache.submitWrite(DatabaseDescriptor.getRowCacheKeysToSave()));
        futures.add(counterCache.submitWrite(DatabaseDescriptor.getCounterCacheKeysToSave()));

        FBUtilities.waitOnFutures(futures);
    }
}
