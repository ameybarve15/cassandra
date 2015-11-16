

public class FileCacheService
{
    private static final Logger logger = LoggerFactory.getLogger(FileCacheService.class);

    private static final long MEMORY_USAGE_THRESHOLD = DatabaseDescriptor.getFileCacheSizeInMB() * 1024L * 1024L;
    private static final int AFTER_ACCESS_EXPIRATION = 512; // in millis

    public static FileCacheService instance = new FileCacheService();

    private static final AtomicLong cacheKeyIdCounter = new AtomicLong();
    public static final class CacheKey
    {
        final long id;
        public CacheKey()
        {
            this.id = cacheKeyIdCounter.incrementAndGet();
        }
        public boolean equals(Object that)
        {
            return that instanceof CacheKey && ((CacheKey) that).id == this.id;
        }
        public int hashCode()
        {
            return (int) id;
        }
    }

    private static final Callable<CacheBucket> cacheForPathCreator = new Callable<CacheBucket>()
    {
        @Override
        public CacheBucket call()
        {
            return new CacheBucket();
        }
    };

    private static final AtomicInteger memoryUsage = new AtomicInteger();

    private final Cache<CacheKey, CacheBucket> cache;
    private final FileCacheMetrics metrics = new FileCacheMetrics();

    private static final class CacheBucket
    {
        final ConcurrentLinkedQueue<RandomAccessReader> queue = new ConcurrentLinkedQueue<>();
        volatile boolean discarded = false;
    }

    protected FileCacheService()
    {
        RemovalListener<CacheKey, CacheBucket> onRemove = new RemovalListener<CacheKey, CacheBucket>()
        {
            @Override
            public void onRemoval(RemovalNotification<CacheKey, CacheBucket> notification)
            {
                CacheBucket bucket = notification.getValue();
                if (bucket == null)
                    return;

                // set discarded before deallocating the readers, to ensure we don't leak any
                bucket.discarded = true;
                Queue<RandomAccessReader> q = bucket.queue;
                boolean first = true;
                for (RandomAccessReader reader = q.poll() ; reader != null ; reader = q.poll())
                {
                    if (logger.isDebugEnabled() && first)
                    {
                        logger.debug("Evicting cold readers for {}", reader.getPath());
                        first = false;
                    }
                    memoryUsage.addAndGet(-1 * reader.getTotalBufferSize());
                    reader.deallocate();
                }
            }
        };

        cache = CacheBuilder.newBuilder()
                .expireAfterAccess(AFTER_ACCESS_EXPIRATION, TimeUnit.MILLISECONDS)
                .concurrencyLevel(DatabaseDescriptor.getConcurrentReaders())
                .removalListener(onRemove)
                .initialCapacity(16 << 10)
                .build();
    }

    public RandomAccessReader get(CacheKey key)
    {
        metrics.requests.mark();

        CacheBucket bucket = getCacheFor(key);
        RandomAccessReader result = bucket.queue.poll();
        if (result != null)
        {
            metrics.hits.mark();
            memoryUsage.addAndGet(-result.getTotalBufferSize());
        }

        return result;
    }

    private CacheBucket getCacheFor(CacheKey key)
    {
        return cache.get(key, cacheForPathCreator);
    }

    public void put(CacheKey cacheKey, RandomAccessReader instance)
    {
        int memoryUsed = memoryUsage.get();
        
        CacheBucket bucket = cache.getIfPresent(cacheKey);
        if (memoryUsed >= MEMORY_USAGE_THRESHOLD || bucket == null)
        {
            instance.deallocate();
        }
        else
        {
            memoryUsage.addAndGet(instance.getTotalBufferSize());
            bucket.queue.add(instance);
            if (bucket.discarded)
            {
                RandomAccessReader reader = bucket.queue.poll();
                if (reader != null)
                {
                    memoryUsage.addAndGet(-1 * reader.getTotalBufferSize());
                    reader.deallocate();
                }
            }
        }
    }

    public void invalidate(CacheKey cacheKey, String path)
    {
        cache.invalidate(cacheKey);
    }

    // TODO: this method is unsafe, as it calls getTotalBufferSize() on items that can have been discarded
    public long sizeInBytes()
    {
        long n = 0;
        for (CacheBucket bucket : cache.asMap().values())
            for (RandomAccessReader reader : bucket.queue)
                n += reader.getTotalBufferSize();
        return n;
    }
}
