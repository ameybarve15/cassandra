
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;

public class AutoSavingCache<K extends CacheKey, V> extends InstrumentingCache<K, V>
{
    public interface IStreamFactory
    {
        public InputStream getInputStream(File path) throws FileNotFoundException;
        public OutputStream getOutputStream(File path) throws FileNotFoundException;
    }


    /** True if a cache flush is currently executing: only one may execute at a time. */
    public static final Set<CacheService.CacheType> flushInProgress = new NonBlockingHashSet<CacheService.CacheType>();

    protected volatile ScheduledFuture<?> saveTask;
    protected final CacheService.CacheType cacheType;

    private CacheSerializer<K, V> cacheLoader;

    /*
     * CASSANDRA-10155 required a format change to fix 2i indexes and caching.
     * 2.2 is already at version "c" and 3.0 is at "d".
     *
     * Since cache versions match exactly and there is no partial fallback just add
     * a minor version letter.
     */
    private static final String CURRENT_VERSION = "ba";

    private static volatile IStreamFactory streamFactory = new IStreamFactory()
    {
        public InputStream getInputStream(File path) throws FileNotFoundException
        {
            return new FileInputStream(path);
        }

        public OutputStream getOutputStream(File path) throws FileNotFoundException
        {
            return new FileOutputStream(path);
        }
    };

    // Unused, but exposed for a reason. See CASSANDRA-8096.
    public static void setStreamFactory(IStreamFactory streamFactory)
    {
        AutoSavingCache.streamFactory = streamFactory;
    }

    public AutoSavingCache(ICache<K, V> cache, CacheService.CacheType cacheType, CacheSerializer<K, V> cacheloader)
    {
        super(cacheType.toString(), cache);
        this.cacheType = cacheType;
        this.cacheLoader = cacheloader;
    }

    public File getCachePath(String version)
    {
        return DatabaseDescriptor.getSerializedCachePath(cacheType, version);
    }

    public Writer getWriter(int keysToSave)
    {
        return new Writer(keysToSave);
    }

    public void scheduleSaving(int savePeriodInSeconds, final int keysToSave)
    {
        if (saveTask != null)
        {
            saveTask.cancel(false); // Do not interrupt an in-progress save
            saveTask = null;
        }
        if (savePeriodInSeconds > 0)
        {
            Runnable runnable = new Runnable()
            {
                public void run()
                {
                    submitWrite(keysToSave);
                }
            };
            saveTask = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(runnable,
                                                                               savePeriodInSeconds,
                                                                               savePeriodInSeconds,
                                                                               TimeUnit.SECONDS);
        }
    }

    public ListenableFuture<Integer> loadSavedAsync()
    {
        final ListeningExecutorService es = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        final long start = System.nanoTime();

        ListenableFuture<Integer> cacheLoad = es.submit(new Callable<Integer>()
        {
            @Override
            public Integer call() throws Exception
            {
                return loadSaved();
            }
        });
        cacheLoad.addListener(new Runnable() {
            @Override
            public void run()
            {
                if (size() > 0)
                    logger.info("Completed loading ({} ms; {} keys) {} cache",
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start),
                            CacheService.instance.keyCache.size(),
                            cacheType);
                es.shutdown();
            }
        }, MoreExecutors.sameThreadExecutor());

        return cacheLoad;
    }

    public int loadSaved()
    {
        int count = 0;
        long start = System.nanoTime();

        // modern format, allows both key and value (so key cache load can be purely sequential)
        File path = getCachePath(CURRENT_VERSION);
        if (path.exists())
        {
            DataInputStream in = null;
            try
            {
                in = new DataInputStream(new LengthAvailableInputStream(new BufferedInputStream(streamFactory.getInputStream(path)), path.length()));

                //Check the schema has not changed since CFs are looked up by name which is ambiguous
                UUID schemaVersion = new UUID(in.readLong(), in.readLong());
                if (!schemaVersion.equals(Schema.instance.getVersion()))
                    throw new RuntimeException("Cache schema version "
                                              + schemaVersion.toString()
                                              + " does not match current schema version "
                                              + Schema.instance.getVersion());

                ArrayDeque<Future<Pair<K, V>>> futures = new ArrayDeque<Future<Pair<K, V>>>();

                while (in.available() > 0)
                {
                    //ksname and cfname are serialized by the serializers in CacheService
                    //That is delegated there because there are serializer specific conditions
                    //where a cache key is skipped and not written
                    String ksname = in.readUTF();
                    String cfname = in.readUTF();

                    ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreIncludingIndexes(Pair.create(ksname, cfname));

                    Future<Pair<K, V>> entryFuture = cacheLoader.deserialize(in, cfs);
                    // Key cache entry can return null, if the SSTable doesn't exist.
                    if (entryFuture == null)
                        continue;

                    futures.offer(entryFuture);
                    count++;

                    /*
                     * Kind of unwise to accrue an unbounded number of pending futures
                     * So now there is this loop to keep a bounded number pending.
                     */
                    do
                    {
                        while (futures.peek() != null && futures.peek().isDone())
                        {
                            Future<Pair<K, V>> future = futures.poll();
                            Pair<K, V> entry = future.get();
                            if (entry != null && entry.right != null)
                                put(entry.left, entry.right);
                        }

                        if (futures.size() > 1000)
                            Thread.yield();
                    } while(futures.size() > 1000);
                }

                Future<Pair<K, V>> future = null;
                while ((future = futures.poll()) != null)
                {
                    Pair<K, V> entry = future.get();
                    if (entry != null && entry.right != null)
                        put(entry.left, entry.right);
                }
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.info(String.format("Harmless error reading saved cache %s", path.getAbsolutePath()), t);
            }
            finally
            {
                FileUtils.closeQuietly(in);
            }
        }

        return count;
    }

    public Future<?> submitWrite(int keysToSave)
    {
        return CompactionManager.instance.submitCacheWrite(getWriter(keysToSave));
    }

    public class Writer extends CompactionInfo.Holder
    {
        private final Set<K> keys;
        private final CompactionInfo info;
        private long keysWritten;

        protected Writer(int keysToSave)
        {
            if (keysToSave >= getKeySet().size())
                keys = getKeySet();
            else
                keys = hotKeySet(keysToSave);

            OperationType type;
            if (cacheType == CacheService.CacheType.KEY_CACHE)
                type = OperationType.KEY_CACHE_SAVE;
            else if (cacheType == CacheService.CacheType.ROW_CACHE)
                type = OperationType.ROW_CACHE_SAVE;
            else if (cacheType == CacheService.CacheType.COUNTER_CACHE)
                type = OperationType.COUNTER_CACHE_SAVE;
            else
                type = OperationType.UNKNOWN;

            info = new CompactionInfo(CFMetaData.denseCFMetaData(Keyspace.SYSTEM_KS, cacheType.toString(), BytesType.instance),
                                      type,
                                      0,
                                      keys.size(),
                                      "keys");
        }

        public CacheService.CacheType cacheType()
        {
            return cacheType;
        }

        public CompactionInfo getCompactionInfo()
        {
            // keyset can change in size, thus total can too
            return info.forProgress(keysWritten, Math.max(keysWritten, keys.size()));
        }

        public void saveCache()
        {
            logger.debug("Deleting old {} files.", cacheType);
            deleteOldCacheFiles();

            if (keys.isEmpty())
            {
                logger.debug("Skipping {} save, cache is empty.", cacheType);
                return;
            }

            long start = System.nanoTime();

            DataOutputStreamPlus writer = null;
            File tempCacheFile = tempCacheFile();
            try
            {
                try
                {
                    writer = new DataOutputStreamPlus(streamFactory.getOutputStream(tempCacheFile));
                }
                catch (FileNotFoundException e)
                {
                    throw new RuntimeException(e);
                }

                try
                {
                    //Need to be able to check schema version because CF names are ambiguous
                    UUID schemaVersion = Schema.instance.getVersion();
                    if (schemaVersion == null)
                    {
                        Schema.instance.updateVersion();
                        schemaVersion = Schema.instance.getVersion();
                    }
                    writer.writeLong(schemaVersion.getMostSignificantBits());
                    writer.writeLong(schemaVersion.getLeastSignificantBits());

                    for (K key : keys)
                    {

                        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreIncludingIndexes(key.ksAndCFName);
                        if (cfs == null)
                            continue; // the table or 2i has been dropped.

                        cacheLoader.serialize(key, writer, cfs);

                        keysWritten++;
                    }
                }
            }
            finally
            {
                if (writer != null)
                    FileUtils.closeQuietly(writer);
            }

            File cacheFile = getCachePath(CURRENT_VERSION);

            cacheFile.delete(); // ignore error if it didn't exist

            if (!tempCacheFile.renameTo(cacheFile))
                logger.error("Unable to rename {} to {}", tempCacheFile, cacheFile);

        }

        private File tempCacheFile()
        {
            File path = getCachePath(CURRENT_VERSION);
            return FileUtils.createTempFile(path.getName(), null, path.getParentFile());
        }

        private void deleteOldCacheFiles()
        {
            File savedCachesDir = new File(DatabaseDescriptor.getSavedCachesLocation());
            assert savedCachesDir.exists() && savedCachesDir.isDirectory();
            File[] files = savedCachesDir.listFiles();
            if (files != null)
            {
                for (File file : files)
                {
                    if (!file.isFile())
                        continue; // someone's been messing with our directory.  naughty!

                    if (file.getName().endsWith(cacheType.toString())
                            || file.getName().endsWith(String.format("%s-%s.db", cacheType.toString(), CURRENT_VERSION)))
                    {
                        file.delete();                    }
                }
            }
        }
    }

    public interface CacheSerializer<K extends CacheKey, V>
    {
        void serialize(K key, DataOutputPlus out, ColumnFamilyStore cfs) throws IOException;

        Future<Pair<K, V>> deserialize(DataInputStream in, ColumnFamilyStore cfs) throws IOException;
    }
}
