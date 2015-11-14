
/**
 * Serializes cache values off-heap.
 */
public class SerializingCache<K, V> implements ICache<K, V>
{
    private static final TypeSizes ENCODED_TYPE_SIZES = TypeSizes.VINT;

    private static final int DEFAULT_CONCURENCY_LEVEL = 64;

    private final ConcurrentLinkedHashMap<K, RefCountedMemory> map;
    
    private SerializingCache(long capacity, Weigher<RefCountedMemory> weigher, ISerializer<V> serializer)
    {
        EvictionListener<K,RefCountedMemory> listener = new EvictionListener<K, RefCountedMemory>()
        {
            public void onEviction(K k, RefCountedMemory mem)
            {
                mem.unreference();
            }
        };

        this.map = new ConcurrentLinkedHashMap.Builder<K, RefCountedMemory>()
                   .weigher(weigher)
                   .maximumWeightedCapacity(capacity)
                   .concurrencyLevel(DEFAULT_CONCURENCY_LEVEL)
                   .listener(listener)
                   .build();
    }

    public static <K, V> SerializingCache<K, V> create(long weightedCapacity, Weigher<RefCountedMemory> weigher, ISerializer<V> serializer)
    {
        return new SerializingCache<>(weightedCapacity, weigher, serializer);
    }

    public static <K, V> SerializingCache<K, V> create(long weightedCapacity, ISerializer<V> serializer)
    {
        return create(weightedCapacity, new Weigher<RefCountedMemory>()
        {
            public int weightOf(RefCountedMemory value)
            {
                long size = value.size();
                assert size < Integer.MAX_VALUE : "Serialized size cannot be more than 2GB";
                return (int) size;
            }
        }, serializer);
    }

    public V get(K key)
    {
        RefCountedMemory mem = map.get(key);
        if (mem == null)
            return null;
        if (!mem.reference())
            return null;
        try
        {
            return deserialize(mem);
        }
        finally
        {
            mem.unreference();
        }
    }

    public void put(K key, V value)
    {
        RefCountedMemory mem = serialize(value);
        if (mem == null)
            return; // out of memory.  never mind.

        RefCountedMemory old;
        try
        {
            old = map.put(key, mem);
        }
        catch (Throwable t)
        {
            mem.unreference();
            throw t;
        }

        if (old != null)
            old.unreference();
    }

    public boolean putIfAbsent(K key, V value)
    {
        RefCountedMemory mem = serialize(value);
        if (mem == null)
            return false; // out of memory.  never mind.

        RefCountedMemory old;
        try
        {
            old = map.putIfAbsent(key, mem);
        }

        if (old != null)
            // the new value was not put, we've uselessly allocated some memory, free it
            mem.unreference();
        return old == null;
    }

    public boolean replace(K key, V oldToReplace, V value)
    {
        // if there is no old value in our map, we fail
        RefCountedMemory old = map.get(key);
        if (old == null)
            return false;

        V oldValue;
        // reference old guy before de-serializing
        if (!old.reference())
            return false; // we have already freed hence noop.

        oldValue = deserialize(old);
        old.unreference();

        if (!oldValue.equals(oldToReplace))
            return false;

        // see if the old value matches the one we want to replace
        RefCountedMemory mem = serialize(value);
        if (mem == null)
            return false; // out of memory.  never mind.

        boolean success;
        try
        {
            success = map.replace(key, old, mem);
        }

        if (success)
            old.unreference(); // so it will be eventually be cleaned
        else
            mem.unreference();
        return success;
    }

    public void remove(K key)
    {
        RefCountedMemory mem = map.remove(key);
        if (mem != null)
            mem.unreference();
    }
}
