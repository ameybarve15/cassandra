

/**
 * Wraps an ICache in requests + hits tracking.
 */
public class InstrumentingCache<K, V>
{
    private volatile boolean capacitySetManually;
    private final ICache<K, V> map;
    private final String type;

    private CacheMetrics metrics;

    public InstrumentingCache(String type, ICache<K, V> map)
    {
        this.map = map;
        this.type = type;
        this.metrics = new CacheMetrics(type, map);
    }

    public void put(K key, V value)
    {
        map.put(key, value);
    }

    public boolean putIfAbsent(K key, V value)
    {
        return map.putIfAbsent(key, value);
    }

    public boolean replace(K key, V old, V value)
    {
        return map.replace(key, old, value);
    }

    public V get(K key)
    {
        V v = map.get(key);
        metrics.requests.mark();
        if (v != null)
            metrics.hits.mark();
        return v;
    }

    public V getInternal(K key)
    {
        return map.get(key);
    }

    public void remove(K key)
    {
        map.remove(key);
    }

    public long getCapacity()
    {
        return map.capacity();
    }

    public boolean isCapacitySetManually()
    {
        return capacitySetManually;
    }

    public void updateCapacity(long capacity)
    {
        map.setCapacity(capacity);
    }

    public void setCapacity(long capacity)
    {
        updateCapacity(capacity);
        capacitySetManually = true;
    }

    public int size()
    {
        return map.size();
    }

    public long weightedSize()
    {
        return map.weightedSize();
    }

    public void clear()
    {
        map.clear();
        metrics = new CacheMetrics(type, map);
    }

    public Set<K> getKeySet()
    {
        return map.keySet();
    }

    public Set<K> hotKeySet(int n)
    {
        return map.hotKeySet(n);
    }

    public boolean containsKey(K key)
    {
        return map.containsKey(key);
    }

    public CacheMetrics getMetrics()
    {
        return metrics;
    }
}
