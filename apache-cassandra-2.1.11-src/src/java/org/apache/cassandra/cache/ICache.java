

/**
 * This is similar to the Map interface, but requires maintaining a given capacity
 * and does not require put or remove to return values, which lets SerializingCache
 * be more efficient by avoiding deserialize except on get.
 */
public interface ICache<K, V>
{
    public long capacity();

    public void setCapacity(long capacity);

    public void put(K key, V value);

    public boolean putIfAbsent(K key, V value);

    public boolean replace(K key, V old, V value);

    public V get(K key);

    public void remove(K key);

    public int size();

    public long weightedSize();

    public void clear();

    public Set<K> keySet();

    public Set<K> hotKeySet(int n);

    public boolean containsKey(K key);
}
