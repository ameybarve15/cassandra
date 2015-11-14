

public final class RowCacheKey extends CacheKey
{
    public final byte[] key;

    private static final long EMPTY_SIZE = ObjectSizes.measure(new RowCacheKey(null, ByteBufferUtil.EMPTY_BYTE_BUFFER));

    public RowCacheKey(Pair<String, String> ksAndCFName, DecoratedKey key)
    {
        this(ksAndCFName, key.getKey());
    }

    public RowCacheKey(Pair<String, String> ksAndCFName, ByteBuffer key)
    {
        super(ksAndCFName);
        this.key = ByteBufferUtil.getArray(key);
        assert this.key != null;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(key);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RowCacheKey that = (RowCacheKey) o;

        return ksAndCFName.equals(that.ksAndCFName) && Arrays.equals(key, that.key);
    }
}
