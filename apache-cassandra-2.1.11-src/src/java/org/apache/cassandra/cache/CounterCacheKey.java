

public final class CounterCacheKey extends CacheKey
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new CounterCacheKey(null, ByteBufferUtil.EMPTY_BYTE_BUFFER, CellNames.simpleDense(ByteBuffer.allocate(1))));

    public final byte[] partitionKey;
    public final byte[] cellName;

    private CounterCacheKey(Pair<String, String> ksAndCFName, ByteBuffer partitionKey, CellName cellName)
    {
        super(ksAndCFName);
        this.partitionKey = ByteBufferUtil.getArray(partitionKey);
        this.cellName = ByteBufferUtil.getArray(cellName.toByteBuffer());
    }

    public static CounterCacheKey create(Pair<String, String> ksAndCFName, ByteBuffer partitionKey, CellName cellName)
    {
        return new CounterCacheKey(ksAndCFName, partitionKey, cellName);
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
               + ObjectSizes.sizeOfArray(partitionKey)
               + ObjectSizes.sizeOfArray(cellName);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CounterCacheKey))
            return false;

        CounterCacheKey cck = (CounterCacheKey) o;

        return ksAndCFName.equals(cck.ksAndCFName)
            && Arrays.equals(partitionKey, cck.partitionKey)
            && Arrays.equals(cellName, cck.cellName);
    }
}
