

public class ByteOrderedPartitioner extends AbstractByteOrderedPartitioner
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(MINIMUM);

    public BytesToken getToken(ByteBuffer key)
    {
        if (key.remaining() == 0)
            return MINIMUM;
        return new BytesToken(key);
    }

    @Override
    public long getHeapSizeOf(Token token)
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(((BytesToken) token).token);
    }
}
