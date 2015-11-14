

public class LocalPartitioner extends AbstractPartitioner
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new LocalToken(null, null));

    private final AbstractType<?> comparator;

    public LocalPartitioner(AbstractType<?> comparator)
    {
        this.comparator = comparator;
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return new BufferDecoratedKey(getToken(key), key);
    }

    public Token midpoint(Token left, Token right)
    {
        throw new UnsupportedOperationException();
    }

    public LocalToken getMinimumToken()
    {
        return new LocalToken(comparator, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    public LocalToken getToken(ByteBuffer key)
    {
        return new LocalToken(comparator, key);
    }

    public long getHeapSizeOf(Token token)
    {
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(((LocalToken) token).token);
    }

    public LocalToken getRandomToken()
    {
        throw new UnsupportedOperationException();
    }

    public Token.TokenFactory getTokenFactory()
    {
        throw new UnsupportedOperationException();
    }

    public boolean preservesOrder()
    {
        return true;
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        return Collections.singletonMap((Token)getMinimumToken(), new Float(1.0));
    }

    public AbstractType<?> getTokenValidator()
    {
        return comparator;
    }
}
