

abstract class AbstractPartitioner implements IPartitioner
{
    @SuppressWarnings("unchecked")
    public <R extends RingPosition<R>> R minValue(Class<R> klass)
    {
        Token minToken = getMinimumToken();
        if (minToken.getClass().equals(klass))
            return (R)minToken;
        else
            return (R)minToken.minKeyBound();
    }
}
