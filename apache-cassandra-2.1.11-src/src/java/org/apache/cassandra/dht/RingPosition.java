

/**
 * Interface representing a position on the ring.
 * Both Token and DecoratedKey represent a position in the ring, a token being
 * less precise than a DecoratedKey (a token is really a range of keys).
 */
public interface RingPosition<C extends RingPosition<C>> extends Comparable<C>
{
    public Token getToken();
    public boolean isMinimum(IPartitioner partitioner);
}
