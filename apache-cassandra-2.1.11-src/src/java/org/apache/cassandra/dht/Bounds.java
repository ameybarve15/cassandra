
/**
 * AbstractBounds containing both its endpoints: [left, right].  Used by "classic" by-key range scans.
 */
public class Bounds<T extends RingPosition<T>> extends AbstractBounds<T>
{
    public Bounds(T left, T right)
    {
        this(left, right, StorageService.getPartitioner());
    }

    public Bounds(T left, T right, IPartitioner partitioner)
    {
        super(left, right, partitioner);
        // unlike a Range, a Bounds may not wrap
        assert left.compareTo(right) <= 0 || right.isMinimum(partitioner) : "[" + left + "," + right + "]";
    }

    public boolean contains(T position)
    {
        // Range.contains doesnt work correctly if left == right (unless both
        // are minimum) because for Range that means a wrapping range that select
        // the whole ring. So we must explicitely handle this case
        return left.equals(position) || ((right.isMinimum(partitioner) || !left.equals(right)) && Range.contains(left, right, position));
    }

    public Pair<AbstractBounds<T>, AbstractBounds<T>> split(T position)
    {
        assert contains(position);
        // Check if the split would have no effect on the range
        if (position.equals(right))
            return null;

        AbstractBounds<T> lb = new Bounds<T>(left, position, partitioner);
        AbstractBounds<T> rb = new Range<T>(position, right, partitioner);
        return Pair.create(lb, rb);
    }

    public boolean inclusiveLeft()
    {
        return true;
    }

    public boolean inclusiveRight()
    {
        return true;
    }

    public boolean intersects(Bounds<T> that)
    {
        // We either contains one of the that bounds, or we are fully contained into that.
        return contains(that.left) || contains(that.right) || that.contains(left);
    }

    public List<? extends AbstractBounds<T>> unwrap()
    {
        // Bounds objects never wrap
        return Collections.<AbstractBounds<T>>singletonList(this);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Bounds))
            return false;
        Bounds<?> rhs = (Bounds<?>)o;
        return left.equals(rhs.left) && right.equals(rhs.right);
    }

    @Override
    public String toString()
    {
        return "[" + left + "," + right + "]";
    }

    protected String getOpeningString()
    {
        return "[";
    }

    protected String getClosingString()
    {
        return "]";
    }

    /**
     * Compute a bounds of keys corresponding to a given bounds of token.
     */
    public static Bounds<RowPosition> makeRowBounds(Token left, Token right, IPartitioner partitioner)
    {
        return new Bounds<RowPosition>(left.minKeyBound(partitioner), right.maxKeyBound(partitioner), partitioner);
    }

    @SuppressWarnings("unchecked")
    public AbstractBounds<RowPosition> toRowBounds()
    {
        return (left instanceof Token) ? makeRowBounds((Token)left, (Token)right, partitioner) : (Bounds<RowPosition>)this;
    }

    @SuppressWarnings("unchecked")
    public AbstractBounds<Token> toTokenBounds()
    {
        return (left instanceof RowPosition) ? new Bounds<Token>(((RowPosition)left).getToken(), ((RowPosition)right).getToken(), partitioner) : (Bounds<Token>)this;
    }

    public AbstractBounds<T> withNewRight(T newRight)
    {
        return new Bounds<T>(left, newRight);
    }
}
