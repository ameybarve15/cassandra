
/**
 * AbstractBounds containing only its left endpoint: [left, right).  Used by CQL key >= X AND key < Y range scans.
 */
public class IncludingExcludingBounds<T extends RingPosition<T>> extends AbstractBounds<T>
{
    public IncludingExcludingBounds(T left, T right)
    {
        this(left, right, StorageService.getPartitioner());
    }

    IncludingExcludingBounds(T left, T right, IPartitioner partitioner)
    {
        super(left, right, partitioner);
        // unlike a Range, an IncludingExcludingBounds may not wrap, nor have
        // right == left unless the right is the min token
        assert left.compareTo(right) < 0 || right.isMinimum(partitioner) : "[" + left + "," + right + ")";
    }

    public boolean contains(T position)
    {
        return (Range.contains(left, right, position) || left.equals(position)) && !right.equals(position);
    }

    public Pair<AbstractBounds<T>, AbstractBounds<T>> split(T position)
    {
        assert contains(position);
        AbstractBounds<T> lb = new Bounds<T>(left, position, partitioner);
        AbstractBounds<T> rb = new ExcludingBounds<T>(position, right, partitioner);
        return Pair.create(lb, rb);
    }

    public boolean inclusiveLeft()
    {
        return true;
    }

    public boolean inclusiveRight()
    {
        return false;
    }

    public List<? extends AbstractBounds<T>> unwrap()
    {
        // IncludingExcludingBounds objects never wrap
        return Collections.<AbstractBounds<T>>singletonList(this);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof IncludingExcludingBounds))
            return false;
        IncludingExcludingBounds<?> rhs = (IncludingExcludingBounds<?>)o;
        return left.equals(rhs.left) && right.equals(rhs.right);
    }

    /**
     * Compute a bounds of keys corresponding to a given bounds of token.
     */
    private static IncludingExcludingBounds<RowPosition> makeRowBounds(Token left, Token right, IPartitioner partitioner)
    {
        return new IncludingExcludingBounds<RowPosition>(left.maxKeyBound(partitioner), right.minKeyBound(partitioner), partitioner);
    }

    @SuppressWarnings("unchecked")
    public AbstractBounds<RowPosition> toRowBounds()
    {
        return (left instanceof Token) ? makeRowBounds((Token)left, (Token)right, partitioner) : (IncludingExcludingBounds<RowPosition>)this;
    }

    @SuppressWarnings("unchecked")
    public AbstractBounds<Token> toTokenBounds()
    {
        return (left instanceof RowPosition) ? new IncludingExcludingBounds<Token>(((RowPosition)left).getToken(), ((RowPosition)right).getToken(), partitioner) : (IncludingExcludingBounds<Token>)this;
    }

    public AbstractBounds<T> withNewRight(T newRight)
    {
        return new IncludingExcludingBounds<T>(left, newRight);
    }
}
