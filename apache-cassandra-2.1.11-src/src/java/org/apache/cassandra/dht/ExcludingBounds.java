

/**
 * AbstractBounds containing neither of its endpoints: (left, right).  Used by CQL key > X AND key < Y range scans.
 */
public class ExcludingBounds<T extends RingPosition<T>> extends AbstractBounds<T>
{
    public ExcludingBounds(T left, T right)
    {
        this(left, right, StorageService.getPartitioner());
    }

    ExcludingBounds(T left, T right, IPartitioner partitioner)
    {
        super(left, right, partitioner);
        // unlike a Range, an ExcludingBounds may not wrap, nor be empty
        assert left.compareTo(right) < 0 || right.isMinimum(partitioner) : "(" + left + "," + right + ")";
    }

    public boolean contains(T position)
    {
        return Range.contains(left, right, position) && !right.equals(position);
    }

    public Pair<AbstractBounds<T>, AbstractBounds<T>> split(T position)
    {
        assert contains(position) || left.equals(position);
        if (left.equals(position))
            return null;
        AbstractBounds<T> lb = new Range<T>(left, position, partitioner);
        AbstractBounds<T> rb = new ExcludingBounds<T>(position, right, partitioner);
        return Pair.create(lb, rb);
    }

    public boolean inclusiveLeft()
    {
        return false;
    }

    public boolean inclusiveRight()
    {
        return false;
    }

    public List<? extends AbstractBounds<T>> unwrap()
    {
        // ExcludingBounds objects never wrap
        return Collections.<AbstractBounds<T>>singletonList(this);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ExcludingBounds))
            return false;
        ExcludingBounds<T> rhs = (ExcludingBounds<T>)o;
        return left.equals(rhs.left) && right.equals(rhs.right);
    }

    @Override
    public String toString()
    {
        return "(" + left + "," + right + ")";
    }

    protected String getOpeningString()
    {
        return "(";
    }

    protected String getClosingString()
    {
        return ")";
    }

    /**
     * Compute a bounds of keys corresponding to a given bounds of token.
     */
    private static ExcludingBounds<RowPosition> makeRowBounds(Token left, Token right, IPartitioner partitioner)
    {
        return new ExcludingBounds<RowPosition>(left.maxKeyBound(partitioner), right.minKeyBound(partitioner), partitioner);
    }

    @SuppressWarnings("unchecked")
    public AbstractBounds<RowPosition> toRowBounds()
    {
        return (left instanceof Token) ? makeRowBounds((Token)left, (Token)right, partitioner) : (ExcludingBounds<RowPosition>)this;
    }

    @SuppressWarnings("unchecked")
    public AbstractBounds<Token> toTokenBounds()
    {
        return (left instanceof RowPosition) ? new ExcludingBounds<Token>(((RowPosition)left).getToken(), ((RowPosition)right).getToken(), partitioner) : (ExcludingBounds<Token>)this;
    }

    public AbstractBounds<T> withNewRight(T newRight)
    {
        return new ExcludingBounds<T>(left, newRight);
    }
}
