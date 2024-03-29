

public abstract class AbstractBounds<T extends RingPosition<T>> implements Serializable
{
    private static final long serialVersionUID = 1L;
    public static final AbstractBoundsSerializer serializer = new AbstractBoundsSerializer();

    private enum Type
    {
        RANGE,
        BOUNDS
    }

    public final T left;
    public final T right;

    protected transient final IPartitioner partitioner;

    public AbstractBounds(T left, T right, IPartitioner partitioner)
    {
        this.left = left;
        this.right = right;
        this.partitioner = partitioner;
    }

    /**
     * Given token T and AbstractBounds ?L,R?, returns Pair(?L,T], (T,R?),
     * where ? means that the same type of AbstractBounds is returned as the original.
     *
     * Put another way, returns a Pair of everything this AbstractBounds contains
     * up to and including the split position, and everything it contains after
     * (not including the split position).
     *
     * The original AbstractBounds must either contain the position T, or T
     * should be equals to the left bound L.
     *
     * If the split would only yield the same AbstractBound, null is returned
     * instead.
     */
    public abstract Pair<AbstractBounds<T>, AbstractBounds<T>> split(T position);
    public abstract boolean inclusiveLeft();
    public abstract boolean inclusiveRight();

    @Override
    public int hashCode()
    {
        return 31 * left.hashCode() + right.hashCode();
    }

    /** return true if @param range intersects any of the given @param ranges */
    public boolean intersects(Iterable<Range<T>> ranges)
    {
        for (Range<T> range2 : ranges)
        {
            if (range2.intersects(this))
                return true;
        }
        return false;
    }

    public abstract boolean contains(T start);

    public abstract List<? extends AbstractBounds<T>> unwrap();

    public String getString(AbstractType<?> keyValidator)
    {
        return getOpeningString() + format(left, keyValidator) + ", " + format(right, keyValidator) + getClosingString();
    }

    private String format(T value, AbstractType<?> keyValidator)
    {
        if (value instanceof DecoratedKey)
        {
            return keyValidator.getString(((DecoratedKey)value).getKey());
        }
        else
        {
            return value.toString();
        }
    }

    protected abstract String getOpeningString();
    protected abstract String getClosingString();

    /**
     * Transform this abstract bounds to equivalent covering bounds of row positions.
     * If this abstract bounds was already an abstractBounds of row positions, this is a noop.
     */
    public abstract AbstractBounds<RowPosition> toRowBounds();

    /**
     * Transform this abstract bounds to a token abstract bounds.
     * If this abstract bounds was already an abstractBounds of token, this is a noop, otherwise this use the row position tokens.
     */
    public abstract AbstractBounds<Token> toTokenBounds();

    public abstract AbstractBounds<T> withNewRight(T newRight);

    public static class AbstractBoundsSerializer implements IVersionedSerializer<AbstractBounds<?>>
    {
        public void serialize(AbstractBounds<?> range, DataOutputPlus out, int version) throws IOException
        {
            /*
             * The first int tells us if it's a range or bounds (depending on the value) _and_ if it's tokens or keys (depending on the
             * sign). We use negative kind for keys so as to preserve the serialization of token from older version.
             */
            out.writeInt(kindInt(range));
            if (range.left instanceof Token)
            {
                Token.serializer.serialize((Token) range.left, out);
                Token.serializer.serialize((Token) range.right, out);
            }
            else
            {
                RowPosition.serializer.serialize((RowPosition) range.left, out);
                RowPosition.serializer.serialize((RowPosition) range.right, out);
            }
        }

        private int kindInt(AbstractBounds<?> ab)
        {
            int kind = ab instanceof Range ? Type.RANGE.ordinal() : Type.BOUNDS.ordinal();
            if (!(ab.left instanceof Token))
                kind = -(kind + 1);
            return kind;
        }

        public AbstractBounds<?> deserialize(DataInput in, int version) throws IOException
        {
            int kind = in.readInt();
            boolean isToken = kind >= 0;
            if (!isToken)
                kind = -(kind+1);

            RingPosition<?> left, right;
            if (isToken)
            {
                left = Token.serializer.deserialize(in);
                right = Token.serializer.deserialize(in);
            }
            else
            {
                left = RowPosition.serializer.deserialize(in);
                right = RowPosition.serializer.deserialize(in);
            }

            if (kind == Type.RANGE.ordinal())
                return new Range(left, right);
            return new Bounds(left, right);
        }

        public long serializedSize(AbstractBounds<?> ab, int version)
        {
            int size = TypeSizes.NATIVE.sizeof(kindInt(ab));
            if (ab.left instanceof Token)
            {
                size += Token.serializer.serializedSize((Token) ab.left, TypeSizes.NATIVE);
                size += Token.serializer.serializedSize((Token) ab.right, TypeSizes.NATIVE);
            }
            else
            {
                size += RowPosition.serializer.serializedSize((RowPosition) ab.left, TypeSizes.NATIVE);
                size += RowPosition.serializer.serializedSize((RowPosition) ab.right, TypeSizes.NATIVE);
            }
            return size;
        }
    }

    public static <T extends RingPosition<T>> AbstractBounds<T> bounds(Boundary<T> min, Boundary<T> max)
    {
        return bounds(min.boundary, min.inclusive, max.boundary, max.inclusive);
    }
    public static <T extends RingPosition<T>> AbstractBounds<T> bounds(T min, boolean inclusiveMin, T max, boolean inclusiveMax)
    {
        if (inclusiveMin && inclusiveMax)
            return new Bounds<T>(min, max);
        else if (inclusiveMax)
            return new Range<T>(min, max);
        else if (inclusiveMin)
            return new IncludingExcludingBounds<T>(min, max);
        else
            return new ExcludingBounds<T>(min, max);
    }

    // represents one side of a bounds (which side is not encoded)
    public static class Boundary<T extends RingPosition<T>>
    {
        public final T boundary;
        public final boolean inclusive;
        public Boundary(T boundary, boolean inclusive)
        {
            this.boundary = boundary;
            this.inclusive = inclusive;
        }
    }

    public Boundary<T> leftBoundary()
    {
        return new Boundary<>(left, inclusiveLeft());
    }

    public Boundary<T> rightBoundary()
    {
        return new Boundary<>(right, inclusiveRight());
    }

    public static <T extends RingPosition<T>> boolean isEmpty(Boundary<T> left, Boundary<T> right)
    {
        int c = left.boundary.compareTo(right.boundary);
        return c > 0 || (c == 0 && !(left.inclusive && right.inclusive));
    }

    public static <T extends RingPosition<T>> Boundary<T> minRight(Boundary<T> right1, T right2, boolean isInclusiveRight2)
    {
        return minRight(right1, new Boundary<T>(right2, isInclusiveRight2));
    }

    public static <T extends RingPosition<T>> Boundary<T> minRight(Boundary<T> right1, Boundary<T> right2)
    {
        int c = right1.boundary.compareTo(right2.boundary);
        if (c != 0)
            return c < 0 ? right1 : right2;
        // return the exclusive version, if either
        return right2.inclusive ? right1 : right2;
    }

    public static <T extends RingPosition<T>> Boundary<T> maxLeft(Boundary<T> left1, T left2, boolean isInclusiveLeft2)
    {
        return maxLeft(left1, new Boundary<T>(left2, isInclusiveLeft2));
    }

    public static <T extends RingPosition<T>> Boundary<T> maxLeft(Boundary<T> left1, Boundary<T> left2)
    {
        int c = left1.boundary.compareTo(left2.boundary);
        if (c != 0)
            return c > 0 ? left1 : left2;
        // return the exclusive version, if either
        return left2.inclusive ? left1 : left2;
    }
}
