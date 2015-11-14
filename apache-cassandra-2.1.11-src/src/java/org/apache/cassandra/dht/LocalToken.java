

public class LocalToken extends ComparableObjectToken<ByteBuffer>
{
    static final long serialVersionUID = 8437543776403014875L;

    private final AbstractType<?> comparator;

    public LocalToken(AbstractType<?> comparator, ByteBuffer token)
    {
        super(token);
        this.comparator = comparator;
    }

    @Override
    public String toString()
    {
        return comparator.getString(token);
    }

    public int compareTo(Token o)
    {
        return comparator.compare(token, ((LocalToken) o).token);
    }
}
