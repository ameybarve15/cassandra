

abstract class ComparableObjectToken<C extends Comparable<C>> extends Token
{
    private static final long serialVersionUID = 1L;

    final C token;   // Package-private to allow access from subtypes, which should all reside in the dht package.

    protected ComparableObjectToken(C token)
    {
        this.token = token;
    }

    @Override
    public C getTokenValue()
    {
        return token;
    }

    @Override
    public String toString()
    {
        return token.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null || this.getClass() != obj.getClass())
            return false;

        return token.equals(((ComparableObjectToken<?>)obj).token);
    }

    @Override
    public int hashCode()
    {
        return token.hashCode();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(Token o)
    {
        if (o.getClass() != getClass())
            throw new IllegalArgumentException("Invalid type of Token.compareTo() argument.");

        return token.compareTo(((ComparableObjectToken<C>) o).token);
    }
}
