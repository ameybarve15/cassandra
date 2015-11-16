
public class BigIntegerToken extends ComparableObjectToken<BigInteger>
{
    static final long serialVersionUID = -5833589141319293006L;

    public BigIntegerToken(BigInteger token)
    {
        super(token);
    }

    // convenience method for testing
    public BigIntegerToken(String token) {
        this(new BigInteger(token));
    }
}
