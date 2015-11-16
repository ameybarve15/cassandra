
public class BytesToken extends Token
{
    static final long serialVersionUID = -2630749093733680626L;

    final byte[] token;

    public BytesToken(ByteBuffer token)
    {
        this(ByteBufferUtil.getArray(token));
    }

    public BytesToken(byte[] token)
    {
        this.token = token;
    }

    @Override
    public String toString()
    {
        return Hex.bytesToHex(token);
    }

    public int compareTo(Token other)
    {
        BytesToken o = (BytesToken) other;
        return FBUtilities.compareUnsigned(token, o.token, 0, 0, token.length, o.token.length);
    }


    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (!(obj instanceof BytesToken))
            return false;
        BytesToken other = (BytesToken) obj;

        return Arrays.equals(token, other.token);
    }

    @Override
    public byte[] getTokenValue()
    {
        return token;
    }
}
