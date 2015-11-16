

/**
 * The result of the computation of an MD5 digest.
 *
 * A MD5 is really just a byte[] but arrays are a no go as map keys. We could
 * wrap it in a ByteBuffer but:
 *   1. MD5Digest is a more explicit name than ByteBuffer to represent a md5.
 *   2. Using our own class allows to use our FastByteComparison for equals.
 */
public class MD5Digest
{
    public final byte[] bytes;
    private final int hashCode;

    private MD5Digest(byte[] bytes)
    {
        this.bytes = bytes;
        hashCode = Arrays.hashCode(bytes);
    }

    public static MD5Digest wrap(byte[] digest)
    {
        return new MD5Digest(digest);
    }

    public static MD5Digest compute(byte[] toHash)
    {
        return new MD5Digest(FBUtilities.threadLocalMD5Digest().digest(toHash));
    }

    public static MD5Digest compute(String toHash)
    {
        return compute(toHash.getBytes());
    }

    @Override
    public final int hashCode()
    {
        return hashCode;
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof MD5Digest))
            return false;
        MD5Digest that = (MD5Digest)o;
        // handles nulls properly
        return FBUtilities.compareUnsigned(this.bytes, that.bytes, 0, 0, this.bytes.length, that.bytes.length) == 0;
    }

    @Override
    public String toString()
    {
        return Hex.bytesToHex(bytes);
    }
}
