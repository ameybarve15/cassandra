


public class TokenSerializer
{

    public static void serialize(IPartitioner partitioner, Collection<Token> tokens, DataOutput out) throws IOException
    {
        for (Token token : tokens)
        {
            byte[] bintoken = partitioner.getTokenFactory().toByteArray(token).array();
            out.writeInt(bintoken.length);
            out.write(bintoken);
        }
        out.writeInt(0);
    }

    public static Collection<Token> deserialize(IPartitioner partitioner, DataInput in) throws IOException
    {
        Collection<Token> tokens = new ArrayList<Token>();
        while (true)
        {
            int size = in.readInt();
            if (size < 1)
                break;
            logger.trace("Reading token of {} bytes", size);
            byte[] bintoken = new byte[size];
            in.readFully(bintoken);
            tokens.add(partitioner.getTokenFactory().fromByteArray(ByteBuffer.wrap(bintoken)));
        }
        return tokens;
    }
}