

public class RowDigestResolver extends AbstractRowResolver
{
    public RowDigestResolver(String keyspaceName, ByteBuffer key)
    {
        super(key, keyspaceName);
    }

    /**
     * Special case of resolve() so that CL.ONE reads never throw DigestMismatchException in the foreground
     */
    public Row getData()
    {
        for (MessageIn<ReadResponse> message : replies)
        {
            ReadResponse result = message.payload;
            if (!result.isDigestQuery())
            {
                if (result.digest() == null)
                    result.setDigest(ColumnFamily.digest(result.row().cf));

                return result.row();
            }
        }
        return null;
    }

    /*
     * This method handles two different scenarios:
     *
     * a) we're handling the initial read, of data from the closest replica + digests
     *    from the rest.  In this case we check the digests against each other,
     *    throw an exception if there is a mismatch, otherwise return the data row.
     *
     * b) we're checking additional digests that arrived after the minimum to handle
     *    the requested ConsistencyLevel, i.e. asynchronous read repair check
     */
    public Row resolve() throws DigestMismatchException
    {
        long start = System.nanoTime();

        // validate digests against each other; throw immediately on mismatch.
        // also extract the data reply, if any.
        ColumnFamily data = null;
        ByteBuffer digest = null;

        for (MessageIn<ReadResponse> message : replies)
        {
            ReadResponse response = message.payload;

            ByteBuffer newDigest;
            if (response.isDigestQuery())
            {
                newDigest = response.digest();
            }
            else
            {
                // note that this allows for multiple data replies, post-CASSANDRA-5932
                data = response.row().cf;
                if (response.digest() == null)
                    message.payload.setDigest(ColumnFamily.digest(data));

                newDigest = response.digest();
            }

            if (digest == null)
                digest = newDigest;
            else if (!digest.equals(newDigest))
                throw new DigestMismatchException(key, digest, newDigest);
        }

        return new Row(key, data);
    }

    public boolean isDataPresent()
    {
        for (MessageIn<ReadResponse> message : replies)
        {
            if (!message.payload.isDigestQuery())
                return true;
        }
        return false;
    }
}
