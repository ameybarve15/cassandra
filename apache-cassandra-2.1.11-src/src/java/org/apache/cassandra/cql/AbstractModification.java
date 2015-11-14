

public abstract class AbstractModification
{
    public static final ConsistencyLevel defaultConsistency = ConsistencyLevel.ONE;

    protected final String keyspace;
    protected final String columnFamily;
    protected final ConsistencyLevel cLevel;
    protected final Long timestamp;
    protected final int timeToLive;
    protected final String keyName;

    public AbstractModification(String keyspace, String columnFamily, String keyAlias, Attributes attrs)
    {
        this(keyspace, columnFamily, keyAlias, attrs.getConsistencyLevel(), attrs.getTimestamp(), attrs.getTimeToLive());
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return (cLevel != null) ? cLevel : defaultConsistency;
    }

    /**
     * True if an explicit consistency level was parsed from the statement.
     *
     * @return true if a consistency was parsed, false otherwise.
     */
    public boolean isSetConsistencyLevel()
    {
        return cLevel != null;
    }

    public long getTimestamp(ThriftClientState clientState)
    {
        return timestamp == null ? clientState.getQueryState().getTimestamp() : timestamp;
    }

    public boolean isSetTimestamp()
    {
        return timestamp != null;
    }


    /**
     * Convert statement into a list of mutations to apply on the server
     *
     * @param keyspace The working keyspace
     * @param clientState current client status
     *
     * @return list of the mutations
     *
     * @throws InvalidRequestException on the wrong request
     */
    public abstract List<IMutation> prepareRowMutations(String keyspace, ThriftClientState clientState, List<ByteBuffer> variables)
    throws InvalidRequestException, UnauthorizedException;

    /**
     * Convert statement into a list of mutations to apply on the server
     *
     * @param keyspace The working keyspace
     * @param clientState current client status
     * @param timestamp global timestamp to use for all mutations
     *
     * @return list of the mutations
     *
     * @throws InvalidRequestException on the wrong request
     */
    public abstract List<IMutation> prepareRowMutations(String keyspace, ThriftClientState clientState, Long timestamp, List<ByteBuffer> variables)
    throws InvalidRequestException, UnauthorizedException;
}
