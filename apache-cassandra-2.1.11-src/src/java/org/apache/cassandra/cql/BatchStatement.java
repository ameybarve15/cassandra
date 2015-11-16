

/**
 * A <code>BATCH</code> statement parsed from a CQL query.
 *
 */
public class BatchStatement
{
    // statements to execute
    protected final List<AbstractModification> statements;

    // global consistency level
    protected final ConsistencyLevel consistency;

    // global timestamp to apply for each mutation
    protected final Long timestamp;

    // global time to live
    protected final int timeToLive;

    public List<IMutation> getMutations(String keyspace, ThriftClientState clientState, List<ByteBuffer> variables)
    throws InvalidRequestException, UnauthorizedException
    {
        List<IMutation> batch = new LinkedList<IMutation>();

        for (AbstractModification statement : statements) {
            batch.addAll(statement.prepareRowMutations(keyspace, clientState, timestamp, variables));
        }

        return batch;
    }

    public boolean isSetTimestamp()
    {
        return timestamp != null;
    }
}
