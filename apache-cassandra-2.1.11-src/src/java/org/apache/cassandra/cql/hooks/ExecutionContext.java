

/**
 * Contextual information about the execution of a CQLStatement.
 * Used by {@link org.apache.cassandra.cql.hooks.PreExecutionHook}
 *
 * The CQL string representing the statement being executed is optional
 * and is not present for prepared statements. Contexts created for the
 * execution of regular (i.e. non-prepared) statements will always
 * contain a CQL string.
 */
public class ExecutionContext
{
    public final ThriftClientState clientState;
    public final Optional<String> queryString;
    public final List<ByteBuffer> variables;

    public ExecutionContext(ThriftClientState clientState, String queryString, List<ByteBuffer> variables)
    {
        this.clientState = clientState;
        this.queryString = Optional.fromNullable(queryString);
        this.variables = variables;
    }
}
