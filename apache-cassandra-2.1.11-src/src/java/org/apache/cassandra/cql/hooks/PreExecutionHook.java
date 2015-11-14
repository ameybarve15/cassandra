

/**
 * Run before the CQL Statement is executed in
 * {@link org.apache.cassandra.cql.QueryProcessor}. The CQLStatement
 * returned from the processStatement method is what is actually executed
 * by the QueryProcessor.
 */
public interface PreExecutionHook
{
    /**
     * Perform pre-processing on a CQL statement prior to it being
     * executed by the QueryProcessor. If required, implementations
     * may modify the statement as the returned instance is what
     * is actually executed.
     *
     * @param statement the statement to perform pre-processing on
     * @param context execution context containing additional info
     *                about the operation and statement
     * @return the actual statement that will be executed, possibly
     *         a modification of the initial statement
     * @throws RequestExecutionException, RequestValidationException
     */
    CQLStatement processStatement(CQLStatement statement, ExecutionContext context) throws RequestExecutionException, RequestValidationException;
}
