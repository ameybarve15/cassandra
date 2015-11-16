
/**
 * Run directly after a CQL Statement is prepared in
 * {@link org.apache.cassandra.cql.QueryProcessor}.
 */
public interface PostPreparationHook
{
    /**
     * Called in QueryProcessor, once a CQL statement has been prepared.
     *
     * @param statement the statement to perform additional processing on
     * @param context preparation context containing additional info
     *                about the operation and statement
     * @throws RequestValidationException
     */
    void processStatement(CQLStatement statement, PreparationContext context) throws RequestValidationException;
}
