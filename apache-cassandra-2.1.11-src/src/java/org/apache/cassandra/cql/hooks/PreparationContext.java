

/**
 * Contextual information about the preparation of a CQLStatement.
 * Used by {@link org.apache.cassandra.cql.hooks.PostPreparationHook}
 */
public class PreparationContext
{
    public final ThriftClientState clientState;
    public final String queryString;
    public final CQLStatement statement;
}
