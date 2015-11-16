

/** A <code>CREATE INDEX</code> statement parsed from a CQL query. */
public class CreateIndexStatement
{
    private final String columnFamily;
    private final String indexName;
    private final Term columnName;
}
