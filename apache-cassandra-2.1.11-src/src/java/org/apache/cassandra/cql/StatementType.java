

public enum StatementType
{
    SELECT, INSERT, UPDATE, BATCH, USE, TRUNCATE, DELETE, CREATE_KEYSPACE, CREATE_COLUMNFAMILY, CREATE_INDEX, DROP_INDEX,
        DROP_KEYSPACE, DROP_COLUMNFAMILY, ALTER_TABLE;

    /** Statement types that don't require a keyspace to be set */
    private static final EnumSet<StatementType> TOP_LEVEL = EnumSet.of(USE, CREATE_KEYSPACE, DROP_KEYSPACE);

    /** Statement types that require a keyspace to be set */
    public static final EnumSet<StatementType> REQUIRES_KEYSPACE = EnumSet.complementOf(TOP_LEVEL);
}
