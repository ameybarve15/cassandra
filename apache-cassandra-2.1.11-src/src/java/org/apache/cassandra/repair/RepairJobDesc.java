

/**
 * RepairJobDesc is used from various repair processes to distinguish one RepairJob to another.
 *
 * @since 2.0
 */
public class RepairJobDesc
{
    public static final IVersionedSerializer<RepairJobDesc> serializer = new RepairJobDescSerializer();

    public final UUID parentSessionId;
    /** RepairSession id */
    public final UUID sessionId;
    public final String keyspace;
    public final String columnFamily;
    /** repairing range  */
    public final Range<Token> range;

    public RepairJobDesc(UUID parentSessionId, UUID sessionId, String keyspace, String columnFamily, Range<Token> range)
    {
        this.parentSessionId = parentSessionId;
        this.sessionId = sessionId;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.range = range;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepairJobDesc that = (RepairJobDesc) o;

        if (!columnFamily.equals(that.columnFamily)) return false;
        if (!keyspace.equals(that.keyspace)) return false;
        if (range != null ? !range.equals(that.range) : that.range != null) return false;
        if (!sessionId.equals(that.sessionId)) return false;
        if (parentSessionId != null ? !parentSessionId.equals(that.parentSessionId) : that.parentSessionId != null) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(sessionId, keyspace, columnFamily, range);
    }
}
