

/**
 * Common interface to single partition queries (by slice and by name).
 *
 * For use by MultiPartitionPager.
 */
public interface SinglePartitionPager extends QueryPager
{
    public ByteBuffer key();
    public ColumnCounter columnCounter();
}
