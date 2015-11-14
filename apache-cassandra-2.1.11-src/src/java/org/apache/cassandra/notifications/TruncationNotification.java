

/**
 * Fired during truncate, after the memtable has been flushed but before any
 * snapshot is taken and SSTables are discarded
 */
public class TruncationNotification implements INotification
{
    public final long truncatedAt;
}
