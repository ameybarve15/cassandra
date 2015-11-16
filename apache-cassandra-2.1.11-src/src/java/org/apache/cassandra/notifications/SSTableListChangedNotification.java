
public class SSTableListChangedNotification implements INotification
{
    public final Collection<SSTableReader> removed;
    public final Collection<SSTableReader> added;
    public final OperationType compactionType;
}
