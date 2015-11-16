

public class SSTableAddedNotification implements INotification
{
    public final SSTableReader added;
    public SSTableAddedNotification(SSTableReader added)
    {
        this.added = added;
    }
}
