

/**
 * Fired right before removing an SSTable.
 */
public class SSTableDeletingNotification implements INotification
{
    public final SSTableReader deleting;

    public SSTableDeletingNotification(SSTableReader deleting)
    {
        this.deleting = deleting;
    }
}
