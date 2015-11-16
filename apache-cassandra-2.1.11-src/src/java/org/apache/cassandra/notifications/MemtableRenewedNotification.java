

public class MemtableRenewedNotification implements INotification
{
    public final Memtable renewed;

    public MemtableRenewedNotification(Memtable renewed)
    {
        this.renewed = renewed;
    }
}
