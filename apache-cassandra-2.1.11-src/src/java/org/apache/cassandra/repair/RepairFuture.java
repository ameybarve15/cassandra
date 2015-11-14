
public class RepairFuture extends FutureTask<Void>
{
    public final RepairSession session;

    public RepairFuture(RepairSession session)
    {
        super(session, null);
        this.session = session;
    }
}
