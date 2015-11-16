

public class SnapshotMessage extends RepairMessage
{
    public final static MessageSerializer serializer = new SnapshotMessageSerializer();

    public SnapshotMessage(RepairJobDesc desc)
    {
        super(Type.SNAPSHOT, desc);
    }
}
