

/**
 *
 * @since 2.0
 */
public class SyncComplete extends RepairMessage
{
    public static final MessageSerializer serializer = new SyncCompleteSerializer();

    /** nodes that involved in this sync */
    public final NodePair nodes;
    /** true if sync success, false otherwise */
    public final boolean success;

    public SyncComplete(RepairJobDesc desc, NodePair nodes, boolean success)
    {
        super(Type.SYNC_COMPLETE, desc);
        this.nodes = nodes;
        this.success = success;
    }

    public SyncComplete(RepairJobDesc desc, InetAddress endpoint1, InetAddress endpoint2, boolean success)
    {
        super(Type.SYNC_COMPLETE, desc);
        this.nodes = new NodePair(endpoint1, endpoint2);
        this.success = success;
    }
}
