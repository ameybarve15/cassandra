
/**
 * Body part of SYNC_REQUEST repair message.
 * Request {@code src} node to sync data with {@code dst} node for range {@code ranges}.
 *
 * @since 2.0
 */
public class SyncRequest extends RepairMessage
{
    public static MessageSerializer serializer = new SyncRequestSerializer();

    public final InetAddress initiator;
    public final InetAddress src;
    public final InetAddress dst;
    public final Collection<Range<Token>> ranges;

    public SyncRequest(RepairJobDesc desc, InetAddress initiator, InetAddress src, InetAddress dst, Collection<Range<Token>> ranges)
    {
        super(Type.SYNC_REQUEST, desc);
        this.initiator = initiator;
        this.src = src;
        this.dst = dst;
        this.ranges = ranges;
    }
}
