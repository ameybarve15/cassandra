

/**
 * Message to cleanup repair resources on replica nodes.
 *
 * @since 2.1.6
 */
public class CleanupMessage extends RepairMessage
{
    public static MessageSerializer serializer = new CleanupMessageSerializer();
    public final UUID parentRepairSession;

    public CleanupMessage(UUID parentRepairSession)
    {
        super(Type.CLEANUP, null);
        this.parentRepairSession = parentRepairSession;
    }
}
