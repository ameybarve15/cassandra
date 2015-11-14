


public class PrepareMessage extends RepairMessage
{
    public final static MessageSerializer serializer = new PrepareMessageSerializer();
    public final List<UUID> cfIds;
    public final Collection<Range<Token>> ranges;

    public final UUID parentRepairSession;

    public PrepareMessage(UUID parentRepairSession, List<UUID> cfIds, Collection<Range<Token>> ranges)
    {
        super(Type.PREPARE_MESSAGE, null);
        this.parentRepairSession = parentRepairSession;
        this.cfIds = cfIds;
        this.ranges = ranges;
    }
}
