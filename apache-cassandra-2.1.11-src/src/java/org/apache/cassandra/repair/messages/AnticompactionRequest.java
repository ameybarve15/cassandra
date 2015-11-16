

public class AnticompactionRequest extends RepairMessage
{
    public static MessageSerializer serializer = new AnticompactionRequestSerializer();
    public final UUID parentRepairSession;

    public AnticompactionRequest(UUID parentRepairSession)
    {
        super(Type.ANTICOMPACTION_REQUEST, null);
        this.parentRepairSession = parentRepairSession;
    }
}