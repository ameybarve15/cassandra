

/**
 * Base class of all repair related request/response messages.
 *
 * @since 2.0
 */
public abstract class RepairMessage
{
    public static final IVersionedSerializer<RepairMessage> serializer = new RepairMessageSerializer();

    public static interface MessageSerializer<T extends RepairMessage> extends IVersionedSerializer<T> {}

    public static enum Type
    {
        VALIDATION_REQUEST(0, ValidationRequest.serializer),
        VALIDATION_COMPLETE(1, ValidationComplete.serializer),
        SYNC_REQUEST(2, SyncRequest.serializer),
        SYNC_COMPLETE(3, SyncComplete.serializer),
        ANTICOMPACTION_REQUEST(4, AnticompactionRequest.serializer),
        PREPARE_MESSAGE(5, PrepareMessage.serializer),
        SNAPSHOT(6, SnapshotMessage.serializer),
        CLEANUP(7, CleanupMessage.serializer);

        private final byte type;
        private final MessageSerializer<RepairMessage> serializer;

        private Type(int type, MessageSerializer<RepairMessage> serializer)
        {
            this.type = (byte) type;
            this.serializer = serializer;
        }

        public static Type fromByte(byte b)
        {
            for (Type t : values())
            {
               if (t.type == b)
                   return t;
            }
            throw new IllegalArgumentException("Unknown RepairMessage.Type: " + b);
        }
    }

    public final Type messageType;
    public final RepairJobDesc desc;

    protected RepairMessage(Type messageType, RepairJobDesc desc)
    {
        this.messageType = messageType;
        this.desc = desc;
    }

    public MessageOut<RepairMessage> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.REPAIR_MESSAGE, this, RepairMessage.serializer);
    }
}
