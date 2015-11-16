
public class EchoMessage
{
    public static IVersionedSerializer<EchoMessage> serializer = new EchoMessageSerializer();

    public static class EchoMessageSerializer implements IVersionedSerializer<EchoMessage>
    {
        public void serialize(EchoMessage t, DataOutputPlus out, int version)
        {
        }

        public EchoMessage deserialize(DataInput in, int version)
        {
            return new EchoMessage();
        }

        public long serializedSize(EchoMessage t, int version)
        {
            return 0;
        }
    }
}
