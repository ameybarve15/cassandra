
/**
 * NodePair is used for repair message body to indicate the pair of nodes.
 *
 * @since 2.0
 */
public class NodePair
{
    public static IVersionedSerializer<NodePair> serializer = new NodePairSerializer();

    public final InetAddress endpoint1;
    public final InetAddress endpoint2;

    public NodePair(InetAddress endpoint1, InetAddress endpoint2)
    {
        this.endpoint1 = endpoint1;
        this.endpoint2 = endpoint2;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodePair nodePair = (NodePair) o;
        return endpoint1.equals(nodePair.endpoint1) && endpoint2.equals(nodePair.endpoint2);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(endpoint1, endpoint2);
    }

    public static class NodePairSerializer implements IVersionedSerializer<NodePair>
    {
        public void serialize(NodePair nodePair, DataOutputPlus out, int version) throws IOException
        {
            CompactEndpointSerializationHelper.serialize(nodePair.endpoint1, out);
            CompactEndpointSerializationHelper.serialize(nodePair.endpoint2, out);
        }

        public NodePair deserialize(DataInput in, int version) throws IOException
        {
            InetAddress ep1 = CompactEndpointSerializationHelper.deserialize(in);
            InetAddress ep2 = CompactEndpointSerializationHelper.deserialize(in);
            return new NodePair(ep1, ep2);
        }

        public long serializedSize(NodePair nodePair, int version)
        {
            return 2 * CompactEndpointSerializationHelper.serializedSize(nodePair.endpoint1);
        }
    }
}
