
/**
 * This ack gets sent out as a result of the receipt of a GossipDigestSynMessage by an
 * endpoint. This is the 2 stage of the 3 way messaging in the Gossip protocol.
 */
public class GossipDigestAck
{
    final List<GossipDigest> gDigestList;
    final Map<InetAddress, EndpointState> epStateMap;
}

