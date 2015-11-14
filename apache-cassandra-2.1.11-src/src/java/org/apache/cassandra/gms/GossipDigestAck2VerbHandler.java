

public class GossipDigestAck2VerbHandler implements IVerbHandler<GossipDigestAck2>
{
    public void doVerb(MessageIn<GossipDigestAck2> message, int id)
    {
        Map<InetAddress, EndpointState> remoteEpStateMap = message.payload.getEndpointStateMap();
        /* Notify the Failure Detector */
        Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
        Gossiper.instance.applyStateLocally(remoteEpStateMap);
    }
}
