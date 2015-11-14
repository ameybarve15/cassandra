

public class GossipDigestAckVerbHandler implements IVerbHandler<GossipDigestAck>
{
   public void doVerb(MessageIn<GossipDigestAck> message, int id)
    {
        InetAddress from = message.from;

        GossipDigestAck gDigestAckMessage = message.payload;
        List<GossipDigest> gDigestList = gDigestAckMessage.getGossipDigestList();
        Map<InetAddress, EndpointState> epStateMap = gDigestAckMessage.getEndpointStateMap();
  
        if (epStateMap.size() > 0)
        {
            /* Notify the Failure Detector */
            Gossiper.instance.notifyFailureDetector(epStateMap);
            Gossiper.instance.applyStateLocally(epStateMap);
        }

        if (Gossiper.instance.isInShadowRound())
        {
            Gossiper.instance.finishShadowRound();
            return; // don't bother doing anything else, we have what we came for
        }

        /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
        Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<InetAddress, EndpointState>();
        for (GossipDigest gDigest : gDigestList)
        {
            InetAddress addr = gDigest.getEndpoint();
            EndpointState localEpStatePtr = Gossiper.instance.getStateForVersionBiggerThan(addr, gDigest.getMaxVersion());
            if (localEpStatePtr != null)
                deltaEpStateMap.put(addr, localEpStatePtr);
        }

        MessageOut<GossipDigestAck2> gDigestAck2Message = new MessageOut<GossipDigestAck2>(MessagingService.Verb.GOSSIP_DIGEST_ACK2,
                                                                                           new GossipDigestAck2(deltaEpStateMap),
                                                                                           GossipDigestAck2.serializer);
        MessagingService.instance().sendOneWay(gDigestAck2Message, from);
    }
}
