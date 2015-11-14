

public class GossipDigestSynVerbHandler implements IVerbHandler<GossipDigestSyn>
{
    public void doVerb(MessageIn<GossipDigestSyn> message, int id)
    {
        InetAddress from = message.from;

        GossipDigestSyn gDigestMessage = message.payload;
        /* If the message is from a different cluster throw it away. */
        check@  gDigestMessage.clusterId == DatabaseDescriptor.getClusterName();

        check@  gDigestMessage.partioner == DatabaseDescriptor.getPartitionerName();

        List<GossipDigest> gDigestList = gDigestMessage.getGossipDigests();

        doSort(gDigestList);

        List<GossipDigest> deltaGossipDigestList = new ArrayList<GossipDigest>();
        Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<InetAddress, EndpointState>();
        Gossiper.instance.examineGossiper(gDigestList, deltaGossipDigestList, deltaEpStateMap);
        
        MessageOut<GossipDigestAck> gDigestAckMessage 
            = new MessageOut<GossipDigestAck>(
                    MessagingService.Verb.GOSSIP_DIGEST_ACK,
                    new GossipDigestAck(deltaGossipDigestList, deltaEpStateMap),
                    GossipDigestAck.serializer);

        MessagingService.instance().sendOneWay(gDigestAckMessage, from);
    }

    /*
     * First construct a map whose key is the endpoint in the GossipDigest and the value is the
     * GossipDigest itself. Then build a list of version differences i.e difference between the
     * version in the GossipDigest and the version in the local state for a given InetAddress.
     * Sort this list. Now loop through the sorted list and retrieve the GossipDigest corresponding
     * to the endpoint from the map that was initially constructed.
    */
    private void doSort(List<GossipDigest> gDigestList)
    {
        /* Construct a map of endpoint to GossipDigest. */
        Map<InetAddress, GossipDigest> epToDigestMap = new HashMap<InetAddress, GossipDigest>();
        for (GossipDigest gDigest : gDigestList)
        {
            epToDigestMap.put(gDigest.getEndpoint(), gDigest);
        }

        /*
         * These digests have their maxVersion set to the difference of the version
         * of the local EndpointState and the version found in the GossipDigest.
        */
        List<GossipDigest> diffDigests = new ArrayList<GossipDigest>(gDigestList.size());
        for (GossipDigest gDigest : gDigestList)
        {
            InetAddress ep = gDigest.getEndpoint();
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
            int version = (epState != null) ? Gossiper.instance.getMaxEndpointStateVersion(epState) : 0;
            int diffVersion = Math.abs(version - gDigest.getMaxVersion());
            diffDigests.add(new GossipDigest(ep, gDigest.getGeneration(), diffVersion));
        }

        gDigestList.clear();
        Collections.sort(diffDigests);
        int size = diffDigests.size();
        /*
         * Report the digests in descending order. This takes care of the endpoints
         * that are far behind w.r.t this local endpoint
        */
        for (int i = size - 1; i >= 0; --i)
        {
            gDigestList.add(epToDigestMap.get(diffDigests.get(i).getEndpoint()));
        }
    }
}
