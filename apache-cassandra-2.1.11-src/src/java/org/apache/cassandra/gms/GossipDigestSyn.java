package org.apache.cassandra.gms;

/**
 * This is the first message that gets sent out as a start of the Gossip protocol in a
 * round.
 */
public class GossipDigestSyn
{
    final String clusterId;
    final String partioner;
    final List<GossipDigest> gDigests;
}