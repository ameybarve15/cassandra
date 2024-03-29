

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * A class for caching the ring map at the client. For usage example, see
 * test/unit/org.apache.cassandra.client.TestRingCache.java.
 * TODO: doing a naive linear search of the token map
 */
public class RingCache
{
    private final IPartitioner partitioner;
    private final Configuration conf;

    private Multimap<Range<Token>, InetAddress> rangeMap;

    public RingCache(Configuration conf)
    {
        this.conf = conf;
        this.partitioner = ConfigHelper.getOutputPartitioner(conf);
        refreshEndpointMap();
    }

    public void refreshEndpointMap()
    {
        try
        {
            Cassandra.Client client = ConfigHelper.getClientFromOutputAddressList(conf);

            String keyspace = ConfigHelper.getOutputKeyspace(conf);
            List<TokenRange> ring = ConfigHelper.getOutputLocalDCOnly(conf)
                                  ? client.describe_local_ring(keyspace)
                                  : client.describe_ring(keyspace);
            rangeMap = ArrayListMultimap.create();

            for (TokenRange range : ring)
            {
                Token left = partitioner.getTokenFactory().fromString(range.start_token);
                Token right = partitioner.getTokenFactory().fromString(range.end_token);
                Range<Token> r = new Range<Token>(left, right, partitioner);
                for (String host : range.endpoints)
                {
                    try
                    {
                        rangeMap.put(r, InetAddress.getByName(host));
                    } 
                }
            }
        }
    }

    /** ListMultimap promises to return a List for get(K) */
    public List<InetAddress> getEndpoint(Range<Token> range)
    {
        return (List<InetAddress>) rangeMap.get(range);
    }

    public List<InetAddress> getEndpoint(ByteBuffer key)
    {
        return getEndpoint(getRange(key));
    }

    public Range<Token> getRange(ByteBuffer key)
    {
        // TODO: naive linear search of the token map
        Token t = partitioner.getToken(key);
        for (Range<Token> range : rangeMap.keySet())
            if (range.contains(t))
                return range;

        throw new RuntimeException("Invalid token information returned by describe_ring: " + rangeMap);
    }
}
