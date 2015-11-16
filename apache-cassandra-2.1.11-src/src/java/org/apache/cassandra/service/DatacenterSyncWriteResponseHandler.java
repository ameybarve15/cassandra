

/**
 * This class blocks for a quorum of responses _in all datacenters_ (CL.EACH_QUORUM).
 */
public class DatacenterSyncWriteResponseHandler extends AbstractWriteResponseHandler
{
    private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

    private final NetworkTopologyStrategy strategy;
    private final HashMap<String, AtomicInteger> responses = new HashMap<String, AtomicInteger>();
    private final AtomicInteger acks = new AtomicInteger(0);

    public DatacenterSyncWriteResponseHandler(Collection<InetAddress> naturalEndpoints,
                                              Collection<InetAddress> pendingEndpoints,
                                              ConsistencyLevel consistencyLevel,
                                              Keyspace keyspace,
                                              Runnable callback,
                                              WriteType writeType)
    {
        // Response is been managed by the map so make it 1 for the superclass.
        super(keyspace, naturalEndpoints, pendingEndpoints, consistencyLevel, callback, writeType);
        assert consistencyLevel == ConsistencyLevel.EACH_QUORUM;

        strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();

        for (String dc : strategy.getDatacenters())
        {
            int rf = strategy.getReplicationFactor(dc);
            responses.put(dc, new AtomicInteger((rf / 2) + 1));
        }

        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        for (InetAddress pending : pendingEndpoints)
        {
            responses.get(snitch.getDatacenter(pending)).incrementAndGet();
        }
    }

    public void response(MessageIn message)
    {
        String dataCenter = message == null
                            ? DatabaseDescriptor.getLocalDataCenter()
                            : snitch.getDatacenter(message.from);

        responses.get(dataCenter).getAndDecrement();
        acks.incrementAndGet();

        for (AtomicInteger i : responses.values())
        {
            if (i.get() > 0)
                return;
        }

        // all the quorum conditions are met
        signal();
    }

    protected int ackCount()
    {
        return acks.get();
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
