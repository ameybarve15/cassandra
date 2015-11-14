
/**
 * Handles blocking writes for ONE, ANY, TWO, THREE, QUORUM, and ALL consistency levels.
 */
public class WriteResponseHandler extends AbstractWriteResponseHandler
{
    protected static final Logger logger = LoggerFactory.getLogger(WriteResponseHandler.class);

    protected volatile int responses;
    private static final AtomicIntegerFieldUpdater<WriteResponseHandler> responsesUpdater
            = AtomicIntegerFieldUpdater.newUpdater(WriteResponseHandler.class, "responses");

    public WriteResponseHandler(Collection<InetAddress> writeEndpoints,
                                Collection<InetAddress> pendingEndpoints,
                                ConsistencyLevel consistencyLevel,
                                Keyspace keyspace,
                                Runnable callback,
                                WriteType writeType)
    {
        super(keyspace, writeEndpoints, pendingEndpoints, consistencyLevel, callback, writeType);
        responses = totalBlockFor();
    }

    public WriteResponseHandler(InetAddress endpoint, WriteType writeType, Runnable callback)
    {
        this(Arrays.asList(endpoint), Collections.<InetAddress>emptyList(), ConsistencyLevel.ONE, null, callback, writeType);
    }

    public WriteResponseHandler(InetAddress endpoint, WriteType writeType)
    {
        this(endpoint, writeType, null);
    }

    public void response(MessageIn m)
    {
        if (responsesUpdater.decrementAndGet(this) == 0)
            signal();
    }

    protected int ackCount()
    {
        return totalBlockFor() - responses;
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
