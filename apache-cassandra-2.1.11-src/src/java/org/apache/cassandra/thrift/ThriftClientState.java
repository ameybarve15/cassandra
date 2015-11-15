

/**
 * ClientState used by thrift that also provide a QueryState.
 *
 * Thrift is intrinsically synchronous so there could be only one query per
 * client at a given time. So ClientState and QueryState can be merge into the
 * same object.
 */
public class ThriftClientState extends ClientState
{
    private static final int MAX_CACHE_PREPARED = 10000;    // Enough to keep buggy clients from OOM'ing us

    private final QueryState queryState;

    // An LRU map of prepared statements
    private final Map<Integer, CQLStatement> prepared = new LinkedHashMap<Integer, CQLStatement>(16, 0.75f, true)
    {
        protected boolean removeEldestEntry(Map.Entry<Integer, CQLStatement> eldest)
        {
            return size() > MAX_CACHE_PREPARED;
        }
    };

    public ThriftClientState(SocketAddress remoteAddress)
    {
        super(remoteAddress);
        this.queryState = new QueryState(this);
    }

    public Map<Integer, CQLStatement> getPrepared()
    {
        return prepared;
    }

    public String getSchedulingValue()
    {
        switch(DatabaseDescriptor.getRequestSchedulerId())
        {
            case keyspace: return getRawKeyspace();
        }
        return "default";
    }
}
