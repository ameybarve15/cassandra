
/**
 * This class blocks for a quorum of responses _in the local datacenter only_ (CL.LOCAL_QUORUM).
 */
public class DatacenterWriteResponseHandler extends WriteResponseHandler
{
    public DatacenterWriteResponseHandler(Collection<InetAddress> naturalEndpoints,
                                          Collection<InetAddress> pendingEndpoints,
                                          ConsistencyLevel consistencyLevel,
                                          Keyspace keyspace,
                                          Runnable callback,
                                          WriteType writeType)
    {
        super(naturalEndpoints, pendingEndpoints, consistencyLevel, keyspace, callback, writeType);
        assert consistencyLevel.isDatacenterLocal();
    }

    @Override
    public void response(MessageIn message)
    {
        if (message == null || consistencyLevel.isLocal(message.from))
            super.response(message);
    }

    @Override
    protected int totalBlockFor()
    {
        // during bootstrap, include pending endpoints (only local here) in the count
        // or we may fail the consistency level guarantees (see #833, #8058)
        return consistencyLevel.blockFor(keyspace) + consistencyLevel.countLocalEndpoints(pendingEndpoints);
    }
}
