

public abstract class AbstractPaxosCallback<T> implements IAsyncCallback<T>
{
    protected final CountDownLatch latch;
    protected final int targets;
    private final ConsistencyLevel consistency;

    public AbstractPaxosCallback(int targets, ConsistencyLevel consistency)
    {
        this.targets = targets;
        this.consistency = consistency;
        latch = new CountDownLatch(targets);
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    public int getResponseCount()
    {
        return (int) (targets - latch.getCount());
    }

    public void await() throws WriteTimeoutException
    {
        if (!latch.await(DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS))
            throw new WriteTimeoutException(WriteType.CAS, consistency, getResponseCount(), targets);
    }
}
