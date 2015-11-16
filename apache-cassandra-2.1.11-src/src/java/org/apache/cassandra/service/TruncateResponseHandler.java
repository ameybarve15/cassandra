

public class TruncateResponseHandler implements IAsyncCallback
{
    protected static final Logger logger = LoggerFactory.getLogger(TruncateResponseHandler.class);
    protected final SimpleCondition condition = new SimpleCondition();
    private final int responseCount;
    protected final AtomicInteger responses = new AtomicInteger(0);
    private final long start;

    public TruncateResponseHandler(int responseCount)
    {
        // at most one node per range can bootstrap at a time, and these will be added to the write until
        // bootstrap finishes (at which point we no longer need to write to the old ones).
        assert 1 <= responseCount: "invalid response count " + responseCount;

        this.responseCount = responseCount;
        start = System.nanoTime();
    }

    public void get()
    {
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getTruncateRpcTimeout()) - (System.nanoTime() - start);
        boolean success;
        
        success = condition.await(timeout, TimeUnit.NANOSECONDS); // TODO truncate needs a much longer timeout

        if (!success)
        {
            throw new TimeoutException("Truncate timed out - received only " + responses.get() + " responses");
        }
    }

    public void response(MessageIn message)
    {
        responses.incrementAndGet();
        if (responses.get() >= responseCount)
            condition.signalAll();
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
