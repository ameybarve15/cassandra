

public class AsyncRepairCallback implements IAsyncCallback<ReadResponse>
{
    private final RowDataResolver repairResolver;
    private final int blockfor;
    protected final AtomicInteger received = new AtomicInteger(0);

    public AsyncRepairCallback(RowDataResolver repairResolver, int blockfor)
    {
        this.repairResolver = repairResolver;
        this.blockfor = blockfor;
    }

    public void response(MessageIn<ReadResponse> message)
    {
        repairResolver.preprocess(message);
        if (received.incrementAndGet() == blockfor)
        {
            StageManager.getStage(Stage.READ_REPAIR).execute(new WrappedRunnable()
            {
                protected void runMayThrow() throws DigestMismatchException, IOException
                {
                    repairResolver.resolve();
                }
            });
        }
    }

    public boolean isLatencyForSnitch()
    {
        return true;
    }
}
