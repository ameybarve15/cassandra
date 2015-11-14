

public class AnticompactionTask extends AbstractFuture<InetAddress> implements Runnable
{
    /*
     * Version that anticompaction response is not supported up to.
     * If Cassandra version is more than this, we need to wait for anticompaction response.
     */
    private static final SemanticVersion VERSION_CHECKER = new SemanticVersion("2.1.5");

    private final UUID parentSession;
    private final InetAddress neighbor;
    private final boolean doAnticompaction;

    public AnticompactionTask(UUID parentSession, InetAddress neighbor, boolean doAnticompaction)
    {
        this.parentSession = parentSession;
        this.neighbor = neighbor;
        this.doAnticompaction = doAnticompaction;
    }

    public void run()
    {
        AnticompactionRequest acr = new AnticompactionRequest(parentSession);
        SemanticVersion peerVersion = SystemKeyspace.getReleaseVersion(neighbor);
        if (peerVersion != null && peerVersion.compareTo(VERSION_CHECKER) > 0)
        {
            if (doAnticompaction)
            {
                MessagingService.instance().sendRR(acr.createMessage(), neighbor, new AnticompactionCallback(this), TimeUnit.DAYS.toMillis(1), true);
            }
            else
            {
                // we need to clean up parent session
                MessagingService.instance().sendRR(new CleanupMessage(parentSession).createMessage(), neighbor, new AnticompactionCallback(this), TimeUnit.DAYS.toMillis(1), true);
            }
        }
        else
        {
            MessagingService.instance().sendOneWay(acr.createMessage(), neighbor);
            // immediately return after sending request
            set(neighbor);
        }
    }

    /**
     * Callback for antitcompaction request. Run on INTERNAL_RESPONSE stage.
     */
    public static class AnticompactionCallback implements IAsyncCallbackWithFailure
    {
        final AnticompactionTask task;

        public AnticompactionCallback(AnticompactionTask task)
        {
            this.task = task;
        }

        public void response(MessageIn msg)
        {
            task.set(msg.from);
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }

        public void onFailure(InetAddress from)
        {
            task.setException(new RuntimeException("Anticompaction failed or timed out in " + from));
        }
    }
}
