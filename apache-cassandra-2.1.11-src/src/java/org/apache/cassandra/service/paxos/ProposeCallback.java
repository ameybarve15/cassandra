

/**
 * ProposeCallback has two modes of operation, controlled by the failFast parameter.
 *
 * In failFast mode, we will return a failure as soon as a majority of nodes reject
 * the proposal. This is used when replaying a proposal from an earlier leader.
 *
 * Otherwise, we wait for either all replicas to reply or until we achieve
 * the desired quorum. We continue to wait for all replicas even after we know we cannot succeed
 * because we need to know if no node at all have accepted or if at least one has.
 * In the former case, a proposer is guaranteed no-one will
 * replay its value; in the latter we don't, so we must timeout in case another
 * leader replays it before we can; see CASSANDRA-6013
 */
public class ProposeCallback extends AbstractPaxosCallback<Boolean>
{
    private static final Logger logger = LoggerFactory.getLogger(ProposeCallback.class);

    private final AtomicInteger accepts = new AtomicInteger(0);
    private final int requiredAccepts;
    private final boolean failFast;

    public ProposeCallback(int totalTargets, int requiredTargets, boolean failFast, ConsistencyLevel consistency)
    {
        super(totalTargets, consistency);
        this.requiredAccepts = requiredTargets;
        this.failFast = failFast;
    }

    public void response(MessageIn<Boolean> msg)
    {
        logger.debug("Propose response {} from {}", msg.payload, msg.from);

        if (msg.payload)
            accepts.incrementAndGet();

        latch.countDown();

        if (isSuccessful() || (failFast && (latch.getCount() + accepts.get() < requiredAccepts)))
        {
            while (latch.getCount() > 0)
                latch.countDown();
        }
    }

    public int getAcceptCount()
    {
        return accepts.get();
    }

    public boolean isSuccessful()
    {
        return accepts.get() >= requiredAccepts;
    }

    // Note: this is only reliable if !failFast
    public boolean isFullyRefused()
    {
        // We need to check the latch first to avoid racing with a late arrival
        // between the latch check and the accepts one
        return latch.getCount() == 0 && accepts.get() == 0;
    }
}
