

public class PrepareCallback extends AbstractPaxosCallback<PrepareResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareCallback.class);

    public boolean promised = true;
    public Commit mostRecentCommit;
    public Commit mostRecentInProgressCommit;
    public Commit mostRecentInProgressCommitWithUpdate;

    private final Map<InetAddress, Commit> commitsByReplica = new ConcurrentHashMap<InetAddress, Commit>();

    public PrepareCallback(ByteBuffer key, CFMetaData metadata, int targets, ConsistencyLevel consistency)
    {
        super(targets, consistency);
        // need to inject the right key in the empty commit so comparing with empty commits in the reply works as expected
        mostRecentCommit = Commit.emptyCommit(key, metadata);
        mostRecentInProgressCommit = Commit.emptyCommit(key, metadata);
        mostRecentInProgressCommitWithUpdate = Commit.emptyCommit(key, metadata);
    }

    public synchronized void response(MessageIn<PrepareResponse> message)
    {
        PrepareResponse response = message.payload;
        logger.debug("Prepare response {} from {}", response, message.from);

        // In case of clock skew, another node could be proposing with ballot that are quite a bit
        // older than our own. In that case, we record the more recent commit we've received to make
        // sure we re-prepare on an older ballot.
        if (response.inProgressCommit.isAfter(mostRecentInProgressCommit))
            mostRecentInProgressCommit = response.inProgressCommit;

        if (!response.promised)
        {
            promised = false;
            while (latch.getCount() > 0)
                latch.countDown();
            return;
        }

        commitsByReplica.put(message.from, response.mostRecentCommit);
        if (response.mostRecentCommit.isAfter(mostRecentCommit))
            mostRecentCommit = response.mostRecentCommit;

        // If some response has an update, then we should replay the update with the highest ballot. So find
        // the the highest commit that actually have an update
        if (response.inProgressCommit.isAfter(mostRecentInProgressCommitWithUpdate) && !response.inProgressCommit.update.isEmpty())
            mostRecentInProgressCommitWithUpdate = response.inProgressCommit;

        latch.countDown();
    }

    public Iterable<InetAddress> replicasMissingMostRecentCommit()
    {
        return Iterables.filter(commitsByReplica.keySet(), new Predicate<InetAddress>()
        {
            public boolean apply(InetAddress inetAddress)
            {
                return (!commitsByReplica.get(inetAddress).ballot.equals(mostRecentCommit.ballot));
            }
        });
    }
}
