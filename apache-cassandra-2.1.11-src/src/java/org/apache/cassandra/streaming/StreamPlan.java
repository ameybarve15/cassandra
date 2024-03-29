

/**
 * {@link StreamPlan} is a helper class that builds StreamOperation of given configuration.
 *
 * This is the class you want to use for building streaming plan and starting streaming.
 */
public class StreamPlan
{
    private final UUID planId = UUIDGen.getTimeUUID();
    private final String description;
    private final List<StreamEventHandler> handlers = new ArrayList<>();
    private final long repairedAt;
    private final StreamCoordinator coordinator;

    private StreamConnectionFactory connectionFactory = new DefaultConnectionFactory();

    private boolean flushBeforeTransfer = true;

    /**
     * Start building stream plan.
     *
     * @param description Stream type that describes this StreamPlan
     */
    public StreamPlan(String description)
    {
        this(description, ActiveRepairService.UNREPAIRED_SSTABLE, 1);
    }

    public StreamPlan(String description, long repairedAt, int connectionsPerHost)
    {
        this.description = description;
        this.repairedAt = repairedAt;
        this.coordinator = new StreamCoordinator(connectionsPerHost, connectionFactory);
    }

    /**
     * Request data in {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param connecting Actual connecting address for the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddress from, InetAddress connecting, String keyspace, Collection<Range<Token>> ranges)
    {
        return requestRanges(from, connecting, keyspace, ranges, new String[0]);
    }

    /**
     * Request data in {@code columnFamilies} under {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param connecting Actual connecting address for the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddress from, InetAddress connecting, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        StreamSession session = coordinator.getOrCreateNextSession(from, connecting);
        session.addStreamRequest(keyspace, ranges, Arrays.asList(columnFamilies), repairedAt);
        return this;
    }

    /**
     * Add transfer task to send data of specific {@code columnFamilies} under {@code keyspace} and {@code ranges}.
     *
     * @see #transferRanges(java.net.InetAddress, java.net.InetAddress, String, java.util.Collection, String...)
     */
    public StreamPlan transferRanges(InetAddress to, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        return transferRanges(to, to, keyspace, ranges, columnFamilies);
    }

    /**
     * Add transfer task to send data of specific keyspace and ranges.
     *
     * @param to endpoint address of receiver
     * @param connecting Actual connecting address of the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddress to, InetAddress connecting, String keyspace, Collection<Range<Token>> ranges)
    {
        return transferRanges(to, connecting, keyspace, ranges, new String[0]);
    }

    /**
     * Add transfer task to send data of specific {@code columnFamilies} under {@code keyspace} and {@code ranges}.
     *
     * @param to endpoint address of receiver
     * @param connecting Actual connecting address of the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddress to, InetAddress connecting, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        StreamSession session = coordinator.getOrCreateNextSession(to, connecting);
        session.addTransferRanges(keyspace, ranges, Arrays.asList(columnFamilies), flushBeforeTransfer, repairedAt);
        return this;
    }

    /**
     * Add transfer task to send given SSTable files.
     *
     * @param to endpoint address of receiver
     * @param sstableDetails sstables with file positions and estimated key count.
     *                       this collection will be modified to remove those files that are successfully handed off
     * @return this object for chaining
     */
    public StreamPlan transferFiles(InetAddress to, Collection<StreamSession.SSTableStreamingSections> sstableDetails)
    {
        coordinator.transferFiles(to, sstableDetails);
        return this;

    }

    public StreamPlan listeners(StreamEventHandler handler, StreamEventHandler... handlers)
    {
        this.handlers.add(handler);
        if (handlers != null)
            Collections.addAll(this.handlers, handlers);
        return this;
    }

    /**
     * Set custom StreamConnectionFactory to be used for establishing connection
     *
     * @param factory StreamConnectionFactory to use
     * @return self
     */
    public StreamPlan connectionFactory(StreamConnectionFactory factory)
    {
        this.coordinator.setConnectionFactory(factory);
        return this;
    }

    /**
     * @return true if this plan has no plan to execute
     */
    public boolean isEmpty()
    {
        return !coordinator.hasActiveSessions();
    }

    /**
     * Execute this {@link StreamPlan} asynchronously.
     *
     * @return Future {@link StreamState} that you can use to listen on progress of streaming.
     */
    public StreamResultFuture execute()
    {
        return StreamResultFuture.init(planId, description, handlers, coordinator);
    }

    /**
     * Set flushBeforeTransfer option.
     * When it's true, will flush before streaming ranges. (Default: true)
     *
     * @param flushBeforeTransfer set to true when the node should flush before transfer
     * @return this object for chaining
     */
    public StreamPlan flushBeforeTransfer(boolean flushBeforeTransfer)
    {
        this.flushBeforeTransfer = flushBeforeTransfer;
        return this;
    }
}
