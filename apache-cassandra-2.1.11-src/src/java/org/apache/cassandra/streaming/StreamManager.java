

/**
 * StreamManager manages currently running {@link StreamResultFuture}s and provides status of all operation invoked.
 *
 * All stream operation should be created through this class to track streaming status and progress.
 */
public class StreamManager implements StreamManagerMBean
{
    public static final StreamManager instance = new StreamManager();

    /**
     * Gets streaming rate limiter.
     * When stream_throughput_outbound_megabits_per_sec is 0, this returns rate limiter
     * with the rate of Double.MAX_VALUE bytes per second.
     * Rate unit is bytes per sec.
     *
     * @return StreamRateLimiter with rate limit set based on peer location.
     */
    public static StreamRateLimiter getRateLimiter(InetAddress peer)
    {
        return new StreamRateLimiter(peer);
    }

    public static class StreamRateLimiter
    {
        private static final double BYTES_PER_MEGABIT = (1024 * 1024) / 8; // from bits
        private static final RateLimiter limiter = RateLimiter.create(Double.MAX_VALUE);
        private static final RateLimiter interDCLimiter = RateLimiter.create(Double.MAX_VALUE);
        private final boolean isLocalDC;

        public StreamRateLimiter(InetAddress peer)
        {
            double throughput = ((double) DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec()) * BYTES_PER_MEGABIT;
            mayUpdateThroughput(throughput, limiter);

            double interDCThroughput = ((double) DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSec()) * BYTES_PER_MEGABIT;
            mayUpdateThroughput(interDCThroughput, interDCLimiter);

            if (DatabaseDescriptor.getLocalDataCenter() != null && DatabaseDescriptor.getEndpointSnitch() != null)
                isLocalDC = DatabaseDescriptor.getLocalDataCenter().equals(
                            DatabaseDescriptor.getEndpointSnitch().getDatacenter(peer));
            else
                isLocalDC = true;
        }

        private void mayUpdateThroughput(double limit, RateLimiter rateLimiter)
        {
            // if throughput is set to 0, throttling is disabled
            if (limit == 0)
                limit = Double.MAX_VALUE;
            if (rateLimiter.getRate() != limit)
                rateLimiter.setRate(limit);
        }

        public void acquire(int toTransfer)
        {
            limiter.acquire(toTransfer);
            if (!isLocalDC)
                interDCLimiter.acquire(toTransfer);
        }
    }

    private final StreamEventJMXNotifier notifier = new StreamEventJMXNotifier();

    /*
     * Currently running streams. Removed after completion/failure.
     * We manage them in two different maps to distinguish plan from initiated ones to
     * receiving ones withing the same JVM.
     */
    private final Map<UUID, StreamResultFuture> initiatedStreams = new NonBlockingHashMap<>();
    private final Map<UUID, StreamResultFuture> receivingStreams = new NonBlockingHashMap<>();

    public Set<CompositeData> getCurrentStreams()
    {
        return Sets.newHashSet(Iterables.transform(Iterables.concat(initiatedStreams.values(), receivingStreams.values()), new Function<StreamResultFuture, CompositeData>()
        {
            public CompositeData apply(StreamResultFuture input)
            {
                return StreamStateCompositeData.toCompositeData(input.getCurrentState());
            }
        }));
    }

    public void register(final StreamResultFuture result)
    {
        result.addEventListener(notifier);
        // Make sure we remove the stream on completion (whether successful or not)
        result.addListener(new Runnable()
        {
            public void run()
            {
                initiatedStreams.remove(result.planId);
            }
        }, MoreExecutors.sameThreadExecutor());

        initiatedStreams.put(result.planId, result);
    }

    public void registerReceiving(final StreamResultFuture result)
    {
        result.addEventListener(notifier);
        // Make sure we remove the stream on completion (whether successful or not)
        result.addListener(new Runnable()
        {
            public void run()
            {
                receivingStreams.remove(result.planId);
            }
        }, MoreExecutors.sameThreadExecutor());

        receivingStreams.put(result.planId, result);
    }

    public StreamResultFuture getReceivingStream(UUID planId)
    {
        return receivingStreams.get(planId);
    }

    public void addNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
    {
        notifier.addNotificationListener(listener, filter, handback);
    }

    public void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException
    {
        notifier.removeNotificationListener(listener);
    }

    public void removeNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback) throws ListenerNotFoundException
    {
        notifier.removeNotificationListener(listener, filter, handback);
    }

    public MBeanNotificationInfo[] getNotificationInfo()
    {
        return notifier.getNotificationInfo();
    }
}
