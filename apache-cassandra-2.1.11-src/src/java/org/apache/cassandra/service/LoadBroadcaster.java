

public class LoadBroadcaster implements IEndpointStateChangeSubscriber
{
    static final int BROADCAST_INTERVAL = 60 * 1000;

    public static final LoadBroadcaster instance = new LoadBroadcaster();

    private static final Logger logger = LoggerFactory.getLogger(LoadBroadcaster.class);

    private ConcurrentMap<InetAddress, Double> loadInfo = new ConcurrentHashMap<InetAddress, java.lang.Double>();

    private LoadBroadcaster()
    {
        Gossiper.instance.register(this);
    }

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state != ApplicationState.LOAD)
            return;
        loadInfo.put(endpoint, Double.valueOf(value.value));
    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        VersionedValue localValue = epState.getApplicationState(ApplicationState.LOAD);
        if (localValue != null)
        {
            onChange(endpoint, ApplicationState.LOAD, localValue);
        }
    }
    
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}

    public void onAlive(InetAddress endpoint, EndpointState state) {}

    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRestart(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        loadInfo.remove(endpoint);
    }

    public Map<InetAddress, Double> getLoadInfo()
    {
        return Collections.unmodifiableMap(loadInfo);
    }

    public void startBroadcasting()
    {
        // send the first broadcast "right away" (i.e., in 2 gossip heartbeats, when we should have someone to talk to);
        // after that send every BROADCAST_INTERVAL.
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                if (logger.isDebugEnabled())
                    logger.debug("Disseminating load info ...");
                Gossiper.instance.addLocalApplicationState(ApplicationState.LOAD,
                                                           StorageService.instance.valueFactory.load(StorageService.instance.getLoad()));
            }
        };
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(runnable, 2 * Gossiper.intervalInMillis, BROADCAST_INTERVAL, TimeUnit.MILLISECONDS);
    }
}

