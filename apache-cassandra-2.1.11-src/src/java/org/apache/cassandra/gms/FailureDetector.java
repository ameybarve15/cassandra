
/**
 * This FailureDetector is an implementation of the paper titled
 * "The Phi Accrual Failure Detector" by Hayashibara.
 * Check the paper and the <i>IFailureDetector</i> interface for details.
 */
public class FailureDetector implements IFailureDetector, FailureDetectorMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=FailureDetector";
    private static final int SAMPLE_SIZE = 1000;
    protected static final long INITIAL_VALUE_NANOS = TimeUnit.NANOSECONDS.convert(getInitialValue(), TimeUnit.MILLISECONDS);
    private static final long DEFAULT_MAX_PAUSE = 5000L * 1000000L; // 5 seconds
    private static final long MAX_LOCAL_PAUSE_IN_NANOS = getMaxLocalPause();
    private long lastInterpret = System.nanoTime();
    private long lastPause = 0L;

    private static long getMaxLocalPause()
    {
        if (System.getProperty("cassandra.max_local_pause_in_ms") != null)
        {
            long pause = Long.parseLong(System.getProperty("cassandra.max_local_pause_in_ms"));
            return pause * 1000000L;
        }
        else
            return DEFAULT_MAX_PAUSE;
    }

    public static final IFailureDetector instance = new FailureDetector();

    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    private final double PHI_FACTOR = 1.0 / Math.log(10.0); // 0.434...

    private final Map<InetAddress, ArrivalWindow> arrivalSamples = new Hashtable<InetAddress, ArrivalWindow>();
    private final List<IFailureDetectionEventListener> fdEvntListeners 
        = new CopyOnWriteArrayList<IFailureDetectionEventListener>();

    public FailureDetector()
    {
        // Register this instance with JMX
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static long getInitialValue()
    {
        String newvalue = System.getProperty("cassandra.fd_initial_value_ms");
        if (newvalue == null)
        {
            return Gossiper.intervalInMillis * 2;
        }
        else
        {
            return Integer.parseInt(newvalue);
        }
    }

    public String getAllEndpointStates()
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<InetAddress, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            sb.append(entry.getKey()).append("\n");
            appendEndpointState(sb, entry.getValue());
        }
        return sb.toString();
    }

    public Map<String, String> getSimpleStates()
    {
        Map<String, String> nodesStatus = new HashMap<String, String>(Gossiper.instance.endpointStateMap.size());
        for (Map.Entry<InetAddress, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            if (entry.getValue().isAlive())
                nodesStatus.put(entry.getKey().toString(), "UP");
            else
                nodesStatus.put(entry.getKey().toString(), "DOWN");
        }
        return nodesStatus;
    }

    public int getDownEndpointCount()
    {
        int count = 0;
        for (Map.Entry<InetAddress, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            if (!entry.getValue().isAlive())
                count++;
        }
        return count;
    }

    public int getUpEndpointCount()
    {
        int count = 0;
        for (Map.Entry<InetAddress, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            if (entry.getValue().isAlive())
                count++;
        }
        return count;
    }

    public String getEndpointState(String address) throws UnknownHostException
    {
        StringBuilder sb = new StringBuilder();
        EndpointState endpointState = Gossiper.instance.getEndpointStateForEndpoint(InetAddress.getByName(address));
        appendEndpointState(sb, endpointState);
        return sb.toString();
    }

    private void appendEndpointState(StringBuilder sb, EndpointState endpointState)
    {
        sb.append("  generation:").append(endpointState.getHeartBeatState().getGeneration()).append("\n");
        sb.append("  heartbeat:").append(endpointState.getHeartBeatState().getHeartBeatVersion()).append("\n");
        for (Map.Entry<ApplicationState, VersionedValue> state : endpointState.applicationState.entrySet())
        {
            if (state.getKey() == ApplicationState.TOKENS)
                continue;
            sb.append("  ").append(state.getKey()).append(":").append(state.getValue().version).append(":").append(state.getValue().value).append("\n");
        }
        if (endpointState.applicationState.containsKey(ApplicationState.TOKENS))
        {
            sb.append("  TOKENS:").append(endpointState.applicationState.get(ApplicationState.TOKENS).version).append(":<hidden>\n");
        }
        else
        {
            sb.append("  TOKENS: not present\n");
        }
    }

    /**
     * Dump the inter arrival times for examination if necessary.
     */
    public void dumpInterArrivalTimes()
    {
        File file = FileUtils.createTempFile("failuredetector-", ".dat");

        OutputStream os = null;
        try
        {
            os = new BufferedOutputStream(new FileOutputStream(file, true));
            os.write(toString().getBytes());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file);
        }
        finally
        {
            FileUtils.closeQuietly(os);
        }
    }

    public void setPhiConvictThreshold(double phi)
    {
        DatabaseDescriptor.setPhiConvictThreshold(phi);
    }

    public double getPhiConvictThreshold()
    {
        return DatabaseDescriptor.getPhiConvictThreshold();
    }

    public boolean isAlive(InetAddress ep)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
            return true;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
        // we could assert not-null, but having isAlive fail screws a node over so badly that
        // it's worth being defensive here so minor bugs don't cause disproportionate
        // badness.  (See CASSANDRA-1463 for an example).

        return epState != null && epState.isAlive();
    }

    public void report(InetAddress ep)
    {
        long now = System.nanoTime();
        ArrivalWindow heartbeatWindow = arrivalSamples.get(ep);
        if (heartbeatWindow == null)
        {
            // avoid adding an empty ArrivalWindow to the Map
            heartbeatWindow = new ArrivalWindow(SAMPLE_SIZE);
            heartbeatWindow.add(now, ep);
            arrivalSamples.put(ep, heartbeatWindow);
        }
        else
        {
            heartbeatWindow.add(now, ep);
        }
    }

    public void interpret(InetAddress ep)
    {
        ArrivalWindow hbWnd = arrivalSamples.get(ep);
        if (hbWnd == null)
        {
            return;
        }
        long now = System.nanoTime();
        long diff = now - lastInterpret;
        lastInterpret = now;
        if (diff > MAX_LOCAL_PAUSE_IN_NANOS)
        {
            logger.warn("Not marking nodes down due to local pause of {} > {}", diff, MAX_LOCAL_PAUSE_IN_NANOS);
            lastPause = now;
            return;
        }
        if (System.nanoTime() - lastPause < MAX_LOCAL_PAUSE_IN_NANOS)
        {
            logger.debug("Still not marking nodes down due to local pause");
            return;
        }
        double phi = hbWnd.phi(now);
        if (logger.isTraceEnabled())
            logger.trace("PHI for " + ep + " : " + phi);

        if (PHI_FACTOR * phi > getPhiConvictThreshold())
        {
            logger.trace("notifying listeners that {} is down", ep);
            logger.trace("intervals: {} mean: {}", hbWnd, hbWnd.mean());
            for (IFailureDetectionEventListener listener : fdEvntListeners)
            {
                listener.convict(ep, phi);
            }
        }
    }

    public void forceConviction(InetAddress ep)
    {
        logger.debug("Forcing conviction of {}", ep);
        for (IFailureDetectionEventListener listener : fdEvntListeners)
        {
            listener.convict(ep, getPhiConvictThreshold());
        }
    }

    public void remove(InetAddress ep)
    {
        arrivalSamples.remove(ep);
    }

    public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener)
    {
        fdEvntListeners.add(listener);
    }

    public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener)
    {
        fdEvntListeners.remove(listener);
    }

    public static void main(String[] args)
    {
    }
}

/*
 This class is not thread safe.
 */
class ArrayBackedBoundedStats
{
    private final long[] arrivalIntervals;
    private long sum = 0;
    private int index = 0;
    private boolean isFilled = false;
    private volatile double mean = 0;

    public ArrayBackedBoundedStats(final int size)
    {
        arrivalIntervals = new long[size];
    }

    public void add(long interval)
    {
        if(index == arrivalIntervals.length)
        {
            isFilled = true;
            index = 0;
        }

        if(isFilled)
            sum = sum - arrivalIntervals[index];

        arrivalIntervals[index++] = interval;
        sum += interval;
        mean = (double)sum / size();
    }

    private int size()
    {
        return isFilled ? arrivalIntervals.length : index;
    }
}

class ArrivalWindow
{
    private long tLast = 0L;
    private final ArrayBackedBoundedStats arrivalIntervals;

    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    private final double PHI_FACTOR = 1.0 / Math.log(10.0);

    // in the event of a long partition, never record an interval longer than the rpc timeout,
    // since if a host is regularly experiencing connectivity problems lasting this long we'd
    // rather mark it down quickly instead of adapting
    // this value defaults to the same initial value the FD is seeded with
    private final long MAX_INTERVAL_IN_NANO = getMaxInterval();

    ArrivalWindow(int size)
    {
        arrivalIntervals = new ArrayBackedBoundedStats(size);
    }

    private static long getMaxInterval()
    {
        String newvalue = System.getProperty("cassandra.fd_max_interval_ms");
        if (newvalue == null)
        {
            return FailureDetector.INITIAL_VALUE_NANOS;
        }
        else
        {
            return TimeUnit.NANOSECONDS.convert(Integer.parseInt(newvalue), TimeUnit.MILLISECONDS);
        }
    }

    synchronized void add(long value, InetAddress ep)
    {
        assert tLast >= 0;
        if (tLast > 0L)
        {
            long interArrivalTime = (value - tLast);
            if (interArrivalTime <= MAX_INTERVAL_IN_NANO)
                arrivalIntervals.add(interArrivalTime);

        }
        else
        {
            // We use a very large initial interval since the "right" average depends on the cluster size
            // and it's better to err high (false negatives, which will be corrected by waiting a bit longer)
            // than low (false positives, which cause "flapping").
            arrivalIntervals.add(FailureDetector.INITIAL_VALUE_NANOS);
        }
        tLast = value;
    }

    double mean()
    {
        return arrivalIntervals.mean();
    }

    // see CASSANDRA-2597 for an explanation of the math at work here.
    double phi(long tnow)
    {
        assert arrivalIntervals.mean() > 0 && tLast > 0; // should not be called before any samples arrive
        long t = tnow - tLast;
        return t / mean();
    }
}

