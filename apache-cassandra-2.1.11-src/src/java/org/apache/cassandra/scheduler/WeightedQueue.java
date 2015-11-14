

class WeightedQueue implements WeightedQueueMBean
{
    private final LatencyMetrics metric;

    public final String key;
    public final int weight;
    private final SynchronousQueue<Entry> queue;
    public WeightedQueue(String key, int weight)
    {
        this.key = key;
        this.weight = weight;
        this.queue = new SynchronousQueue<Entry>(true);
        this.metric =  new LatencyMetrics("scheduler", "WeightedQueue", key);
    }

    public void register()
    {
        // expose monitoring data
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        mbs.registerMBean(this, new ObjectName("org.apache.cassandra.scheduler:type=WeightedQueue,queue=" + key));
    }

    public void put(Thread t, long timeoutMS) throws InterruptedException, TimeoutException
    {
        if (!queue.offer(new WeightedQueue.Entry(t), timeoutMS, TimeUnit.MILLISECONDS))
            throw new TimeoutException("Failed to acquire request scheduler slot for '" + key + "'");
    }

    public Thread poll()
    {
        Entry e = queue.poll();
        if (e == null)
            return null;
        metric.addNano(System.nanoTime() - e.creationTime);
        return e.thread;
    }

    private final static class Entry
    {
        public final long creationTime = System.nanoTime();
        public final Thread thread;
        public Entry(Thread thread)
        {
            this.thread = thread;
        }
    }

    /** MBean related methods */

    public long getOperations()
    {
        return metric.latency.count();
    }

    public long getTotalLatencyMicros()
    {
        return metric.totalLatency.count();
    }

    public double getRecentLatencyMicros()
    {
        return metric.getRecentLatency();
    }

    public long[] getTotalLatencyHistogramMicros()
    {
        return metric.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentLatencyHistogramMicros()
    {
        return metric.recentLatencyHistogram.getBuckets(true);
    }
}
