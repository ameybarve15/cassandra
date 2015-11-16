

/**
 * Exposes client request scheduling metrics for a particular scheduler queue.
 * @see org.apache.cassandra.metrics.LatencyMetrics
 */
@Deprecated
public interface WeightedQueueMBean
{
    public long getOperations();
    public long getTotalLatencyMicros();
    public double getRecentLatencyMicros();
    public long[] getTotalLatencyHistogramMicros();
    public long[] getRecentLatencyHistogramMicros();
}
