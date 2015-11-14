

public interface StorageProxyMBean
{
    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#lastOpCount
     */
    @Deprecated
    public long getReadOperations();
    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#totalLatencyHistogram
     */
    @Deprecated
    public long getTotalReadLatencyMicros();
    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#recentLatencyHistogram
     */
    @Deprecated
    public double getRecentReadLatencyMicros();
    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#totalLatencyHistogram
     */
    @Deprecated
    public long[] getTotalReadLatencyHistogramMicros();
    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#recentLatencyHistogram
     */
    @Deprecated
    public long[] getRecentReadLatencyHistogramMicros();

    @Deprecated
    public long getRangeOperations();
    @Deprecated
    public long getTotalRangeLatencyMicros();
    @Deprecated
    public double getRecentRangeLatencyMicros();
    @Deprecated
    public long[] getTotalRangeLatencyHistogramMicros();
    @Deprecated
    public long[] getRecentRangeLatencyHistogramMicros();

    @Deprecated
    public long getWriteOperations();
    @Deprecated
    public long getTotalWriteLatencyMicros();
    @Deprecated
    public double getRecentWriteLatencyMicros();
    @Deprecated
    public long[] getTotalWriteLatencyHistogramMicros();
    @Deprecated
    public long[] getRecentWriteLatencyHistogramMicros();

    public long getTotalHints();
    public boolean getHintedHandoffEnabled();
    public Set<String> getHintedHandoffEnabledByDC();
    public void setHintedHandoffEnabled(boolean b);
    public void setHintedHandoffEnabledByDCList(String dcs);
    public int getMaxHintWindow();
    public void setMaxHintWindow(int ms);
    public int getMaxHintsInProgress();
    public void setMaxHintsInProgress(int qs);
    public int getHintsInProgress();

    public Long getRpcTimeout();
    public void setRpcTimeout(Long timeoutInMillis);
    public Long getReadRpcTimeout();
    public void setReadRpcTimeout(Long timeoutInMillis);
    public Long getWriteRpcTimeout();
    public void setWriteRpcTimeout(Long timeoutInMillis);
    public Long getCounterWriteRpcTimeout();
    public void setCounterWriteRpcTimeout(Long timeoutInMillis);
    public Long getCasContentionTimeout();
    public void setCasContentionTimeout(Long timeoutInMillis);
    public Long getRangeRpcTimeout();
    public void setRangeRpcTimeout(Long timeoutInMillis);
    public Long getTruncateRpcTimeout();
    public void setTruncateRpcTimeout(Long timeoutInMillis);

    public void setNativeTransportMaxConcurrentConnections(Long nativeTransportMaxConcurrentConnections);
    public Long getNativeTransportMaxConcurrentConnections();

    public void reloadTriggerClasses();

    public long getReadRepairAttempted();
    public long getReadRepairRepairedBlocking();
    public long getReadRepairRepairedBackground();

    /** Returns each live node's schema version */
    public Map<String, List<String>> getSchemaVersions();
}
