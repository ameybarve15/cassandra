

public interface StorageProxyMBean
{
    @Deprecated
    public long getReadOperations();
    public long getTotalReadLatencyMicros();
    public double getRecentReadLatencyMicros();
    public long[] getTotalReadLatencyHistogramMicros();
    public long[] getRecentReadLatencyHistogramMicros();
    public long getRangeOperations();
    public long getTotalRangeLatencyMicros();
    public double getRecentRangeLatencyMicros();
    public long[] getTotalRangeLatencyHistogramMicros();
    public long[] getRecentRangeLatencyHistogramMicros();
    public long getWriteOperations();
    public long getTotalWriteLatencyMicros();
    public double getRecentWriteLatencyMicros();
    public long[] getTotalWriteLatencyHistogramMicros();
    public long[] getRecentWriteLatencyHistogramMicros();
    @Deprecated --/

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
