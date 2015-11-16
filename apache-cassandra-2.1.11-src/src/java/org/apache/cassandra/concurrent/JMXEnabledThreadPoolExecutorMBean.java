

/**
 * @see org.apache.cassandra.metrics.ThreadPoolMetrics
 */
@Deprecated
public interface JMXEnabledThreadPoolExecutorMBean extends IExecutorMBean
{
    /**
     * Get the number of tasks that had blocked before being accepted (or
     * rejected).
     */
    public int getTotalBlockedTasks();

    /**
     * Get the number of tasks currently blocked, waiting to be accepted by
     * the executor (because all threads are busy and the backing queue is full).
     */
    public int getCurrentlyBlockedTasks();

    /**
     * Returns core pool size of thread pool.
     */
    public int getCoreThreads();

    /**
     * Allows user to resize core pool size of the thread pool.
     */
    public void setCoreThreads(int number);

    /**
     * Returns maximum pool size of thread pool.
     */
    public int getMaximumThreads();

    /**
     * Allows user to resize maximum size of the thread pool.
     */
    public void setMaximumThreads(int number);
}
