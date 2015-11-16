

/**
 * StreamTask is an abstraction of the streaming task performed over specific ColumnFamily.
 */
public abstract class StreamTask
{
    /** StreamSession that this task belongs */
    protected final StreamSession session;

    protected final UUID cfId;

    protected StreamTask(StreamSession session, UUID cfId)
    {
        this.session = session;
        this.cfId = cfId;
    }

    /**
     * @return total number of files this task receives/streams.
     */
    public abstract int getTotalNumberOfFiles();

    /**
     * @return total bytes expected to receive
     */
    public abstract long getTotalSize();

    /**
     * Abort the task.
     * Subclass should implement cleaning up resources.
     */
    public abstract void abort();

    /**
     * @return StreamSummary that describes this task
     */
    public StreamSummary getSummary()
    {
        return new StreamSummary(cfId, getTotalNumberOfFiles(), getTotalSize());
    }
}
