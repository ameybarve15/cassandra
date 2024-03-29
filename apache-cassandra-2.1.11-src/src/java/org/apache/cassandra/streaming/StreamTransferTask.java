

/**
 * StreamTransferTask sends sections of SSTable files in certain ColumnFamily.
 */
public class StreamTransferTask extends StreamTask
{
    private static final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("StreamingTransferTaskTimeouts"));

    private final AtomicInteger sequenceNumber = new AtomicInteger(0);
    private boolean aborted = false;

    private final Map<Integer, OutgoingFileMessage> files = new HashMap<>();
    private final Map<Integer, ScheduledFuture> timeoutTasks = new HashMap<>();

    private long totalSize;

    public StreamTransferTask(StreamSession session, UUID cfId)
    {
        super(session, cfId);
    }

    public synchronized void addTransferFile(Ref<SSTableReader> ref, long estimatedKeys, List<Pair<Long, Long>> sections, long repairedAt)
    {
        assert ref.get() != null && cfId.equals(ref.get().metadata.cfId);
        OutgoingFileMessage message = new OutgoingFileMessage(ref, sequenceNumber.getAndIncrement(), estimatedKeys, sections, repairedAt);
        files.put(message.header.sequenceNumber, message);
        totalSize += message.header.size();
    }

    /**
     * Received ACK for file at {@code sequenceNumber}.
     *
     * @param sequenceNumber sequence number of file
     */
    public void complete(int sequenceNumber)
    {
        boolean signalComplete;
        synchronized (this)
        {
            ScheduledFuture timeout = timeoutTasks.remove(sequenceNumber);
            if (timeout != null)
                timeout.cancel(false);

            OutgoingFileMessage file = files.remove(sequenceNumber);
            if (file != null)
                file.complete();

            signalComplete = files.isEmpty();
        }

        // all file sent, notify session this task is complete.
        if (signalComplete)
            session.taskCompleted(this);
    }

    public synchronized void abort()
    {
        if (aborted)
            return;
        aborted = true;

        for (ScheduledFuture future : timeoutTasks.values())
            future.cancel(false);
        timeoutTasks.clear();

        Throwable fail = null;
        for (OutgoingFileMessage file : files.values())
        {
            try
            {
                file.complete();
            }
            catch (Throwable t)
            {
                if (fail == null) fail = t;
                else fail.addSuppressed(t);
            }
        }
        files.clear();
        if (fail != null)
            Throwables.propagate(fail);
    }

    public synchronized int getTotalNumberOfFiles()
    {
        return files.size();
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    public synchronized Collection<OutgoingFileMessage> getFileMessages()
    {
        // We may race between queuing all those messages and the completion of the completion of
        // the first ones. So copy the values to avoid a ConcurrentModificationException
        return new ArrayList<>(files.values());
    }

    public synchronized OutgoingFileMessage createMessageForRetry(int sequenceNumber)
    {
        // remove previous time out task to be rescheduled later
        ScheduledFuture future = timeoutTasks.remove(sequenceNumber);
        if (future != null)
            future.cancel(false);
        return files.get(sequenceNumber);
    }

    /**
     * Schedule timeout task to release reference for file sent.
     * When not receiving ACK after sending to receiver in given time,
     * the task will release reference.
     *
     * @param sequenceNumber sequence number of file sent.
     * @param time time to timeout
     * @param unit unit of given time
     * @return scheduled future for timeout task
     */
    public synchronized ScheduledFuture scheduleTimeout(final int sequenceNumber, long time, TimeUnit unit)
    {
        if (!files.containsKey(sequenceNumber))
            return null;

        ScheduledFuture future = timeoutExecutor.schedule(new Runnable()
        {
            public void run()
            {
                synchronized (StreamTransferTask.this)
                {
                    // remove so we don't cancel ourselves
                    timeoutTasks.remove(sequenceNumber);
                    StreamTransferTask.this.complete(sequenceNumber);
                }
            }
        }, time, unit);

        ScheduledFuture prev = timeoutTasks.put(sequenceNumber, future);
        assert prev == null;
        return future;
    }
}
