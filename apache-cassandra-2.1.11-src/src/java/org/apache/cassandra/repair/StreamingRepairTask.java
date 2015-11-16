
/**
 * Task that make two nodes exchange (stream) some ranges (for a given table/cf).
 * This handle the case where the local node is neither of the two nodes that
 * must stream their range, and allow to register a callback to be called on
 * completion.
 */
public class StreamingRepairTask implements Runnable, StreamEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingRepairTask.class);

    /** Repair session ID that this streaming task belongs */
    public final RepairJobDesc desc;
    public final SyncRequest request;

    public StreamingRepairTask(RepairJobDesc desc, SyncRequest request)
    {
        this.desc = desc;
        this.request = request;
    }

    public void run()
    {
        if (request.src.equals(FBUtilities.getBroadcastAddress()))
            initiateStreaming();
        else
            forwardToSource();
    }

    private void initiateStreaming()
    {
        long repairedAt = ActiveRepairService.UNREPAIRED_SSTABLE;
        InetAddress dest = request.dst;
        InetAddress preferred = SystemKeyspace.getPreferredIP(dest);
        if (desc.parentSessionId != null && ActiveRepairService.instance.getParentRepairSession(desc.parentSessionId) != null)
            repairedAt = ActiveRepairService.instance.getParentRepairSession(desc.parentSessionId).repairedAt;
        logger.info(String.format("[streaming task #%s] Performing streaming repair of %d ranges with %s", desc.sessionId, request.ranges.size(), request.dst));
        StreamResultFuture op = new StreamPlan("Repair", repairedAt, 1)
                                    .flushBeforeTransfer(true)
                                    // request ranges from the remote node
                                    .requestRanges(dest, preferred, desc.keyspace, request.ranges, desc.columnFamily)
                                    // send ranges to the remote node
                                    .transferRanges(dest, preferred, desc.keyspace, request.ranges, desc.columnFamily)
                                    .execute();
        op.addEventListener(this);
    }

    private void forwardToSource()
    {
        logger.info(String.format("[repair #%s] Forwarding streaming repair of %d ranges to %s (to be streamed with %s)", desc.sessionId, request.ranges.size(), request.src, request.dst));
        MessagingService.instance().sendOneWay(request.createMessage(), request.src);
    }

    public void handleStreamEvent(StreamEvent event)
    {
        // Nothing to do here, all we care about is the final success or failure and that's handled by
        // onSuccess and onFailure
    }

    /**
     * If we succeeded on both stream in and out, reply back to the initiator.
     */
    public void onSuccess(StreamState state)
    {
        logger.info(String.format("[repair #%s] streaming task succeed, returning response to %s", desc.sessionId, request.initiator));
        MessagingService.instance().sendOneWay(new SyncComplete(desc, request.src, request.dst, true).createMessage(), request.initiator);
    }

    /**
     * If we failed on either stream in or out, reply fail to the initiator.
     */
    public void onFailure(Throwable t)
    {
        MessagingService.instance().sendOneWay(new SyncComplete(desc, request.src, request.dst, false).createMessage(), request.initiator);
    }
}
