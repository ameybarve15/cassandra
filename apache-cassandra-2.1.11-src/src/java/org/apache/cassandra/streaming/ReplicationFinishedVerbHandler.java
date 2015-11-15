

public class ReplicationFinishedVerbHandler implements IVerbHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ReplicationFinishedVerbHandler.class);

    public void doVerb(MessageIn msg, int id)
    {
        StorageService.instance.confirmReplication(msg.from);
        MessageOut response = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE);
        if (logger.isDebugEnabled())
            logger.debug("Replying to {}@{}", id, msg.from);
        MessagingService.instance().sendReply(response, id, msg.from);
    }
}
