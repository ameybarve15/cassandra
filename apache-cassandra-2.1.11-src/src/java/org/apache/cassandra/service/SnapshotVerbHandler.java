

public class SnapshotVerbHandler implements IVerbHandler<SnapshotCommand>
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotVerbHandler.class);

    public void doVerb(MessageIn<SnapshotCommand> message, int id)
    {
        SnapshotCommand command = message.payload;
        if (command.clear_snapshot)
        {
            Keyspace.clearSnapshot(command.snapshot_name, command.keyspace);
        }
        else
            Keyspace.open(command.keyspace).getColumnFamilyStore(command.column_family).snapshot(command.snapshot_name);
        logger.debug("Enqueuing response to snapshot request {} to {}", command.snapshot_name, message.from);
        MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), id, message.from);
    }
}
