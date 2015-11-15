

public class CommitVerbHandler implements IVerbHandler<Commit>
{
    public void doVerb(MessageIn<Commit> message, int id)
    {
        PaxosState.commit(message.payload);

        WriteResponse response = new WriteResponse();
        Tracing.trace("Enqueuing acknowledge to {}", message.from);
        MessagingService.instance().sendReply(response.createMessage(), id, message.from);
    }
}
