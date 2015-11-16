

public class CommitVerbHandler implements IVerbHandler<Commit>
{
    public void doVerb(MessageIn<Commit> message, int id)
    {
        PaxosState.commit(message.payload);

        WriteResponse response = new WriteResponse();
        MessagingService.instance().sendReply(response.createMessage(), id, message.from);
    }
}
