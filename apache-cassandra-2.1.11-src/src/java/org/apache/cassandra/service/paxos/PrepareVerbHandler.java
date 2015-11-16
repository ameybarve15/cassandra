

public class PrepareVerbHandler implements IVerbHandler<Commit>
{
    public void doVerb(MessageIn<Commit> message, int id)
    {
        PrepareResponse response = PaxosState.prepare(message.payload);
        MessageOut<PrepareResponse> reply = new MessageOut<PrepareResponse>(MessagingService.Verb.REQUEST_RESPONSE, response, PrepareResponse.serializer);
        MessagingService.instance().sendReply(reply, id, message.from);
    }
}
