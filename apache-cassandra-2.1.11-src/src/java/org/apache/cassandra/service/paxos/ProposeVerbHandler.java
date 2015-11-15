

public class ProposeVerbHandler implements IVerbHandler<Commit>
{
    public void doVerb(MessageIn<Commit> message, int id)
    {
        Boolean response = PaxosState.propose(message.payload);
        MessageOut<Boolean> reply = new MessageOut<Boolean>(MessagingService.Verb.REQUEST_RESPONSE, response, BooleanSerializer.serializer);
        MessagingService.instance().sendReply(reply, id, message.from);
    }
}
