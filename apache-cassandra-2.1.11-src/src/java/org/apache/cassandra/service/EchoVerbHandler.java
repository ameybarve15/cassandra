
public class EchoVerbHandler implements IVerbHandler<EchoMessage>
{
    private static final Logger logger = LoggerFactory.getLogger(EchoVerbHandler.class);

    public void doVerb(MessageIn<EchoMessage> message, int id)
    {
        MessageOut<EchoMessage> echoMessage = new MessageOut<EchoMessage>(MessagingService.Verb.REQUEST_RESPONSE, new EchoMessage(), EchoMessage.serializer);
        logger.trace("Sending a EchoMessage reply {}", message.from);
        MessagingService.instance().sendReply(echoMessage, id, message.from);
    }
}
