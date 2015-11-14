

public class GossipShutdownVerbHandler implements IVerbHandler
{

    public void doVerb(MessageIn message, int id)
    {
        Gossiper.instance.markAsShutdown(message.from);
    }

}