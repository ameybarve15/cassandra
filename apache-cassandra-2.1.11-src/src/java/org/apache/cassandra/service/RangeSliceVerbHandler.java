

public class RangeSliceVerbHandler implements IVerbHandler<AbstractRangeCommand>
{
    public void doVerb(MessageIn<AbstractRangeCommand> message, int id)
    {
        try
        {
            if (StorageService.instance.isBootstrapMode())
            {
                /* Don't service reads! */
                throw new RuntimeException("Cannot service reads while bootstrapping!");
            }
            RangeSliceReply reply = new RangeSliceReply(message.payload.executeLocally());
            Tracing.trace("Enqueuing response to {}", message.from);
            MessagingService.instance().sendReply(reply.createMessage(), id, message.from);
        }
        catch (TombstoneOverwhelmingException e)
        {
            // error already logged.  Drop the request
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
