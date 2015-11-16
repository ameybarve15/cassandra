

public class SinkManager
{
    private static final Set<IMessageSink> messageSinks = new CopyOnWriteArraySet<>();
    private static final Set<IRequestSink> requestSinks = new CopyOnWriteArraySet<>();

    public static void add(IMessageSink ms)
    {
        messageSinks.add(ms);
    }

    public static void add(IRequestSink rs)
    {
        requestSinks.add(rs);
    }

    public static void remove(IMessageSink ms)
    {
        messageSinks.remove(ms);
    }

    public static void remove(IRequestSink rs)
    {
        requestSinks.remove(rs);
    }

    public static void clear()
    {
        messageSinks.clear();
        requestSinks.clear();
    }

    public static MessageOut processOutboundMessage(MessageOut message, int id, InetAddress to)
    {
        if (messageSinks.isEmpty())
            return message;

        for (IMessageSink ms : messageSinks)
        {
            message = ms.handleMessage(message, id, to);
            if (message == null)
                return null;
        }
        return message;
    }

    public static MessageIn processInboundMessage(MessageIn message, int id)
    {
        if (messageSinks.isEmpty())
            return message;

        for (IMessageSink ms : messageSinks)
        {
            message = ms.handleMessage(message, id, null);
            if (message == null)
                return null;
        }
        return message;
    }

    public static IMutation processWriteRequest(IMutation mutation)
    {
        if (requestSinks.isEmpty())
            return mutation;

        for (IRequestSink rs : requestSinks)
        {
            mutation = rs.handleWriteRequest(mutation);
            if (mutation == null)
                return null;
        }
        return mutation;
    }
}
