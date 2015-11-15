

public interface IMessageSink
{
    /**
     * Transform or drop an outgoing message
     *
     * @return null if the message is dropped, or the transformed message to send, which may be just
     * the original message
     */
    MessageOut handleMessage(MessageOut message, int id, InetAddress to);

    /**
     * Transform or drop an incoming message
     *
     * @return null if the message is dropped, or the transformed message to receive, which may be just
     * the original message
     */
    MessageIn handleMessage(MessageIn message, int id, InetAddress to);
}
