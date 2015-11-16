
/**
 * Interface that creates connection used by streaming.
 */
public interface StreamConnectionFactory
{
    Socket createConnection(InetAddress peer) throws IOException;
}
