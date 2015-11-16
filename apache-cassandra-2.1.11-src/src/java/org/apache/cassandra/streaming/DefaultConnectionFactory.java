

public class DefaultConnectionFactory implements StreamConnectionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultConnectionFactory.class);

    private static final int MAX_CONNECT_ATTEMPTS = 3;

    /**
     * Connect to peer and start exchanging message.
     * When connect attempt fails, this retries for maximum of MAX_CONNECT_ATTEMPTS times.
     *
     * @param peer the peer to connect to.
     * @return the created socket.
     *
     * @throws IOException when connection failed.
     */
    public Socket createConnection(InetAddress peer) throws IOException
    {
        int attempts = 0;
        while (true)
        {
            try
            {
                Socket socket = OutboundTcpConnectionPool.newSocket(peer);
                socket.setSoTimeout(DatabaseDescriptor.getStreamingSocketTimeout());
                socket.setKeepAlive(true);
                return socket;
            }
            catch (IOException e)
            {
                if (++attempts >= MAX_CONNECT_ATTEMPTS)
                    throw e;

                long waitms = DatabaseDescriptor.getRpcTimeout() * (long)Math.pow(2, attempts);
                Thread.sleep(waitms);
            }
        }
    }
}
