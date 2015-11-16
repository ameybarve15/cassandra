

/**
 * Extends Thrift's TServerSocket to allow customization of various desirable TCP properties.
 */
public class TCustomServerSocket extends TServerTransport
{

    private static final Logger logger = LoggerFactory.getLogger(TCustomServerSocket.class);

    /**
     * Underlying serversocket object
     */
    private ServerSocket serverSocket = null;

    private final boolean keepAlive;
    private final Integer sendBufferSize;
    private final Integer recvBufferSize;

    /**
     * Allows fine-tuning of the server socket including keep-alive, reuse of addresses, send and receive buffer sizes.
     *
     * @param bindAddr
     * @param keepAlive
     * @param sendBufferSize
     * @param recvBufferSize
     * @throws TTransportException
     */
    public TCustomServerSocket(InetSocketAddress bindAddr, boolean keepAlive, Integer sendBufferSize,
            Integer recvBufferSize, Integer listenBacklog)
            throws TTransportException
    {
        try
        {
            // Make server socket
            serverSocket = new ServerSocket();
            // Prevent 2MSL delay problem on server restarts
            serverSocket.setReuseAddress(true);
            // Bind to listening port
            serverSocket.bind(bindAddr, listenBacklog);
        }
        catch (IOException ioe)
        {
            serverSocket = null;
            throw new TTransportException("Could not create ServerSocket on address " + bindAddr.toString() + ".");
        }

        this.keepAlive = keepAlive;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
    }

    public TCustomServerSocket(ServerSocket socket, boolean keepAlive, Integer sendBufferSize, Integer recvBufferSize)
    {
        this.serverSocket = socket;
        this.keepAlive = keepAlive;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
    }

    @Override
    protected TCustomSocket acceptImpl() throws TTransportException
    {

        if (serverSocket == null)
            throw new TTransportException(TTransportException.NOT_OPEN, "No underlying server socket.");

        TCustomSocket tsocket = null;
        Socket socket = null;
        try
        {
            socket = serverSocket.accept();
            tsocket = new TCustomSocket(socket);
            tsocket.setTimeout(0);
        }
        catch (IOException iox)
        {
            throw new TTransportException(iox);
        }

        try
        {
            socket.setKeepAlive(this.keepAlive);
        }
        catch (SocketException se)
        {
            logger.warn("Failed to set keep-alive on Thrift socket.", se);
        }

        if (this.sendBufferSize != null)
        {
            try
            {
                socket.setSendBufferSize(this.sendBufferSize.intValue());
            }
            catch (SocketException se)
            {
                logger.warn("Failed to set send buffer size on Thrift socket.", se);
            }
        }

        if (this.recvBufferSize != null)
        {
            try
            {
                socket.setReceiveBufferSize(this.recvBufferSize.intValue());
            }
            catch (SocketException se)
            {
                logger.warn("Failed to set receive buffer size on Thrift socket.", se);
            }
        }

        return tsocket;
    }

    @Override
    public void listen() throws TTransportException
    {
        // Make sure not to block on accept
        if (serverSocket != null)
        {
            try
            {
                serverSocket.setSoTimeout(100);
            }
            catch (SocketException sx)
            {
                logger.error("Could not set socket timeout.", sx);
            }
        }
    }

    @Override
    public void close()
    {
        if (serverSocket != null)
        {
            try
            {
                serverSocket.close();
            }
            catch (IOException iox)
            {
                logger.warn("Could not close server socket.", iox);
            }
            serverSocket = null;
        }
    }

    @Override
    public void interrupt()
    {
        // The thread-safeness of this is dubious, but Java documentation suggests
        // that it is safe to do this from a different thread context
        close();
    }
}
