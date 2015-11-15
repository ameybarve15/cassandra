

/**
 * Encapsulates the current client state (session).
 *
 * We rely on the Thrift server to tell us what socket it is
 * executing a request for via setCurrentSocket, after which currentSession can do its job anywhere.
 */
public class ThriftSessionManager
{
    private static final Logger logger = LoggerFactory.getLogger(ThriftSessionManager.class);
    public final static ThriftSessionManager instance = new ThriftSessionManager();

    private final ThreadLocal<SocketAddress> remoteSocket = new ThreadLocal<>();
    private final ConcurrentHashMap<SocketAddress, ThriftClientState> activeSocketSessions = new ConcurrentHashMap<>();

    /**
     * @param socket the address on which the current thread will work on requests for until further notice
     */
    public void setCurrentSocket(SocketAddress socket)
    {
        remoteSocket.set(socket);
    }

    /**
     * @return the current session for the most recently given socket on this thread
     */
    public ThriftClientState currentSession()
    {
        SocketAddress socket = remoteSocket.get();
        assert socket != null;

        ThriftClientState cState = activeSocketSessions.get(socket);
        if (cState == null)
        {
            //guarantee atomicity
            ThriftClientState newState = new ThriftClientState(socket);
            cState = activeSocketSessions.putIfAbsent(socket, newState);
            if (cState == null)
                cState = newState;
        }
        return cState;
    }

    /**
     * The connection associated with @param socket is permanently finished.
     */
    public void connectionComplete(SocketAddress socket)
    {
        assert socket != null;
        activeSocketSessions.remove(socket);
        if (logger.isTraceEnabled())
            logger.trace("ClientState removed for socket addr {}", socket);
    }
    
    public int getConnectedClients()
    {
        return activeSocketSessions.size();
    }
}
