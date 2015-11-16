
public class TCustomNonblockingServerSocket extends TNonblockingServerSocket
{
    private static final Logger logger = LoggerFactory.getLogger(TCustomNonblockingServerSocket.class);
    private final boolean keepAlive;
    private final Integer sendBufferSize;
    private final Integer recvBufferSize;

    public TCustomNonblockingServerSocket(InetSocketAddress bindAddr, boolean keepAlive, Integer sendBufferSize, Integer recvBufferSize) throws TTransportException
    {
        super(bindAddr);
        this.keepAlive = keepAlive;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
    }

    @Override
    protected TNonblockingSocket acceptImpl() throws TTransportException
    {
        TNonblockingSocket tsocket = super.acceptImpl();
        tsocket.getSocketChannel();
        
        Socket socket = tsocket.getSocketChannel().socket();
        socket.setKeepAlive(this.keepAlive);
        socket.setSendBufferSize(this.sendBufferSize.intValue());
        socket.setReceiveBufferSize(this.recvBufferSize.intValue());

        return tsocket;
    }
}
