
public class BulkLoadConnectionFactory implements StreamConnectionFactory
{
    private final boolean outboundBindAny;
    private final int storagePort;
    private final int secureStoragePort;
    private final EncryptionOptions.ServerEncryptionOptions encryptionOptions;

    public BulkLoadConnectionFactory(int storagePort, int secureStoragePort, EncryptionOptions.ServerEncryptionOptions encryptionOptions, boolean outboundBindAny)
    {
        this.storagePort = storagePort;
        this.secureStoragePort = secureStoragePort;
        this.encryptionOptions = encryptionOptions;
        this.outboundBindAny = outboundBindAny;
    }

    public Socket createConnection(InetAddress peer) throws IOException
    {
        // Connect to secure port for all peers if ServerEncryptionOptions is configured other than 'none'
        // When 'all', 'dc' and 'rack', server nodes always have SSL port open, and since thin client like sstableloader
        // does not know which node is in which dc/rack, connecting to SSL port is always the option.
        if (encryptionOptions != null && encryptionOptions.internode_encryption != EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none)
        {
            if (outboundBindAny)
                return SSLFactory.getSocket(encryptionOptions, peer, secureStoragePort);
            else
                return SSLFactory.getSocket(encryptionOptions, peer, secureStoragePort, FBUtilities.getLocalAddress(), 0);
        }
        else
        {
            Socket socket = SocketChannel.open(new InetSocketAddress(peer, storagePort)).socket();
            if (outboundBindAny && !socket.isBound())
                socket.bind(new InetSocketAddress(FBUtilities.getLocalAddress(), 0));
            return socket;
        }
    }
}
