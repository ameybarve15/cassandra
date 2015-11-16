

public class ThriftServer implements CassandraDaemon.Server
{
    public final static String SYNC = "sync";
    public final static String ASYNC = "async";
    public final static String HSHA = "hsha";

    protected final InetAddress address;
    protected final int port;
    protected final int backlog;
    private volatile ThriftServerThread server;

    public ThriftServer(InetAddress address, int port, int backlog)
    {
        this.address = address;
        this.port = port;
        this.backlog = backlog;
    }

    public void start()
    {
        if (server == null)
        {
            CassandraServer iface = getCassandraServer();
            server = new ThriftServerThread(address, port, backlog, getProcessor(iface), getTransportFactory());
            server.start();
        }
    }

    public void stop()
    {
        if (server != null)
        {
            server.stopServer();
            server.join();

            server = null;
        }
    }

    public boolean isRunning()
    {
        return server != null;
    }

    /*
     * These methods are intended to be overridden to provide custom implementations.
     */
    protected CassandraServer getCassandraServer()
    {
        return new CassandraServer();
    }

    protected TProcessor getProcessor(CassandraServer server)
    {
        return new Cassandra.Processor<Cassandra.Iface>(server);
    }

    protected TTransportFactory getTransportFactory()
    {
        int tFramedTransportSize = DatabaseDescriptor.getThriftFramedTransportSize();
        return new TFramedTransport.Factory(tFramedTransportSize);
    }

    /**
     * Simple class to run the thrift connection accepting code in separate
     * thread of control.
     */
    private static class ThriftServerThread extends Thread
    {
        private final TServer serverEngine;

        public ThriftServerThread(InetAddress listenAddr,
                                  int listenPort,
                                  int listenBacklog,
                                  TProcessor processor,
                                  TTransportFactory transportFactory)
        {
            TServerFactory.Args args = new TServerFactory.Args();
            args.tProtocolFactory = new TBinaryProtocol.Factory(true, true);
            args.addr = new InetSocketAddress(listenAddr, listenPort);
            args.listenBacklog = listenBacklog;
            args.processor = processor;
            args.keepAlive = DatabaseDescriptor.getRpcKeepAlive();
            args.sendBufferSize = DatabaseDescriptor.getRpcSendBufferSize();
            args.recvBufferSize = DatabaseDescriptor.getRpcRecvBufferSize();
            args.inTransportFactory = transportFactory;
            args.outTransportFactory = transportFactory;
            
            serverEngine = new TServerCustomFactory(DatabaseDescriptor.getRpcServerType()).buildTServer(args);
        }

        public void run()
        {
            serverEngine.serve();
        }

        public void stopServer()
        {
            serverEngine.stop();
        }
    }
}
