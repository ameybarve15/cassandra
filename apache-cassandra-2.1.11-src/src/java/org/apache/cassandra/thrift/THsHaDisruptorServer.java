

public class THsHaDisruptorServer extends TDisruptorServer
{
    private static final Logger logger = LoggerFactory.getLogger(THsHaDisruptorServer.class.getName());

    /**
     * All the arguments to Non Blocking Server will apply here. In addition,
     * executor pool will be responsible for creating the internal threads which
     * will process the data. threads for selection usually are equal to the
     * number of cpu's
     */
    public THsHaDisruptorServer(Args args)
    {
        super(args);
        logger.info("Starting up {}", this);
    }

    @Override
    protected void beforeInvoke(Message buffer)
    {
        TNonblockingSocket socket = (TNonblockingSocket) buffer.transport;
        ThriftSessionManager.instance.setCurrentSocket(socket.getSocketChannel().socket().getRemoteSocketAddress());
    }

    public void beforeClose(Message buffer)
    {
        TNonblockingSocket socket = (TNonblockingSocket) buffer.transport;
        ThriftSessionManager.instance.connectionComplete(socket.getSocketChannel().socket().getRemoteSocketAddress());
    }

    public static class Factory implements TServerFactory
    {
        public TServer buildTServer(Args args)
        {
            if (DatabaseDescriptor.getClientEncryptionOptions().enabled)
                throw new RuntimeException("Client SSL is not supported for non-blocking sockets (hsha). Please remove client ssl from the configuration.");

            final InetSocketAddress addr = args.addr;
            TNonblockingServerTransport serverTransport;
            try
            {
                serverTransport = new TCustomNonblockingServerSocket(addr, args.keepAlive, args.sendBufferSize, args.recvBufferSize);
            }
            catch (TTransportException e)
            {
                throw new RuntimeException(String.format("Unable to create thrift socket to %s:%s", addr.getAddress(), addr.getPort()), e);
            }

            ThreadPoolExecutor invoker = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getRpcMinThreads(),
                                                                          DatabaseDescriptor.getRpcMaxThreads(),
                                                                          60L,
                                                                          TimeUnit.SECONDS,
                                                                          new SynchronousQueue<Runnable>(),
                                                                          new NamedThreadFactory("RPC-Thread"), "RPC-THREAD-POOL");

            com.thinkaurelius.thrift.util.TBinaryProtocol.Factory protocolFactory = new com.thinkaurelius.thrift.util.TBinaryProtocol.Factory(true, true);

            TDisruptorServer.Args serverArgs = new TDisruptorServer.Args(serverTransport).useHeapBasedAllocation(true)
                                                                                         .inputTransportFactory(args.inTransportFactory)
                                                                                         .outputTransportFactory(args.outTransportFactory)
                                                                                         .inputProtocolFactory(protocolFactory)
                                                                                         .outputProtocolFactory(protocolFactory)
                                                                                         .processor(args.processor)
                                                                                         .maxFrameSizeInBytes(DatabaseDescriptor.getThriftFramedTransportSize())
                                                                                         .invocationExecutor(invoker)
                                                                                         .alwaysReallocateBuffers(true);

            return new THsHaDisruptorServer(serverArgs);
        }
    }
}
