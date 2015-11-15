


/**
 * Slightly modified version of the Apache Thrift TThreadPoolServer.
 * <p/>
 * This allows passing an executor so you have more control over the actual
 * behaviour of the tasks being run.
 * <p/>
 * Newer version of Thrift should make this obsolete.
 */
public class CustomTThreadPoolServer extends TServer
{

    private static final Logger logger = LoggerFactory.getLogger(CustomTThreadPoolServer.class.getName());

    // Executor service for handling client connections
    private final ExecutorService executorService;

    // Flag for stopping the server
    private volatile boolean stopped;

    // Server options
    private final TThreadPoolServer.Args args;

    //Track and Limit the number of connected clients
    private final AtomicInteger activeClients = new AtomicInteger(0);


    public CustomTThreadPoolServer(TThreadPoolServer.Args args, ExecutorService executorService) {
        super(args);
        this.executorService = executorService;
        this.args = args;
    }

    public void serve()
    {
        try
        {
            serverTransport_.listen();
        }
        catch (TTransportException ttx)
        {
            logger.error("Error occurred during listening.", ttx);
            return;
        }

        stopped = false;
        while (!stopped)
        {
            // block until we are under max clients
            while (activeClients.get() >= args.maxWorkerThreads)
            {
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            }

            try
            {
                TTransport client = serverTransport_.accept();
                activeClients.incrementAndGet();
                WorkerProcess wp = new WorkerProcess(client);
                executorService.execute(wp);
            }
            catch (TTransportException ttx)
            {
                if (ttx.getCause() instanceof SocketTimeoutException) // thrift sucks
                    continue;

                if (!stopped)
                {
                    logger.warn("Transport error occurred during acceptance of message.", ttx);
                }
            }
            catch (RejectedExecutionException e)
            {
                // worker thread decremented activeClients but hadn't finished exiting
                logger.debug("Dropping client connection because our limit of {} has been reached", args.maxWorkerThreads);
                continue;
            }

            if (activeClients.get() >= args.maxWorkerThreads)
                logger.warn("Maximum number of clients {} reached", args.maxWorkerThreads);
        }

        executorService.shutdown();
        // Thrift's default shutdown waits for the WorkerProcess threads to complete.  We do not,
        // because doing that allows a client to hold our shutdown "hostage" by simply not sending
        // another message after stop is called (since process will block indefinitely trying to read
        // the next meessage header).
        //
        // The "right" fix would be to update thrift to set a socket timeout on client connections
        // (and tolerate unintentional timeouts until stopped is set).  But this requires deep
        // changes to the code generator, so simply setting these threads to daemon (in our custom
        // CleaningThreadPool) and ignoring them after shutdown is good enough.
        //
        // Remember, our goal on shutdown is not necessarily that each client request we receive
        // gets answered first [to do that, you should redirect clients to a different coordinator
        // first], but rather (1) to make sure that for each update we ack as successful, we generate
        // hints for any non-responsive replicas, and (2) to make sure that we quickly stop
        // accepting client connections so shutdown can continue.  Not waiting for the WorkerProcess
        // threads here accomplishes (2); MessagingService's shutdown method takes care of (1).
        //
        // See CASSANDRA-3335 and CASSANDRA-3727.
    }

    public void stop()
    {
        stopped = true;
        serverTransport_.interrupt();
    }

    private class WorkerProcess implements Runnable
    {

        /**
         * Client that this services.
         */
        private TTransport client_;

        /**
         * Default constructor.
         *
         * @param client Transport to process
         */
        private WorkerProcess(TTransport client)
        {
            client_ = client;
        }

        /**
         * Loops on processing a client forever
         */
        public void run()
        {
            TProcessor processor = null;
            TTransport inputTransport = null;
            TTransport outputTransport = null;
            TProtocol inputProtocol = null;
            TProtocol outputProtocol = null;
            SocketAddress socket = null;
            try
            {
                socket = ((TCustomSocket) client_).getSocket().getRemoteSocketAddress();
                ThriftSessionManager.instance.setCurrentSocket(socket);
                processor = processorFactory_.getProcessor(client_);
                inputTransport = inputTransportFactory_.getTransport(client_);
                outputTransport = outputTransportFactory_.getTransport(client_);
                inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
                outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);
                // we check stopped first to make sure we're not supposed to be shutting
                // down. this is necessary for graceful shutdown.  (but not sufficient,
                // since process() can take arbitrarily long waiting for client input.
                // See comments at the end of serve().)
                while (!stopped && processor.process(inputProtocol, outputProtocol))
                {
                    inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
                    outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);
                }
            }
            finally
            {
                ThriftSessionManager.instance.connectionComplete(socket);

                inputTransport.close();
                outputTransport.close();
                activeClients.decrementAndGet();
            }
        }
    }

    public static class Factory implements TServerFactory
    {
        public TServer buildTServer(Args args)
        {
            final InetSocketAddress addr = args.addr;
            TServerTransport serverTransport;
            try
            {
                final ClientEncryptionOptions clientEnc = DatabaseDescriptor.getClientEncryptionOptions();
                if (clientEnc.enabled)
                {
                    logger.info("enabling encrypted thrift connections between client and server");
                    TSSLTransportParameters params = new TSSLTransportParameters(clientEnc.protocol, clientEnc.cipher_suites);
                    params.setKeyStore(clientEnc.keystore, clientEnc.keystore_password);
                    if (clientEnc.require_client_auth)
                    {
                        params.setTrustStore(clientEnc.truststore, clientEnc.truststore_password);
                        params.requireClientAuth(true);
                    }
                    TServerSocket sslServer = TSSLTransportFactory.getServerSocket(addr.getPort(), 0, addr.getAddress(), params);
                    SSLServerSocket sslServerSocket = (SSLServerSocket) sslServer.getServerSocket();
                    sslServerSocket.setEnabledProtocols(SSLFactory.ACCEPTED_PROTOCOLS);
                    serverTransport = new TCustomServerSocket(sslServer.getServerSocket(), args.keepAlive, args.sendBufferSize, args.recvBufferSize);
                }
                else
                {
                    serverTransport = new TCustomServerSocket(addr, args.keepAlive, args.sendBufferSize, args.recvBufferSize, args.listenBacklog);
                }
            }
 
            // ThreadPool Server and will be invocation per connection basis...
            TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport)
                                                                     .minWorkerThreads(DatabaseDescriptor.getRpcMinThreads())
                                                                     .maxWorkerThreads(DatabaseDescriptor.getRpcMaxThreads())
                                                                     .inputTransportFactory(args.inTransportFactory)
                                                                     .outputTransportFactory(args.outTransportFactory)
                                                                     .inputProtocolFactory(args.tProtocolFactory)
                                                                     .outputProtocolFactory(args.tProtocolFactory)
                                                                     .processor(args.processor);
            ExecutorService executorService = new ThreadPoolExecutor(serverArgs.minWorkerThreads,
                                                                     serverArgs.maxWorkerThreads,
                                                                     60,
                                                                     TimeUnit.SECONDS,
                                                                     new SynchronousQueue<Runnable>(),
                                                                     new NamedThreadFactory("Thrift"));
            return new CustomTThreadPoolServer(serverArgs, executorService);
        }
    }
}
