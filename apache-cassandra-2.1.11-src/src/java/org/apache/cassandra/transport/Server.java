

public class Server implements CassandraDaemon.Server
{
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private static final boolean enableEpoll = Boolean.valueOf(System.getProperty("cassandra.native.epoll.enabled", "true"));

    public static final int VERSION_3 = 3;
    public static final int CURRENT_VERSION = VERSION_3;

    private final ConnectionTracker connectionTracker = new ConnectionTracker();

    private final Connection.Factory connectionFactory = new Connection.Factory()
    {
        public Connection newConnection(Channel channel, int version)
        {
            return new ServerConnection(channel, version, connectionTracker);
        }
    };

    public final InetSocketAddress socket;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private EventLoopGroup workerGroup;
    private EventExecutor eventExecutorGroup;

    public Server(InetSocketAddress socket)
    {
        this.socket = socket;
        EventNotifier notifier = new EventNotifier(this);
        StorageService.instance.register(notifier);
        MigrationManager.instance.register(notifier);
        registerMetrics();
    }

    public Server(String hostname, int port)
    {
        this(new InetSocketAddress(hostname, port));
    }

    public Server(InetAddress host, int port)
    {
        this(new InetSocketAddress(host, port));
    }

    public Server(int port)
    {
        this(new InetSocketAddress(port));
    }

    public void start()
    {
	    if(!isRunning())
	    {
            run();
	    }
    }

    public void stop()
    {
        if (isRunning.compareAndSet(true, false))
            close();
    }

    public boolean isRunning()
    {
        return isRunning.get();
    }

    private void run()
    {
        // Check that a SaslAuthenticator can be provided by the configured
        // IAuthenticator. If not, don't start the server.
        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        if (authenticator.requireAuthentication() && !(authenticator instanceof ISaslAwareAuthenticator))
        {
            logger.error("Not starting native transport as the configured IAuthenticator is not capable of SASL authentication");
            isRunning.compareAndSet(true, false);
            return;
        }

        // Configure the server.
        eventExecutorGroup = new RequestThreadPoolExecutor();


        boolean hasEpoll = enableEpoll ? Epoll.isAvailable() : false;
        if (hasEpoll)
        {
            workerGroup = new EpollEventLoopGroup();
            logger.info("Netty using native Epoll event loop");
        }
        else
        {
            workerGroup = new NioEventLoopGroup();
            logger.info("Netty using Java NIO event loop");
        }

        ServerBootstrap bootstrap = new ServerBootstrap()
                                    .group(workerGroup)
                                    .channel(hasEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                                    .childOption(ChannelOption.TCP_NODELAY, true)
                                    .childOption(ChannelOption.SO_LINGER, 0)
                                    .childOption(ChannelOption.SO_KEEPALIVE, DatabaseDescriptor.getRpcKeepAlive())
                                    .childOption(ChannelOption.ALLOCATOR, CBUtil.allocator)
                                    .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
                                    .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);

        final EncryptionOptions.ClientEncryptionOptions clientEnc = DatabaseDescriptor.getClientEncryptionOptions();
        if (clientEnc.enabled)
        {
            logger.info("Enabling encrypted CQL connections between client and server");
            bootstrap.childHandler(new SecureInitializer(this, clientEnc));
        }
        else
        {
            bootstrap.childHandler(new Initializer(this));
        }

        // Bind and start to accept incoming connections.
        logger.info("Using Netty Version: {}", Version.identify().entrySet());
        logger.info("Starting listening for CQL clients on {}...", socket);

        ChannelFuture bindFuture = bootstrap.bind(socket);
        if (!bindFuture.awaitUninterruptibly().isSuccess())
            throw new IllegalStateException(String.format("Failed to bind port %d on %s.", socket.getPort(), socket.getAddress().getHostAddress()));

        connectionTracker.allChannels.add(bindFuture.channel());
        isRunning.set(true);
    }

    private void registerMetrics()
    {
        ClientMetrics.instance.addCounter("connectedNativeClients", new Callable<Integer>()
        {
            @Override
            public Integer call() throws Exception
            {
                return connectionTracker.getConnectedClients();
            }
        });
    }

    private void close()
    {
        // Close opened connections
        connectionTracker.closeAll();
        workerGroup.shutdownGracefully();
        workerGroup = null;

        eventExecutorGroup.shutdown();
        eventExecutorGroup = null;
        logger.info("Stop listening for CQL clients");
    }


    public static class ConnectionTracker implements Connection.Tracker
    {
        // TODO: should we be using the GlobalEventExecutor or defining our own?
        public final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        private final EnumMap<Event.Type, ChannelGroup> groups = new EnumMap<Event.Type, ChannelGroup>(Event.Type.class);

        public ConnectionTracker()
        {
            for (Event.Type type : Event.Type.values())
                groups.put(type, new DefaultChannelGroup(type.toString(), GlobalEventExecutor.INSTANCE));
        }

        public void addConnection(Channel ch, Connection connection)
        {
            allChannels.add(ch);
        }

        public void register(Event.Type type, Channel ch)
        {
            groups.get(type).add(ch);
        }

        public void unregister(Channel ch)
        {
            for (ChannelGroup group : groups.values())
                group.remove(ch);
        }

        public void send(Event event)
        {
            groups.get(event.type).writeAndFlush(new EventMessage(event));
        }

        public void closeAll()
        {
            allChannels.close().awaitUninterruptibly();
        }

        public int getConnectedClients()
        {
            /*
              - When server is running: allChannels contains all clients' connections (channels) 
                plus one additional channel used for the server's own bootstrap.
               - When server is stopped: the size is 0
            */
            return allChannels.size() != 0 ? allChannels.size() - 1 : 0;
        }
    }

    private static class Initializer extends ChannelInitializer
    {
        // Stateless handlers
        private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
        private static final Message.ProtocolEncoder messageEncoder = new Message.ProtocolEncoder();
        private static final Frame.Decompressor frameDecompressor = new Frame.Decompressor();
        private static final Frame.Compressor frameCompressor = new Frame.Compressor();
        private static final Frame.Encoder frameEncoder = new Frame.Encoder();
        private static final Message.Dispatcher dispatcher = new Message.Dispatcher();
        private static final ConnectionLimitHandler connectionLimitHandler = new ConnectionLimitHandler();

        private final Server server;

        public Initializer(Server server)
        {
            this.server = server;
        }

        protected void initChannel(Channel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            // Add the ConnectionLimitHandler to the pipeline if configured to do so.
            if (DatabaseDescriptor.getNativeTransportMaxConcurrentConnections() > 0
                    || DatabaseDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp() > 0)
            {
                // Add as first to the pipeline so the limit is enforced as first action.
                pipeline.addFirst("connectionLimitHandler", connectionLimitHandler);
            }

            //pipeline.addLast("debug", new LoggingHandler());

            pipeline.addLast("frameDecoder", new Frame.Decoder(server.connectionFactory));
            pipeline.addLast("frameEncoder", frameEncoder);

            pipeline.addLast("frameDecompressor", frameDecompressor);
            pipeline.addLast("frameCompressor", frameCompressor);

            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", messageEncoder);

            pipeline.addLast(server.eventExecutorGroup, "executor", dispatcher);
        }
    }

    private static class SecureInitializer extends Initializer
    {
        private final SSLContext sslContext;
        private final EncryptionOptions encryptionOptions;

        public SecureInitializer(Server server, EncryptionOptions encryptionOptions)
        {
            super(server);
            this.encryptionOptions = encryptionOptions;
            try
            {
                this.sslContext = SSLFactory.createSSLContext(encryptionOptions, encryptionOptions.require_client_auth);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Failed to setup secure pipeline", e);
            }
        }

        protected void initChannel(Channel channel) throws Exception
        {
            SSLEngine sslEngine = sslContext.createSSLEngine();
            sslEngine.setUseClientMode(false);
            sslEngine.setEnabledCipherSuites(encryptionOptions.cipher_suites);
            sslEngine.setNeedClientAuth(encryptionOptions.require_client_auth);
            sslEngine.setEnabledProtocols(SSLFactory.ACCEPTED_PROTOCOLS);
            SslHandler sslHandler = new SslHandler(sslEngine);
            super.initChannel(channel);
            channel.pipeline().addFirst("ssl", sslHandler);
        }
    }

    private static class EventNotifier extends MigrationListener implements IEndpointLifecycleSubscriber
    {
        private final Server server;
        private final Map<InetAddress, Event.StatusChange.Status> lastStatusChange = new ConcurrentHashMap<>();
        private static final InetAddress bindAll;
        static {
            try
            {
                bindAll = InetAddress.getByAddress(new byte[4]);
            }
            catch (UnknownHostException e)
            {
                throw new AssertionError(e);
            }
        }

        private EventNotifier(Server server)
        {
            this.server = server;
        }

        private InetAddress getRpcAddress(InetAddress endpoint)
        {
            try
            {
                InetAddress rpcAddress = InetAddress.getByName(StorageService.instance.getRpcaddress(endpoint));
                // If rpcAddress == 0.0.0.0 (i.e. bound on all addresses), returning that is not very helpful,
                // so return the internal address (which is ok since "we're bound on all addresses").
                // Note that after all nodes are running a version that includes CASSANDRA-5899, rpcAddress should
                // never be 0.0.0.0, so this can eventually be removed.
                return rpcAddress.equals(bindAll) ? endpoint : rpcAddress;
            }
            catch (UnknownHostException e)
            {
                // That should not happen, so log an error, but return the
                // endpoint address since there's a good change this is right
                logger.error("Problem retrieving RPC address for {}", endpoint, e);
                return endpoint;
            }
        }

        private void send(InetAddress endpoint, Event.NodeEvent event)
        {
            // If the endpoint is not the local node, extract the node address
            // and if it is the same as our own RPC broadcast address (which defaults to the rcp address)
            // then don't send the notification. This covers the case of rpc_address set to "localhost",
            // which is not useful to any driver and in fact may cauase serious problems to some drivers,
            // see CASSANDRA-10052
            if (!endpoint.equals(FBUtilities.getBroadcastAddress()) &&
                event.nodeAddress().equals(DatabaseDescriptor.getBroadcastRpcAddress()))
                return;

            send(event);
        }

        private void send(Event event)
        {
            server.connectionTracker.send(event);
        }

        public void onJoinCluster(InetAddress endpoint)
        {
            send(endpoint, Event.TopologyChange.newNode(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onLeaveCluster(InetAddress endpoint)
        {
            send(endpoint, Event.TopologyChange.removedNode(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onMove(InetAddress endpoint)
        {
            send(endpoint, Event.TopologyChange.movedNode(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onUp(InetAddress endpoint)
        {
            Event.StatusChange.Status prev = lastStatusChange.put(endpoint, Event.StatusChange.Status.UP);
            if (prev == null || prev != Event.StatusChange.Status.UP)
                send(endpoint, Event.StatusChange.nodeUp(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onDown(InetAddress endpoint)
        {
            Event.StatusChange.Status prev = lastStatusChange.put(endpoint, Event.StatusChange.Status.DOWN);
            if (prev == null || prev != Event.StatusChange.Status.DOWN)
                send(endpoint, Event.StatusChange.nodeDown(getRpcAddress(endpoint), server.socket.getPort()));
        }

        public void onCreateKeyspace(String ksName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, ksName));
        }

        public void onCreateColumnFamily(String ksName, String cfName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, ksName, cfName));
        }

        public void onCreateUserType(String ksName, String typeName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TYPE, ksName, typeName));
        }

        public void onUpdateKeyspace(String ksName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, ksName));
        }

        public void onUpdateColumnFamily(String ksName, String cfName, boolean columnsDidChange)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, ksName, cfName));
        }

        public void onUpdateUserType(String ksName, String typeName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TYPE, ksName, typeName));
        }

        public void onDropKeyspace(String ksName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, ksName));
        }

        public void onDropColumnFamily(String ksName, String cfName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TABLE, ksName, cfName));
        }

        public void onDropUserType(String ksName, String typeName)
        {
            send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TYPE, ksName, typeName));
        }
    }
}
