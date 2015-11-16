

public class CustomTNonBlockingServer extends TNonblockingServer
{
    public CustomTNonBlockingServer(Args args)
    {
        super(args);
    }

    @Override
    protected boolean requestInvoke(FrameBuffer frameBuffer)
    {
        TNonblockingSocket socket = (TNonblockingSocket)((CustomFrameBuffer)frameBuffer).getTransport();
        ThriftSessionManager.instance.setCurrentSocket(socket.getSocketChannel().socket().getRemoteSocketAddress());
        frameBuffer.invoke();
        return true;
    }

    public static class Factory implements TServerFactory
    {
        public TServer buildTServer(Args args)
        {
            if (DatabaseDescriptor.getClientEncryptionOptions().enabled)
                throw new RuntimeException("Client SSL is not supported for non-blocking sockets. Please remove client ssl from the configuration.");

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

            // This is single threaded hence the invocation will be all
            // in one thread.
            TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport).inputTransportFactory(args.inTransportFactory)
                                                                                             .outputTransportFactory(args.outTransportFactory)
                                                                                             .inputProtocolFactory(args.tProtocolFactory)
                                                                                             .outputProtocolFactory(args.tProtocolFactory)
                                                                                             .processor(args.processor);
            return new CustomTNonBlockingServer(serverArgs);
        }
    }

    public class CustomFrameBuffer extends FrameBuffer
    {
        public CustomFrameBuffer(final TNonblockingTransport trans,
          final SelectionKey selectionKey,
          final AbstractSelectThread selectThread) {
			super(trans, selectionKey, selectThread);
        }

        public TNonblockingTransport getTransport() {
            return this.trans_;
        }
    }
}
