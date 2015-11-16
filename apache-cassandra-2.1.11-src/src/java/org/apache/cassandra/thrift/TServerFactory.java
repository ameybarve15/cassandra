

public interface TServerFactory
{
    TServer buildTServer(Args args);

    public static class Args
    {
        public InetSocketAddress addr;
        public Integer listenBacklog;
        public TProcessor processor;
        public TProtocolFactory tProtocolFactory;
        public TTransportFactory inTransportFactory;
        public TTransportFactory outTransportFactory;
        public Integer sendBufferSize;
        public Integer recvBufferSize;
        public boolean keepAlive;
    }
}
