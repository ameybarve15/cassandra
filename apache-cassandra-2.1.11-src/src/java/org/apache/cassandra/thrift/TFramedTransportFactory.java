

public class TFramedTransportFactory implements ITransportFactory
{
    private static final String THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB = "cassandra.thrift.framed.size_mb";
    private int thriftFramedTransportSizeMb = 15; // 15Mb is the default for C* & Hadoop ConfigHelper

    public TTransport openTransport(String host, int port) 
    {
        TSocket socket = new TSocket(host, port);
        TTransport transport = new TFramedTransport(socket, thriftFramedTransportSizeMb * 1024 * 1024);
        transport.open();

        return transport;
    }

    public void setOptions(Map<String, String> options)
    {
        if (options.containsKey(THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB))
            thriftFramedTransportSizeMb = Integer.parseInt(options.get(THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB));
    }

    public Set<String> supportedOptions()
    {
        return Collections.singleton(THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB);
    }
}
