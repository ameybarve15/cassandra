
/**
 * Transport factory for establishing thrift connections from clients to a remote server.
 */
public interface ITransportFactory
{
    static final String PROPERTY_KEY = "cassandra.client.transport.factory";

    /**
     * Opens a client transport to a thrift server.
     * Example:
     *
     * <pre>
     * TTransport transport = clientTransportFactory.openTransport(address, port);
     * Cassandra.Iface client = new Cassandra.Client(new BinaryProtocol(transport));
     * </pre>
     *
     * @param host fully qualified hostname of the server
     * @param port RPC port of the server
     * @return open and ready to use transport
     * @throws Exception implementation defined; usually throws TTransportException or IOException
     *         if the connection cannot be established
     */
    TTransport openTransport(String host, int port) throws Exception;

    /**
     * Sets an implementation defined set of options.
     * Keys in this map must conform to the set set returned by ITransportFactory#supportedOptions.
     * @param options option map
     */
    void setOptions(Map<String, String> options);

    /**
     * @return set of options supported by this transport factory implementation
     */
    Set<String> supportedOptions();
}

