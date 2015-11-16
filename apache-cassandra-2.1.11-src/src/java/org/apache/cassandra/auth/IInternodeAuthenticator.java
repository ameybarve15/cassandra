

public interface IInternodeAuthenticator
{
    /**
     * Decides whether or not a peer is allowed to connect to this node.
     * If this method returns false, the socket will be immediately closed.
     *
     * @param remoteAddress ip address of the connecting node.
     * @param remotePort port of the connecting node.
     * @return true if the connection should be accepted, false otherwise.
     */
    boolean authenticate(InetAddress remoteAddress, int remotePort);

    /**
     * Validates configuration of IInternodeAuthenticator implementation (if configurable).
     *
     * @throws ConfigurationException when there is a configuration error.
     */
    void validateConfiguration() throws ConfigurationException;
}
