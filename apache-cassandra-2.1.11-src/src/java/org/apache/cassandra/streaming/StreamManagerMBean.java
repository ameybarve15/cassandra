

public interface StreamManagerMBean extends NotificationEmitter
{
    public static final String OBJECT_NAME = "org.apache.cassandra.net:type=StreamManager";

    /**
     * Returns the current state of all ongoing streams.
     */
    Set<CompositeData> getCurrentStreams();
}
