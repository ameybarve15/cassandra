

/**
 * An embedded, in-memory cassandra storage service that listens
 * on the thrift interface as configured in cassandra.yaml
 * This kind of service is useful when running unit tests of
 * services using cassandra for example.
 *
 * See {@link org.apache.cassandra.service.EmbeddedCassandraServiceTest} for usage.
 * <p>
 * This is the implementation of https://issues.apache.org/jira/browse/CASSANDRA-740
 * <p>
 * How to use:
 * In the client code simply create a new EmbeddedCassandraService and start it.
 * Example:
 * <pre>

        cassandra = new EmbeddedCassandraService();
        cassandra.start();

 * </pre>
 <pre>
 this is highlight
 </pre>
 */
public class EmbeddedCassandraService
{

    CassandraDaemon cassandraDaemon;

    public void start() throws IOException
    {
        cassandraDaemon = new CassandraDaemon();
        cassandraDaemon.init(null);
        cassandraDaemon.start();
    }
}
