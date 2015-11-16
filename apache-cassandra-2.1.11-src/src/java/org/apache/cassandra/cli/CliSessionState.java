

/**
 * Used to hold the state for the CLI.
 */
public class CliSessionState
{

    public String  hostName;      // cassandra server name
    public int     thriftPort;    // cassandra server's thrift port
    public boolean debug = false; // print stack traces when errors occur in the CLI
    public String  username;      // cassandra login name (if password-based authenticator is used)
    public String  password;      // cassandra login password (if password-based authenticator is used)
    public String  keyspace;      // cassandra keyspace user is authenticating
    public boolean batch = false; // enable/disable batch processing mode
    public String  filename = ""; // file to read commands from
    public int     jmxPort = 7199;// JMX service port
    public String  jmxUsername;   // JMX service username
    public String  jmxPassword;   // JMX service password
    public boolean verbose = false; // verbose output
    public ITransportFactory transportFactory = new TFramedTransportFactory();
    public EncryptionOptions encOptions = new ClientEncryptionOptions();

    /*
     * Streams to read/write from
     */
    public InputStream in;
    public PrintStream out;
    public PrintStream err;

    public CliSessionState()
    {
        in = System.in;
        out = System.out;
        err = System.err;
    }

    public boolean inFileMode()
    {
        return !this.filename.isEmpty();
    }

    public NodeProbe getNodeProbe()
    {
        try
        {
            return jmxUsername != null && jmxPassword != null
                   ? new NodeProbe(hostName, jmxPort, jmxUsername, jmxPassword)
                   : new NodeProbe(hostName, jmxPort);
        }

        return null;
    }
}
