

/**
 * Cassandra Command Line Interface (CLI) Main
 */
public class CliMain
{
    public final static String OLD_HISTORYFILE = ".cassandra.history";
    public final static String HISTORYFILE = "cli.history";

    private static TTransport transport = null;
    private static Cassandra.Client thriftClient = null;
    public  static final CliSessionState sessionState = new CliSessionState();
    private static CliClient cliClient;
    private static final CliCompleter completer = new CliCompleter();
    private static int lineNumber = 1;

    /**
     * Establish a thrift connection to cassandra instance
     *
     * @param server - hostname or IP of the server
     * @param port   - Thrift port number
     */
    public static void connect(String server, int port)
    {
        transport.close();

        try
        {
            transport = sessionState.transportFactory.openTransport(server, port);
        }

        TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport, true, true);
        thriftClient = new Cassandra.Client(binaryProtocol);
        cliClient = new CliClient(sessionState, thriftClient);

        if ((sessionState.username != null) && (sessionState.password != null))
        {
            // Authenticate
            Map<String, String> credentials = new HashMap<String, String>();
            credentials.put(IAuthenticator.USERNAME_KEY, sessionState.username);
            credentials.put(IAuthenticator.PASSWORD_KEY, sessionState.password);
            AuthenticationRequest authRequest = new AuthenticationRequest(credentials);
            try
            {
                thriftClient.login(authRequest);
                cliClient.setUsername(sessionState.username);
            }
        }

        if (sessionState.keyspace != null)
        {
            try
            {
                sessionState.keyspace = CliCompiler.getKeySpace(sessionState.keyspace, thriftClient.describe_keyspaces());;
                thriftClient.set_keyspace(sessionState.keyspace);
                cliClient.setKeySpace(sessionState.keyspace);
                updateCompletor(CliUtils.getCfNamesByKeySpace(cliClient.getKSMetaData(sessionState.keyspace)));
            }
        }

        // Lookup the cluster name, this is to make it clear which cluster the user is connected to
        String clusterName;

        try
        {
            clusterName = thriftClient.describe_cluster_name();
        }

        sessionState.out.printf("Connected to: \"%s\" on %s/%d%n", clusterName, server, port);
    }

    /**
     * Disconnect thrift connection to cassandra instance
     */
    public static void disconnect()
    {
        if (transport != null)
        {
            transport.close();
            transport = null;
        }
    }

    /**
     * Checks whether the thrift client is connected.
     * @return boolean - true when connected, false otherwise
     */
    public static boolean isConnected()
    {
        return (thriftClient != null);
    }

    public static void updateCompletor(Set<String> candidates)
    {
        Set<String> actions = new HashSet<String>();
        for (String cf : candidates)
        {
            for (String cmd : completer.getKeyspaceCommands())
                actions.add(String.format("%s %s", cmd, cf));
        }

        String[] strs = Arrays.copyOf(actions.toArray(), actions.toArray().length, String[].class);

        completer.setCandidateStrings(strs);
    }

    public static void processStatement(String query) throws 
    {
        cliClient.executeCLIStatement(query);
    }

    public static void processStatementInteractive(String query)
    {
        try
        {
            cliClient.executeCLIStatement(query);
        }
        finally
        {
            lineNumber++;
        }
    }

    public static void main(String args[]) throws IOException
    {
        // process command line arguments
        CliOptions cliOptions = new CliOptions();
        cliOptions.processArgs(sessionState, args);

        // connect to cassandra server if host argument specified.
        if (sessionState.hostName != null)
        {
            try
            {
                connect(sessionState.hostName, sessionState.thriftPort);
            }
        }

        if ( cliClient == null )
        {
            // Connection parameter was either invalid or not present.
            // User must connect explicitly using the "connect" CLI statement.
            cliClient = new CliClient(sessionState, null);
        }

        // load statements from file and process them
        if (sessionState.inFileMode())
        {
            BufferedReader reader = null;

            try
            {
                reader = new BufferedReader(new FileReader(sessionState.filename));
                evaluateFileStatements(reader);
            }
            finally
            {
                FileUtils.closeQuietly(reader);
            }      

            return;
        }

        ConsoleReader reader = new ConsoleReader();

        if (!sessionState.batch)
        {
            reader.addCompletor(completer);
            reader.setBellEnabled(false);
            File historyFile = handleHistoryFiles();

            try
            {
                History history = new History(historyFile);
                reader.setHistory(history);
            }
        }
        else if (!sessionState.verbose) // if in batch mode but no verbose flag
        {
            sessionState.out.close();
        }

        cliClient.printBanner();

        String prompt;
        String line = "";
        String currentStatement = "";
        boolean inCompoundStatement = false;

        while (line != null)
        {
            prompt = (inCompoundStatement) ? "...\t" : getPrompt(cliClient);

            try
            {
                line = reader.readLine(prompt);
            }

            if (line == null)
                return;

            line = line.trim();

            // skipping empty and comment lines
            if (line.isEmpty() || line.startsWith("--"))
                continue;

            currentStatement += line;

            if (line.endsWith(";") || line.equals("?"))
            {
                processStatementInteractive(currentStatement);
                currentStatement = "";
                inCompoundStatement = false;
            }
            else
            {
                currentStatement += " "; // ready for new line
                inCompoundStatement = true;
            }
        }
    }

    private static File handleHistoryFiles()
    {
        File outputDir = FBUtilities.getToolsOutputDirectory();
        File historyFile = new File(outputDir, HISTORYFILE);
        File oldHistoryFile = new File(System.getProperty("user.home"), OLD_HISTORYFILE);
        if(oldHistoryFile.exists())
            FileUtils.renameWithConfirm(oldHistoryFile, historyFile);

        return historyFile;
    }

    private static void evaluateFileStatements(BufferedReader reader) throws IOException
    {
        String line;
        String currentStatement = "";

        boolean commentedBlock = false;

        while ((line = reader.readLine()) != null)
        {
            line = line.trim();

            // skipping empty and comment lines
            if (line.isEmpty() || line.startsWith("--"))
                continue;

            if (line.startsWith("/*"))
                commentedBlock = true;

            if (line.startsWith("*/") || line.endsWith("*/"))
            {
                commentedBlock = false;
                continue;
            }

            if (commentedBlock) // skip commented lines
                continue;

            currentStatement += line;

            if (line.endsWith(";"))
            {
                processStatementInteractive(currentStatement);
                currentStatement = "";
            }
            else
            {
                currentStatement += " "; // ready for new line
            }
        }
    }

    /**
     * Returns prompt for current connection
     * @param client - currently connected client
     * @return String - prompt with username and keyspace (if any)
     */
    private static String getPrompt(CliClient client)
    {
        return "[" + client.getUsername() + "@" + client.getKeySpace() + "] ";
    }

}
