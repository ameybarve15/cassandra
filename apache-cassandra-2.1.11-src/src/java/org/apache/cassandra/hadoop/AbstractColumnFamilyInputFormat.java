public abstract class AbstractColumnFamilyInputFormat<K, Y> extends InputFormat<K, Y> implements org.apache.hadoop.mapred.InputFormat<K, Y>
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractColumnFamilyInputFormat.class);

    public static final String MAPRED_TASK_ID = "mapred.task.id";
    // The simple fact that we need this is because the old Hadoop API wants us to "write"
    // to the key and value whereas the new asks for it.
    // I choose 8kb as the default max key size (instanciated only once), but you can
    // override it in your jobConf with this setting.
    public static final String CASSANDRA_HADOOP_MAX_KEY_SIZE = "cassandra.hadoop.max_key_size";
    public static final int    CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT = 8192;

    private String keyspace;
    private String cfName;
    private IPartitioner partitioner;

    protected void validateConfiguration(Configuration conf)
    {
        if (ConfigHelper.getInputKeyspace(conf) == null || ConfigHelper.getInputColumnFamily(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the keyspace and columnfamily with setInputColumnFamily()");
        }
        if (ConfigHelper.getInputInitialAddress(conf) == null)
            throw new UnsupportedOperationException("You must set the initial output address to a Cassandra node with setInputInitialAddress");
        if (ConfigHelper.getInputPartitioner(conf) == null)
            throw new UnsupportedOperationException("You must set the Cassandra partitioner class with setInputPartitioner");
    }

    public static Cassandra.Client createAuthenticatedClient(String location, int port, Configuration conf) throws Exception
    {
        logger.debug("Creating authenticated client for CF input format");
        TTransport transport;
        try
        {
            transport = ConfigHelper.getClientTransportFactory(conf).openTransport(location, port);
        }
        catch (Exception e)
        {
            throw new TTransportException("Failed to open a transport to " + location + ":" + port + ".", e);
        }
        TProtocol binaryProtocol = new TBinaryProtocol(transport, true, true);
        Cassandra.Client client = new Cassandra.Client(binaryProtocol);

        // log in
        client.set_keyspace(ConfigHelper.getInputKeyspace(conf));
        if ((ConfigHelper.getInputKeyspaceUserName(conf) != null) && (ConfigHelper.getInputKeyspacePassword(conf) != null))
        {
            Map<String, String> creds = new HashMap<String, String>();
            creds.put(IAuthenticator.USERNAME_KEY, ConfigHelper.getInputKeyspaceUserName(conf));
            creds.put(IAuthenticator.PASSWORD_KEY, ConfigHelper.getInputKeyspacePassword(conf));
            AuthenticationRequest authRequest = new AuthenticationRequest(creds);
            client.login(authRequest);
        }
        logger.debug("Authenticated client for CF input format created successfully");
        return client;
    }

    public List<InputSplit> getSplits(JobContext context) throws IOException
    {
        Configuration conf = HadoopCompat.getConfiguration(context);;

        validateConfiguration(conf);

        // cannonical ranges and nodes holding replicas
        List<TokenRange> masterRangeNodes = getRangeMap(conf);

        keyspace = ConfigHelper.getInputKeyspace(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        partitioner = ConfigHelper.getInputPartitioner(conf);
        logger.debug("partitioner is {}", partitioner);


        // cannonical ranges, split into pieces, fetching the splits in parallel
        ExecutorService executor = new ThreadPoolExecutor(0, 128, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        List<InputSplit> splits = new ArrayList<InputSplit>();

        try
        {
            List<Future<List<InputSplit>>> splitfutures = new ArrayList<Future<List<InputSplit>>>();
            KeyRange jobKeyRange = ConfigHelper.getInputKeyRange(conf);
            Range<Token> jobRange = null;
            if (jobKeyRange != null)
            {
                if (jobKeyRange.start_key != null)
                {
                    if (!partitioner.preservesOrder())
                        throw new UnsupportedOperationException("KeyRange based on keys can only be used with a order preserving paritioner");
                    if (jobKeyRange.start_token != null)
                        throw new IllegalArgumentException("only start_key supported");
                    if (jobKeyRange.end_token != null)
                        throw new IllegalArgumentException("only start_key supported");
                    jobRange = new Range<>(partitioner.getToken(jobKeyRange.start_key),
                                           partitioner.getToken(jobKeyRange.end_key),
                                           partitioner);
                }
                else if (jobKeyRange.start_token != null)
                {
                    jobRange = new Range<>(partitioner.getTokenFactory().fromString(jobKeyRange.start_token),
                                           partitioner.getTokenFactory().fromString(jobKeyRange.end_token),
                                           partitioner);
                }
                else
                {
                    logger.warn("ignoring jobKeyRange specified without start_key or start_token");
                }
            }

            for (TokenRange range : masterRangeNodes)
            {
                if (jobRange == null)
                {
                    // for each range, pick a live owner and ask it to compute bite-sized splits
                    splitfutures.add(executor.submit(new SplitCallable(range, conf)));
                }
                else
                {
                    Range<Token> dhtRange = new Range<Token>(partitioner.getTokenFactory().fromString(range.start_token),
                                                             partitioner.getTokenFactory().fromString(range.end_token),
                                                             partitioner);

                    if (dhtRange.intersects(jobRange))
                    {
                        for (Range<Token> intersection: dhtRange.intersectionWith(jobRange))
                        {
                            range.start_token = partitioner.getTokenFactory().toString(intersection.left);
                            range.end_token = partitioner.getTokenFactory().toString(intersection.right);
                            // for each range, pick a live owner and ask it to compute bite-sized splits
                            splitfutures.add(executor.submit(new SplitCallable(range, conf)));
                        }
                    }
                }
            }

            // wait until we have all the results back
            for (Future<List<InputSplit>> futureInputSplits : splitfutures)
            {
                try
                {
                    splits.addAll(futureInputSplits.get());
                }
                catch (Exception e)
                {
                    throw new IOException("Could not get input splits", e);
                }
            }
        }
        finally
        {
            executor.shutdownNow();
        }

        assert splits.size() > 0;
        Collections.shuffle(splits, new Random(System.nanoTime()));
        return splits;
    }

    /**
     * Gets a token range and splits it up according to the suggested
     * size into input splits that Hadoop can use.
     */
    class SplitCallable implements Callable<List<InputSplit>>
    {

        private final TokenRange range;
        private final Configuration conf;

        public SplitCallable(TokenRange tr, Configuration conf)
        {
            this.range = tr;
            this.conf = conf;
        }

        public List<InputSplit> call() throws Exception
        {
            ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
            List<CfSplit> subSplits = getSubSplits(keyspace, cfName, range, conf);
            assert range.rpc_endpoints.size() == range.endpoints.size() : "rpc_endpoints size must match endpoints size";
            // turn the sub-ranges into InputSplits
            String[] endpoints = range.endpoints.toArray(new String[range.endpoints.size()]);
            // hadoop needs hostname, not ip
            int endpointIndex = 0;
            for (String endpoint: range.rpc_endpoints)
            {
                String endpoint_address = endpoint;
                if (endpoint_address == null || endpoint_address.equals("0.0.0.0"))
                    endpoint_address = range.endpoints.get(endpointIndex);
                endpoints[endpointIndex++] = InetAddress.getByName(endpoint_address).getHostName();
            }

            Token.TokenFactory factory = partitioner.getTokenFactory();
            for (CfSplit subSplit : subSplits)
            {
                Token left = factory.fromString(subSplit.getStart_token());
                Token right = factory.fromString(subSplit.getEnd_token());
                Range<Token> range = new Range<Token>(left, right, partitioner);
                List<Range<Token>> ranges = range.isWrapAround() ? range.unwrap() : ImmutableList.of(range);
                for (Range<Token> subrange : ranges)
                {
                    ColumnFamilySplit split =
                            new ColumnFamilySplit(
                                    factory.toString(subrange.left),
                                    factory.toString(subrange.right),
                                    subSplit.getRow_count(),
                                    endpoints);

                    logger.debug("adding {}", split);
                    splits.add(split);
                }
            }
            return splits;
        }
    }

    private List<CfSplit> getSubSplits(String keyspace, String cfName, TokenRange range, Configuration conf) throws IOException
    {
        int splitsize = ConfigHelper.getInputSplitSize(conf);
        for (int i = 0; i < range.rpc_endpoints.size(); i++)
        {
            String host = range.rpc_endpoints.get(i);

            if (host == null || host.equals("0.0.0.0"))
                host = range.endpoints.get(i);

            try
            {
                Cassandra.Client client = ConfigHelper.createConnection(conf, host, ConfigHelper.getInputRpcPort(conf));
                client.set_keyspace(keyspace);

                try
                {
                    return client.describe_splits_ex(cfName, range.start_token, range.end_token, splitsize);
                }
                catch (TApplicationException e)
                {
                    // fallback to guessing split size if talking to a server without describe_splits_ex method
                    if (e.getType() == TApplicationException.UNKNOWN_METHOD)
                    {
                        List<String> splitPoints = client.describe_splits(cfName, range.start_token, range.end_token, splitsize);
                        return tokenListToSplits(splitPoints, splitsize);
                    }
                    throw e;
                }
            }
            catch (IOException e)
            {
                logger.debug("failed connect to endpoint {}", host, e);
            }
            catch (InvalidRequestException e)
            {
                throw new RuntimeException(e);
            }
            catch (TException e)
            {
                throw new RuntimeException(e);
            }
        }
        throw new IOException("failed connecting to all endpoints " + StringUtils.join(range.endpoints, ","));
    }

    private List<CfSplit> tokenListToSplits(List<String> splitTokens, int splitsize)
    {
        List<CfSplit> splits = Lists.newArrayListWithExpectedSize(splitTokens.size() - 1);
        for (int j = 0; j < splitTokens.size() - 1; j++)
            splits.add(new CfSplit(splitTokens.get(j), splitTokens.get(j + 1), splitsize));
        return splits;
    }

    private List<TokenRange> getRangeMap(Configuration conf) throws IOException
    {
        Cassandra.Client client = ConfigHelper.getClientFromInputAddressList(conf);

        List<TokenRange> map;
        try
        {
            map = client.describe_local_ring(ConfigHelper.getInputKeyspace(conf));
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        return map;
    }

    //
    // Old Hadoop API
    //
    public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException
    {
        TaskAttemptContext tac = HadoopCompat.newTaskAttemptContext(jobConf, new TaskAttemptID());
        List<org.apache.hadoop.mapreduce.InputSplit> newInputSplits = this.getSplits(tac);
        org.apache.hadoop.mapred.InputSplit[] oldInputSplits = new org.apache.hadoop.mapred.InputSplit[newInputSplits.size()];
        for (int i = 0; i < newInputSplits.size(); i++)
            oldInputSplits[i] = (ColumnFamilySplit)newInputSplits.get(i);
        return oldInputSplits;
    }
}
