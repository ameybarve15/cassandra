

public class BootStrapper
{
    private static final Logger logger = LoggerFactory.getLogger(BootStrapper.class);

    /* endpoint that needs to be bootstrapped */
    protected final InetAddress address;
    /* token of the node being bootstrapped. */
    protected final Collection<Token> tokens;
    protected final TokenMetadata tokenMetadata;

    public BootStrapper(InetAddress address, Collection<Token> tokens, TokenMetadata tmd)
    {
        assert address != null;
        assert tokens != null && !tokens.isEmpty();

        this.address = address;
        this.tokens = tokens;
        tokenMetadata = tmd;
    }

    public void bootstrap()
    {
        if (logger.isDebugEnabled())
            logger.debug("Beginning bootstrap process");

        RangeStreamer streamer = new RangeStreamer(tokenMetadata, tokens, address, "Bootstrap");
        streamer.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(FailureDetector.instance));

        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            AbstractReplicationStrategy strategy = Keyspace.open(keyspaceName).getReplicationStrategy();
            streamer.addRanges(keyspaceName, strategy.getPendingAddressRanges(tokenMetadata, tokens, address));
        }

        try
        {
            streamer.fetchAsync().get();
            StorageService.instance.finishBootstrapping();
        }
    }

    /**
     * if initialtoken was specified, use that (split on comma).
     * otherwise, if num_tokens == 1, pick a token to assume half the load of the most-loaded node.
     * else choose num_tokens tokens at random
     */
    public static Collection<Token> getBootstrapTokens(final TokenMetadata metadata) throws ConfigurationException
    {
        Collection<String> initialTokens = DatabaseDescriptor.getInitialTokens();
        // if user specified tokens, use those
        if (initialTokens.size() > 0)
        {
            logger.debug("tokens manually specified as {}",  initialTokens);
            List<Token> tokens = new ArrayList<Token>(initialTokens.size());
            for (String tokenString : initialTokens)
            {
                Token token = StorageService.getPartitioner().getTokenFactory().fromString(tokenString);
                if (metadata.getEndpoint(token) != null)
                    throw new ConfigurationException("Bootstrapping to existing token " + tokenString + " is not allowed (decommission/removenode the old node first).");
                tokens.add(token);
            }
            return tokens;
        }

        int numTokens = DatabaseDescriptor.getNumTokens();
        if (numTokens < 1)
            throw new ConfigurationException("num_tokens must be >= 1");

        if (numTokens == 1)
            logger.warn("Picking random token for a single vnode.  You should probably add more vnodes; failing that, you should probably specify the token manually");

        return getRandomTokens(metadata, numTokens);
    }

    public static Collection<Token> getRandomTokens(TokenMetadata metadata, int numTokens)
    {
        Set<Token> tokens = new HashSet<Token>(numTokens);
        while (tokens.size() < numTokens)
        {
            Token token = StorageService.getPartitioner().getRandomToken();
            if (metadata.getEndpoint(token) == null)
                tokens.add(token);
        }
        return tokens;
    }

    public static class StringSerializer implements IVersionedSerializer<String>
    {
        public static final StringSerializer instance = new StringSerializer();

        public void serialize(String s, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(s);
        }

        public String deserialize(DataInput in, int version) throws IOException
        {
            return in.readUTF();
        }

        public long serializedSize(String s, int version)
        {
            return TypeSizes.NATIVE.sizeof(s);
        }
    }
}
