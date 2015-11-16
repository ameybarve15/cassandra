

public abstract class AbstractRowResolver implements IResponseResolver<ReadResponse, Row>
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractRowResolver.class);

    protected final String keyspaceName;
    // CLQ gives us thread-safety without the overhead of guaranteeing uniqueness like a Set would
    protected final Queue<MessageIn<ReadResponse>> replies = new ConcurrentLinkedQueue<>();
    protected final DecoratedKey key;

    public AbstractRowResolver(ByteBuffer key, String keyspaceName)
    {
        this.key = StorageService.getPartitioner().decorateKey(key);
        this.keyspaceName = keyspaceName;
    }

    public void preprocess(MessageIn<ReadResponse> message)
    {
        replies.add(message);
    }

    public Iterable<MessageIn<ReadResponse>> getMessages()
    {
        return replies;
    }
}
