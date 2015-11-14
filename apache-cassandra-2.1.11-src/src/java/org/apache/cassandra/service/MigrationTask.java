


class MigrationTask extends WrappedRunnable
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationTask.class);

    private final InetAddress endpoint;

    MigrationTask(InetAddress endpoint)
    {
        this.endpoint = endpoint;
    }

    public void runMayThrow() throws Exception
    {
        // There is a chance that quite some time could have passed between now and the MM#maybeScheduleSchemaPull(),
        // potentially enough for the endpoint node to restart - which is an issue if it does restart upgraded, with
        // a higher major.
        if (!MigrationManager.shouldPullSchemaFrom(endpoint))
        {
            logger.info("Skipped sending a migration request: node {} has a higher major version now.", endpoint);
            return;
        }

        if (!FailureDetector.instance.isAlive(endpoint))
        {
            logger.debug("Can't send schema pull request: node {} is down.", endpoint);
            return;
        }

        MessageOut message = new MessageOut<>(MessagingService.Verb.MIGRATION_REQUEST, null, MigrationManager.MigrationsSerializer.instance);

        IAsyncCallback<Collection<Mutation>> cb = new IAsyncCallback<Collection<Mutation>>()
        {
            @Override
            public void response(MessageIn<Collection<Mutation>> message)
            {
                try
                {
                    DefsTables.mergeSchema(message.payload);
                }
                catch (IOException e)
                {
                    logger.error("IOException merging remote schema", e);
                }
                catch (ConfigurationException e)
                {
                    logger.error("Configuration exception merging remote schema", e);
                }
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }
        };
        MessagingService.instance().sendRR(message, endpoint, cb);
    }
}
