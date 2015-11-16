

/**
 * Helper implementation to create a thrift TServer based on one of the common types we support (sync, hsha),
 * or a custom type by setting the fully qualified java class name in the rpc_server_type setting.
 */
public class TServerCustomFactory implements TServerFactory
{
    private final String serverType;

    public TServerCustomFactory(String serverType)
    {
        assert serverType != null;
        this.serverType = serverType;
    }

    public TServer buildTServer(TServerFactory.Args args)
    {
        TServer server;
        switch(serverType)
        {
            case SYNC:
                server = new CustomTThreadPoolServer.Factory().buildTServer(args); break;
            case ASYNC:
                server = new CustomTNonBlockingServer.Factory().buildTServer(args); break;
            case HSHA:
                server = new THsHaDisruptorServer.Factory().buildTServer(args); break;
            default:
                TServerFactory serverFactory = (TServerFactory) Class.forName(serverType).newInstance();
                server = serverFactory.buildTServer(args);
        }

        return server;
    }
}
