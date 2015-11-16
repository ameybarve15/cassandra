

public class CliCompleter extends SimpleCompletor
{
    private static final String[] commands = {
            "connect",
            "describe keyspace",
            "exit",
            "help",
            "quit",
            "show cluster name",
            "show keyspaces",
            "show schema",
            "show api version",
            "create keyspace",
            "create column family",
            "drop keyspace",
            "drop column family",
            "rename keyspace",
            "rename column family",
            "consistencylevel",

            "help connect",
            "help describe keyspace",
            "help exit",
            "help help",
            "help quit",
            "help show cluster name",
            "help show keyspaces",
            "help show schema",
            "help show api version",
            "help create keyspace",
            "help create column family",
            "help drop keyspace",
            "help drop column family",
            "help rename keyspace",
            "help rename column family",
            "help get",
            "help set",
            "help del",
            "help count",
            "help list",
            "help truncate",
            "help consistencylevel"
    };
    private static final String[] keyspaceCommands = {
            "get",
            "set",
            "count",
            "del",
            "list",
            "truncate",
            "incr",
            "decr"
    };

    public CliCompleter()
    {
        super(commands);
    }

    String[] getKeyspaceCommands()
    {
        return keyspaceCommands;
    }
}
