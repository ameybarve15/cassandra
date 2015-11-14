
/** A <code>CREATE KEYSPACE</code> statement parsed from a CQL query. */
public class CreateKeyspaceStatement
{
    private final String name;
    private final Map<String, String> attrs;
    private String strategyClass;
    private final Map<String, String> strategyOptions = new HashMap<String, String>();


    /**
     * The <code>CqlParser</code> only goes as far as extracting the keyword arguments
     * from these statements, so this method is responsible for processing and
     * validating, and must be called prior to access.
     *
     * @throws InvalidRequestException if arguments are missing or unacceptable
     */
    public void validate() throws InvalidRequestException
    {
        // required
        if (!attrs.containsKey("strategy_class"))
            throw new InvalidRequestException("missing required argument \"strategy_class\"");
        strategyClass = attrs.get("strategy_class");

        // optional
        for (String key : attrs.keySet())
            if ((key.contains(":")) && (key.startsWith("strategy_options")))
                strategyOptions.put(key.split(":")[1], attrs.get(key));
    }
}
