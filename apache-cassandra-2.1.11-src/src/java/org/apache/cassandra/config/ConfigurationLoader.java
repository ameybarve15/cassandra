
public interface ConfigurationLoader
{
    /**
     * Loads a {@link Config} object to use to configure a node.
     *
     * @return the {@link Config} to use.
     * @throws ConfigurationException if the configuration cannot be properly loaded.
     */
    Config loadConfig() throws ConfigurationException;
}
