

public class AllowAllInternodeAuthenticator implements IInternodeAuthenticator
{
    public boolean authenticate(InetAddress remoteAddress, int remotePort)
    {
        return true;
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }
}
