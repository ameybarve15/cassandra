

public class AllowAllAuthenticator implements IAuthenticator
{
    public boolean requireAuthentication()
    {
        return false;
    }

    public Set<Option> supportedOptions()
    {
        return Collections.emptySet();
    }

    public Set<Option> alterableOptions()
    {
        return Collections.emptySet();
    }

    public AuthenticatedUser authenticate(Map<String, String> credentials) throws AuthenticationException
    {
        return AuthenticatedUser.ANONYMOUS_USER;
    }

    public void create(String username, Map<Option, Object> options) throws InvalidRequestException
    {
        throw new InvalidRequestException("CREATE USER operation is not supported by AllowAllAuthenticator");
    }

    public void alter(String username, Map<Option, Object> options) throws InvalidRequestException
    {
        throw new InvalidRequestException("ALTER USER operation is not supported by AllowAllAuthenticator");
    }

    public void drop(String username) throws InvalidRequestException
    {
        throw new InvalidRequestException("DROP USER operation is not supported by AllowAllAuthenticator");
    }

    public Set<IResource> protectedResources()
    {
        return Collections.emptySet();
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
    }
}
