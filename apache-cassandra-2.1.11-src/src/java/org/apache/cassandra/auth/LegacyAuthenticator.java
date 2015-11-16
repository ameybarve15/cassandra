

/**
 * Provides a transitional IAuthenticator implementation for old-style (pre-1.2) authenticators.
 *
 * Comes with default implementation for the all of the new methods.
 * Subclass LegacyAuthenticator instead of implementing the old IAuthenticator and your old IAuthenticator
 * implementation should continue to work.
 */
public abstract class LegacyAuthenticator implements IAuthenticator
{
    /**
     * @return The user that a connection is initialized with, or 'null' if a user must call login().
     */
    public abstract AuthenticatedUser defaultUser();

    /**
     * @param credentials An implementation specific collection of identifying information.
     * @return A successfully authenticated user: should throw AuthenticationException rather than ever returning null.
     */
    public abstract AuthenticatedUser authenticate(Map<String, String> credentials) throws AuthenticationException;

    public abstract void validateConfiguration() throws ConfigurationException;

    @Override
    public boolean requireAuthentication()
    {
        return defaultUser() == null;
    }

    @Override
    public Set<Option> supportedOptions()
    {
        return Collections.emptySet();
    }

    @Override
    public Set<Option> alterableOptions()
    {
        return Collections.emptySet();
    }

    @Override
    public void create(String username, Map<Option, Object> options) throws RequestValidationException, RequestExecutionException
    {
    }

    @Override
    public void alter(String username, Map<Option, Object> options) throws RequestValidationException, RequestExecutionException
    {
    }

    @Override
    public void drop(String username) throws RequestValidationException, RequestExecutionException
    {
    }

    @Override
    public Set<IResource> protectedResources()
    {
        return Collections.emptySet();
    }

    @Override
    public void setup()
    {
    }
}
