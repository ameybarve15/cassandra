

public class AllowAllAuthorizer implements IAuthorizer
{
    public Set<Permission> authorize(AuthenticatedUser user, IResource resource)
    {
        return Permission.ALL;
    }

    public void grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, String to)
    throws InvalidRequestException
    {
        throw new InvalidRequestException("GRANT operation is not supported by AllowAllAuthorizer");
    }

    public void revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, String from)
    throws InvalidRequestException
    {
        throw new InvalidRequestException("REVOKE operation is not supported by AllowAllAuthorizer");
    }

    public void revokeAll(String droppedUser)
    {
    }

    public void revokeAll(IResource droppedResource)
    {
    }

    public Set<PermissionDetails> list(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, String of)
    throws InvalidRequestException
    {
        throw new InvalidRequestException("LIST PERMISSIONS operation is not supported by AllowAllAuthorizer");
    }

    public Set<IResource> protectedResources()
    {
        return Collections.emptySet();
    }

    public void validateConfiguration()
    {
    }

    public void setup()
    {
    }
}
