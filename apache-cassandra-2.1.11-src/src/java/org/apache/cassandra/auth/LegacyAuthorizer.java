

/**
 * Provides a transitional IAuthorizer implementation for old-style (pre-1.2) authorizers.
 *
 * Translates old-style authorize() calls to the new-style, expands Permission.READ and Permission.WRITE
 * into the new Permission values, translates the new resource hierarchy into the old hierarchy.
 * Stubs the rest of the new methods.
 * Subclass LegacyAuthorizer instead of implementing the old IAuthority and your old IAuthority implementation should
 * continue to work.
 */
public abstract class LegacyAuthorizer implements IAuthorizer
{
    /**
     * @param user Authenticated user requesting authorization.
     * @param resource List of Objects containing Strings and byte[]s: represents a resource in the old hierarchy.
     * @return Set of permissions of the user on the resource. Should never return null. Use Permission.NONE instead.
     */
    public abstract EnumSet<Permission> authorize(AuthenticatedUser user, List<Object> resource);

    public abstract void validateConfiguration() throws ConfigurationException;

    /**
     * Translates new-style authorize() method call to the old-style (including permissions and the hierarchy).
     */
    @Override
    public Set<Permission> authorize(AuthenticatedUser user, IResource resource)
    {
        if (!(resource instanceof DataResource))
            throw new IllegalArgumentException(String.format("%s resource is not supported by LegacyAuthorizer", resource.getName()));
        DataResource dr = (DataResource) resource;

        List<Object> legacyResource = new ArrayList<Object>();
        legacyResource.add(Resources.ROOT);
        legacyResource.add(Resources.KEYSPACES);
        if (!dr.isRootLevel())
            legacyResource.add(dr.getKeyspace());
        if (dr.isColumnFamilyLevel())
            legacyResource.add(dr.getColumnFamily());

        Set<Permission> permissions = authorize(user, legacyResource);
        if (permissions.contains(Permission.READ))
            permissions.add(Permission.SELECT);
        if (permissions.contains(Permission.WRITE))
            permissions.addAll(EnumSet.of(Permission.CREATE, Permission.ALTER, Permission.DROP, Permission.MODIFY));

        return permissions;
    }

    @Override
    public void grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, String to)
    throws InvalidRequestException
    {
        throw new InvalidRequestException("GRANT operation is not supported by LegacyAuthorizer");
    }

    @Override
    public void revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, String from)
    throws InvalidRequestException
    {
        throw new InvalidRequestException("REVOKE operation is not supported by LegacyAuthorizer");
    }

    @Override
    public void revokeAll(String droppedUser)
    {
    }

    @Override
    public void revokeAll(IResource droppedResource)
    {
    }

    @Override
    public Set<PermissionDetails> list(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, String of)
    throws InvalidRequestException, UnauthorizedException
    {
        throw new InvalidRequestException("LIST PERMISSIONS operation is not supported by LegacyAuthorizer");
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
