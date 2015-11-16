

/**
 *  Sets of instances of this class are returned by IAuthorizer.listPermissions() method for LIST PERMISSIONS query.
 *  None of the fields are nullable.
 */
public class PermissionDetails implements Comparable<PermissionDetails>
{
    public final String username;
    public final IResource resource;
    public final Permission permission;
}
