

/**
 * An enum encapsulating the set of possible permissions that an authenticated user can have on a resource.
 *
 * IAuthorizer implementations may encode permissions using ordinals, so the Enum order must never change order.
 * Adding new values is ok.
 */
public enum Permission
{
    @Deprecated
    READ,
    @Deprecated
    WRITE,

    // schema management
    CREATE, // required for CREATE KEYSPACE and CREATE TABLE.
    ALTER,  // required for ALTER KEYSPACE, ALTER TABLE, CREATE INDEX, DROP INDEX.
    DROP,   // required for DROP KEYSPACE and DROP TABLE.

    // data access
    SELECT, // required for SELECT.
    MODIFY, // required for INSERT, UPDATE, DELETE, TRUNCATE.

    // permission management
    AUTHORIZE; // required for GRANT and REVOKE.


    public static final Set<Permission> ALL_DATA =
            ImmutableSet.copyOf(EnumSet.range(Permission.CREATE, Permission.AUTHORIZE));

    public static final Set<Permission> ALL =
            ImmutableSet.copyOf(EnumSet.range(Permission.CREATE, Permission.AUTHORIZE));
    public static final Set<Permission> NONE = ImmutableSet.of();
}
