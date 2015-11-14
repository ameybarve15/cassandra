
/**
 * The interface at the core of Cassandra authorization.
 *
 * Represents a resource in the hierarchy.
 * Currently just one resource type is supported by Cassandra
 * @see DataResource
 */
public interface IResource
{
    /**
     * @return printable name of the resource.
     */
    String getName();

    /**
     * Gets next resource in the hierarchy. Call hasParent first to make sure there is one.
     *
     * @return Resource parent (or IllegalStateException if there is none). Never a null.
     */
    IResource getParent();

    /**
     * Indicates whether or not this resource has a parent in the hierarchy.
     *
     * Please perform this check before calling getParent() method.
     * @return Whether or not the resource has a parent.
     */
    boolean hasParent();

    /**
     * @return Whether or not this resource exists in Cassandra.
     */
    boolean exists();
}
