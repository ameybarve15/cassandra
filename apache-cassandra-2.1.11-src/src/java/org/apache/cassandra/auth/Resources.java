

public final class Resources
{
    /**
     * Construct a chain of resource parents starting with the resource and ending with the root.
     *
     * @param resource The staring point.
     * @return list of resource in the chain form start to the root.
     */
    public static List<? extends IResource> chain(IResource resource)
    {
        List<IResource> chain = new ArrayList<IResource>();
        while (true)
        {
           chain.add(resource);
           if (!resource.hasParent())
               break;
           resource = resource.getParent();
        }
        return chain;
    }

    @Deprecated
    public final static String ROOT = "cassandra";
    @Deprecated
    public final static String KEYSPACES = "keyspaces";
}
