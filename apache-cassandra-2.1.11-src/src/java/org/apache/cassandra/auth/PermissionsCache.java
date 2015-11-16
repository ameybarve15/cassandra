

public class PermissionsCache implements PermissionsCacheMBean
{
    private final String MBEAN_NAME = "org.apache.cassandra.auth:type=PermissionsCache";

    private final ThreadPoolExecutor cacheRefreshExecutor = new DebuggableThreadPoolExecutor("PermissionsCacheRefresh",
                                                                                             Thread.NORM_PRIORITY);
    private final IAuthorizer authorizer;
    private volatile LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> cache;

    public PermissionsCache(IAuthorizer authorizer)
    {
        this.authorizer = authorizer;
        this.cache = initCache(null);
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
    }

    public Set<Permission> getPermissions(AuthenticatedUser user, IResource resource)
    {
        if (cache == null)
            return authorizer.authorize(user, resource);

        try
        {
            return cache.get(Pair.create(user, resource));
        }
    }

    public void invalidate()
    {
        cache = initCache(null);
    }

    public void setValidity(int validityPeriod)
    {
        DatabaseDescriptor.setPermissionsValidity(validityPeriod);
        cache = initCache(cache);
    }

    public int getValidity()
    {
        return DatabaseDescriptor.getPermissionsValidity();
    }

    public void setUpdateInterval(int updateInterval)
    {
        DatabaseDescriptor.setPermissionsUpdateInterval(updateInterval);
        cache = initCache(cache);
    }

    public int getUpdateInterval()
    {
        return DatabaseDescriptor.getPermissionsUpdateInterval();
    }

    private LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> initCache(
                                                             LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> existing)
    {
        if (authorizer instanceof AllowAllAuthorizer)
            return null;

        if (DatabaseDescriptor.getPermissionsValidity() <= 0)
            return null;

        LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> newcache = CacheBuilder.newBuilder()
                           .refreshAfterWrite(DatabaseDescriptor.getPermissionsUpdateInterval(), TimeUnit.MILLISECONDS)
                           .expireAfterWrite(DatabaseDescriptor.getPermissionsValidity(), TimeUnit.MILLISECONDS)
                           .maximumSize(DatabaseDescriptor.getPermissionsCacheMaxEntries())
                           .build(new CacheLoader<Pair<AuthenticatedUser, IResource>, Set<Permission>>()
                           {
                               public Set<Permission> load(Pair<AuthenticatedUser, IResource> userResource)
                               {
                                   return authorizer.authorize(userResource.left, userResource.right);
                               }

                               public ListenableFuture<Set<Permission>> reload(final Pair<AuthenticatedUser, IResource> userResource,
                                                                               final Set<Permission> oldValue)
                               {
                                   ListenableFutureTask<Set<Permission>> task = ListenableFutureTask.create(new Callable<Set<Permission>>()
                                   {
                                       public Set<Permission>call() throws Exception
                                       {
                                           try
                                           {
                                               return authorizer.authorize(userResource.left, userResource.right);
                                           }
                                           catch (Exception e)
                                           {
                                               logger.debug("Error performing async refresh of user permissions", e);
                                               throw e;
                                           }
                                       }
                                   });
                                   cacheRefreshExecutor.execute(task);
                                   return task;
                               }
                           });
        if (existing != null)
            newcache.putAll(existing.asMap());
        return newcache;
    }
}
