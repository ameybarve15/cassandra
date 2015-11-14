

/**
 * Interface on which interested parties can be notified of high level endpoint
 * state changes.
 *
 * Note that while IEndpointStateChangeSubscriber notify about gossip related
 * changes (IEndpointStateChangeSubscriber.onJoin() is called when a node join
 * gossip), this interface allows to be notified about higher level events.
 */
public interface IEndpointLifecycleSubscriber
{
    /**
     * Called when a new node joins the cluster, i.e. either has just been
     * bootstrapped or "instajoins".
     *
     * @param endpoint the newly added endpoint.
     */
    public void onJoinCluster(InetAddress endpoint);

    /**
     * Called when a new node leave the cluster (decommission or removeToken).
     *
     * @param endpoint the endpoint that is leaving.
     */
    public void onLeaveCluster(InetAddress endpoint);

    /**
     * Called when a node is marked UP.
     *
     * @param endpoint the endpoint marked UP.
     */
    public void onUp(InetAddress endpoint);

    /**
     * Called when a node is marked DOWN.
     *
     * @param endpoint the endpoint marked DOWN.
     */
    public void onDown(InetAddress endpoint);

    /**
     * Called when a node has moved (to a new token).
     *
     * @param endpoint the endpoint that has moved.
     */
    public void onMove(InetAddress endpoint);
}
