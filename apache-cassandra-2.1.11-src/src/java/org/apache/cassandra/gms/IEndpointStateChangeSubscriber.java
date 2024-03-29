

/**
 * This is called by an instance of the IEndpointStateChangePublisher to notify
 * interested parties about changes in the the state associated with any endpoint.
 * For instance if node A figures there is a changes in state for an endpoint B
 * it notifies all interested parties of this change. It is upto to the registered
 * instance to decide what he does with this change. Not all modules maybe interested
 * in all state changes.
 */
public interface IEndpointStateChangeSubscriber
{
    /**
     * Use to inform interested parties about the change in the state
     * for specified endpoint
     *
     * @param endpoint endpoint for which the state change occurred.
     * @param epState  state that actually changed for the above endpoint.
     */
    public void onJoin(InetAddress endpoint, EndpointState epState);
    
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue);

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value);

    public void onAlive(InetAddress endpoint, EndpointState state);

    public void onDead(InetAddress endpoint, EndpointState state);

    public void onRemove(InetAddress endpoint);

    /**
     * Called whenever a node is restarted.
     * Note that there is no guarantee when that happens that the node was
     * previously marked down. It will have only if {@code state.isAlive() == false}
     * as {@code state} is from before the restarted node is marked up.
     */
    public void onRestart(InetAddress endpoint, EndpointState state);
}
