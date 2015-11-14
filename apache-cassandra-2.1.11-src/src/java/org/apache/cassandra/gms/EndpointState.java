

public class EndpointState
{

    private volatile HeartBeatState hbState;
    final Map<ApplicationState, VersionedValue> applicationState = new NonBlockingHashMap<ApplicationState, VersionedValue>();

    /* fields below do not get serialized */
    private volatile long updateTimestamp;
    private volatile boolean isAlive;

    EndpointState(HeartBeatState initialHbState)
    {
        hbState = initialHbState;
        updateTimestamp = System.nanoTime();
        isAlive = true;
    }

    HeartBeatState getHeartBeatState()
    {
        return hbState;
    }

    void setHeartBeatState(HeartBeatState newHbState)
    {
        updateTimestamp();
        hbState = newHbState;
    }

    public VersionedValue getApplicationState(ApplicationState key)
    {
        return applicationState.get(key);
    }

    /**
     * TODO replace this with operations that don't expose private state
     */
    @Deprecated
    public Map<ApplicationState, VersionedValue> getApplicationStateMap()
    {
        return applicationState;
    }

    void addApplicationState(ApplicationState key, VersionedValue value)
    {
        applicationState.put(key, value);
    }

    void markDead()
    {
        isAlive = false;
    }
}

