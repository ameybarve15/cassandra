

/**
 * Current snapshot of streaming progress.
 */
public class StreamState implements Serializable
{
    public final UUID planId;
    public final String description;
    public final Set<SessionInfo> sessions;

    public StreamState(UUID planId, String description, Set<SessionInfo> sessions)
    {
        this.planId = planId;
        this.description = description;
        this.sessions = sessions;
    }

    public boolean hasFailedSession()
    {
        return Iterables.any(sessions, new Predicate<SessionInfo>()
        {
            public boolean apply(SessionInfo session)
            {
                return session.isFailed();
            }
        });
    }
}
