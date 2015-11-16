

public abstract class StreamEvent
{
    public static enum Type
    {
        STREAM_PREPARED,
        STREAM_COMPLETE,
        FILE_PROGRESS,
    }

    public final Type eventType;
    public final UUID planId;

    protected StreamEvent(Type eventType, UUID planId)
    {
        this.eventType = eventType;
        this.planId = planId;
    }

    public static class SessionCompleteEvent extends StreamEvent
    {
        public final InetAddress peer;
        public final boolean success;
        public final int sessionIndex;

        public SessionCompleteEvent(StreamSession session)
        {
            super(Type.STREAM_COMPLETE, session.planId());
            this.peer = session.peer;
            this.success = session.isSuccess();
            this.sessionIndex = session.sessionIndex();
        }
    }

    public static class ProgressEvent extends StreamEvent
    {
        public final ProgressInfo progress;

        public ProgressEvent(UUID planId, ProgressInfo progress)
        {
            super(Type.FILE_PROGRESS, planId);
            this.progress = progress;
        }

        @Override
        public String toString()
        {
            return "<ProgressEvent " + progress.toString() + ">";
        }
    }

    public static class SessionPreparedEvent extends StreamEvent
    {
        public final SessionInfo session;

        public SessionPreparedEvent(UUID planId, SessionInfo session)
        {
            super(Type.STREAM_PREPARED, planId);
            this.session = session;
        }
    }
}
