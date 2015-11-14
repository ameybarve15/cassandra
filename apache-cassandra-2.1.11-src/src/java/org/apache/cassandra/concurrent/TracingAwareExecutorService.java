

public interface TracingAwareExecutorService extends ExecutorService
{
    // we need a way to inject a TraceState directly into the Executor context without going through
    // the global Tracing sessions; see CASSANDRA-5668
    public void execute(Runnable command, TraceState state);

    // permits executing in the context of the submitting thread
    public void maybeExecuteImmediately(Runnable command);
}
