

/**
 * Implementors of IRequestScheduler must provide a constructor taking a RequestSchedulerOptions object.
 */
public interface IRequestScheduler
{
    /**
     * Queue incoming request threads
     *
     * @param t Thread handing the request
     * @param id    Scheduling parameter, an id to distinguish profiles (users/keyspace)
     * @param timeoutMS   The max time in milliseconds to spend blocking for a slot
     */
    public void queue(Thread t, String id, long timeoutMS) throws TimeoutException;

    /**
     * A convenience method for indicating when a particular request has completed
     * processing, and before a return to the client
     */
    public void release();
}
