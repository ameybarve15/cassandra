


/**
 * This is basically not having a scheduler, the requests are
 * processed as normally would be handled by the JVM.
 */
public class NoScheduler implements IRequestScheduler
{

    public NoScheduler(RequestSchedulerOptions options) {}

    public NoScheduler() {}

    public void queue(Thread t, String id, long timeoutMS) {}

    public void release() {}
}
