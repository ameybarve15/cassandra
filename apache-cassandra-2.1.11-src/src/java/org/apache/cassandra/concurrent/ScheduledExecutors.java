
public class ScheduledExecutors
{
    /**
     * This pool is used for periodic short (sub-second) tasks.
     */
     public static final DebuggableScheduledThreadPoolExecutor scheduledTasks = new DebuggableScheduledThreadPoolExecutor("ScheduledTasks");

    /**
     * This executor is used for tasks that can have longer execution times, and usually are non periodic.
     */
    public static final DebuggableScheduledThreadPoolExecutor nonPeriodicTasks = new DebuggableScheduledThreadPoolExecutor("NonPeriodicTasks");
    static
    {
        nonPeriodicTasks.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    /**
     * This executor is used for tasks that do not need to be waited for on shutdown/drain.
     */
    public static final DebuggableScheduledThreadPoolExecutor optionalTasks = new DebuggableScheduledThreadPoolExecutor("OptionalTasks");
}
