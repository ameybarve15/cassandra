

/**
 * Like DebuggableThreadPoolExecutor, DebuggableScheduledThreadPoolExecutor always
 * logs exceptions from the tasks it is given, even if Future.get is never called elsewhere.
 *
 * DebuggableScheduledThreadPoolExecutor also catches exceptions during Task execution
 * so that they don't supress subsequent invocations of the task.
 *
 * Finally, there is a special rejected execution handler for tasks rejected during the shutdown hook.
 *
 * For fire and forget tasks (like ref tidy) we can safely ignore the exceptions.
 * For any callers that care to know their task was rejected we cancel passed task.
 */
public class DebuggableScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(DebuggableScheduledThreadPoolExecutor.class);

    public static final RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler()
    {
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor)
        {
            if (executor.isShutdown())
            {
                if (!StorageService.instance.isInShutdownHook())
                    throw new RejectedExecutionException("ScheduledThreadPoolExecutor has shut down.");

                //Give some notification to the caller the task isn't going to run
                if (task instanceof Future)
                    ((Future) task).cancel(false);

                logger.debug("ScheduledThreadPoolExecutor has shut down as part of C* shutdown");
            }
            else
            {
                throw new AssertionError("Unknown rejection of ScheduledThreadPoolExecutor task");
            }
        }
    };

    public DebuggableScheduledThreadPoolExecutor(int corePoolSize, String threadPoolName, int priority)
    {
        super(corePoolSize, new NamedThreadFactory(threadPoolName, priority));
        setRejectedExecutionHandler(rejectedExecutionHandler);
    }

    public DebuggableScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory)
    {
        super(corePoolSize, threadFactory);
        setRejectedExecutionHandler(rejectedExecutionHandler);
    }

    public DebuggableScheduledThreadPoolExecutor(String threadPoolName)
    {
        this(1, threadPoolName, Thread.NORM_PRIORITY);
        setRejectedExecutionHandler(rejectedExecutionHandler);
    }

    // We need this as well as the wrapper for the benefit of non-repeating tasks
    @Override
    public void afterExecute(Runnable r, Throwable t)
    {
        super.afterExecute(r,t);
        DebuggableThreadPoolExecutor.logExceptionsAfterExecute(r, t);
    }

    // override scheduling to supress exceptions that would cancel future executions
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
    {
        return super.scheduleAtFixedRate(new UncomplainingRunnable(command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
    {
        return super.scheduleWithFixedDelay(new UncomplainingRunnable(command), initialDelay, delay, unit);
    }

    private static class UncomplainingRunnable implements Runnable
    {
        private final Runnable runnable;

        public UncomplainingRunnable(Runnable runnable)
        {
            this.runnable = runnable;
        }

        public void run()
        {
            runnable.run();
        }
    }
}
