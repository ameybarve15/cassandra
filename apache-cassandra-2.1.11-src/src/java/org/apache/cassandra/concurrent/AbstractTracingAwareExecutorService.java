

public abstract class AbstractTracingAwareExecutorService implements TracingAwareExecutorService
{
    protected abstract void addTask(FutureTask<?> futureTask);
    protected abstract void onCompletion();

    /** Task Submission / Creation / Objects **/

    public <T> FutureTask<T> submit(Callable<T> task)
    {
        return submit(newTaskFor(task));
    }

    public FutureTask<?> submit(Runnable task)
    {
        return submit(newTaskFor(task, null));
    }

    public <T> FutureTask<T> submit(Runnable task, T result)
    {
        return submit(newTaskFor(task, result));
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
    {
        throw new UnsupportedOperationException();
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
    {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        throw new UnsupportedOperationException();
    }

    protected <T> FutureTask<T> newTaskFor(Runnable runnable, T result)
    {
        return newTaskFor(runnable, result, Tracing.instance.get());
    }

    protected <T> FutureTask<T> newTaskFor(Runnable runnable, T result, TraceState traceState)
    {
        if (traceState != null)
        {
            if (runnable instanceof TraceSessionFutureTask)
                return (TraceSessionFutureTask<T>) runnable;
            return new TraceSessionFutureTask<T>(runnable, result, traceState);
        }
        if (runnable instanceof FutureTask)
            return (FutureTask<T>) runnable;
        return new FutureTask<>(runnable, result);
    }

    protected <T> FutureTask<T> newTaskFor(Callable<T> callable)
    {
        if (isTracing())
        {
            if (callable instanceof TraceSessionFutureTask)
                return (TraceSessionFutureTask<T>) callable;
            return new TraceSessionFutureTask<T>(callable, Tracing.instance.get());
        }
        if (callable instanceof FutureTask)
            return (FutureTask<T>) callable;
        return new FutureTask<>(callable);
    }

    private class TraceSessionFutureTask<T> extends FutureTask<T>
    {
        private final TraceState state;

        public TraceSessionFutureTask(Callable<T> callable, TraceState state)
        {
            super(callable);
            this.state = state;
        }

        public TraceSessionFutureTask(Runnable runnable, T result, TraceState state)
        {
            super(runnable, result);
            this.state = state;
        }

        public void run()
        {
            TraceState oldState = Tracing.instance.get();
            Tracing.instance.set(state);
            try
            {
                super.run();
            }
            finally
            {
                Tracing.instance.set(oldState);
            }
        }
    }

    class FutureTask<T> extends SimpleCondition implements Future<T>, Runnable
    {
        private boolean failure;
        private Object result = this;
        private final Callable<T> callable;

        public FutureTask(Callable<T> callable)
        {
            this.callable = callable;
        }
        public FutureTask(Runnable runnable, T result)
        {
            this(Executors.callable(runnable, result));
        }

        public void run()
        {
            try
            {
                result = callable.call();
            }
            finally
            {
                signalAll();
                onCompletion();
            }
        }

        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        public boolean isCancelled()
        {
            return false;
        }

        public boolean isDone()
        {
            return isSignaled();
        }

        public T get() throws InterruptedException, ExecutionException
        {
            await();
            Object result = this.result;
            if (failure)
                throw new ExecutionException((Throwable) result);
            return (T) result;
        }

        public T get(long timeout, TimeUnit unit) throws 
        {
            await(timeout, unit);
            Object result = this.result;
            if (failure)
                throw new ExecutionException((Throwable) result);
            return (T) result;
        }
    }

    private <T> FutureTask<T> submit(FutureTask<T> task)
    {
        addTask(task);
        return task;
    }

    public void execute(Runnable command)
    {
        addTask(newTaskFor(command, null));
    }

    public void execute(Runnable command, TraceState state)
    {
        addTask(newTaskFor(command, null, state));
    }
}
