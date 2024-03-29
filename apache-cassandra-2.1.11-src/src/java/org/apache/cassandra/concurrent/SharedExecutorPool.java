
/**
 * A pool of worker threads that are shared between all Executors created with it. Each executor is treated as a distinct
 * unit, with its own concurrency and task queue limits, but the threads that service the tasks on each executor are
 * free to hop between executors at will.
 *
 * To keep producers from incurring unnecessary delays, once an executor is "spun up" (i.e. is processing tasks at a steady
 * rate), adding tasks to the executor often involves only placing the task on the work queue and updating the
 * task permits (which imposes our max queue length constraints). Only when it cannot be guaranteed the task will be serviced
 * promptly does the producer have to signal a thread itself to perform the work.
 *
 * We do this by scheduling only if
 *
 * The worker threads schedule themselves as far as possible: when they are assigned a task, they will attempt to spawn
 * a partner worker to service any other work outstanding on the queue (if any); once they have finished the task they
 * will either take another (if any remaining) and repeat this, or they will attempt to assign themselves to another executor
 * that does have tasks remaining. If both fail, it will enter a non-busy-spinning phase, where it will sleep for a short
 * random interval (based upon the number of threads in this mode, so that the total amount of non-sleeping time remains
 * approximately fixed regardless of the number of spinning threads), and upon waking up will again try to assign themselves
 * an executor with outstanding tasks to perform.
 */
public class SharedExecutorPool
{

    // the name assigned to workers in the pool, and the id suffix
    final String poolName;
    final AtomicLong workerId = new AtomicLong();

    // the collection of executors serviced by this pool; periodically ordered by traffic volume
    final List<SEPExecutor> executors = new CopyOnWriteArrayList<>();

    // the number of workers currently in a spinning state
    final AtomicInteger spinningCount = new AtomicInteger();
    // see SEPWorker.maybeStop() - used to self coordinate stopping of threads
    final AtomicLong stopCheck = new AtomicLong();
    // the collection of threads that are (most likely) in a spinning state - new workers are scheduled from here first
    // TODO: consider using a queue partially-ordered by scheduled wake-up time
    // (a full-fledged correctly ordered SkipList is overkill)
    final ConcurrentSkipListMap<Long, SEPWorker> spinning = new ConcurrentSkipListMap<>();
    // the collection of threads that have been asked to stop/deschedule - new workers are scheduled from here last
    final ConcurrentSkipListMap<Long, SEPWorker> descheduled = new ConcurrentSkipListMap<>();

    public SharedExecutorPool(String poolName)
    {
        this.poolName = poolName;
    }

    void schedule(Work work)
    {
        // we try to hand-off our work to the spinning queue before the descheduled queue, even though we expect it to be empty
        // all we're doing here is hoping to find a worker without work to do, but it doesn't matter too much what we find;
        // we atomically set the task so even if this were a collection of all workers it would be safe, and if they are both
        // empty we schedule a new thread
        Map.Entry<Long, SEPWorker> e;
        while (null != (e = spinning.pollFirstEntry()) || null != (e = descheduled.pollFirstEntry()))
            if (e.getValue().assign(work, false))
                return;

        if (!work.isStop())
            new SEPWorker(workerId.incrementAndGet(), work, this);
    }

    void maybeStartSpinningWorker()
    {
        // in general the workers manage spinningCount directly; however if it is zero, we increment it atomically
        // ourselves to avoid starting a worker unless we have to
        int current = spinningCount.get();
        if (current == 0 && spinningCount.compareAndSet(0, 1))
            schedule(Work.SPINNING);
    }
}