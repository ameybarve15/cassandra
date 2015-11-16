
public class RefCountedMemory extends Memory implements AutoCloseable
{
    private volatile int references = 1;
    private static final AtomicIntegerFieldUpdater<RefCountedMemory> UPDATER = AtomicIntegerFieldUpdater.newUpdater(RefCountedMemory.class, "references");

    public RefCountedMemory(long size)
    {
        super(size);
    }

    /**
     * @return true if we succeed in referencing before the reference count reaches zero.
     * (A FreeableMemory object is created with a reference count of one.)
     */
    public boolean reference()
    {
        while (true)
        {
            int n = UPDATER.get(this);
            if (n <= 0)
                return false;
            if (UPDATER.compareAndSet(this, n, n + 1))
                return true;
        }
    }

    /** decrement reference count.  if count reaches zero, the object is freed. */
    public void unreference()
    {
        if (UPDATER.decrementAndGet(this) == 0)
            super.free();
    }

    public RefCountedMemory copy(long newSize)
    {
        RefCountedMemory copy = new RefCountedMemory(newSize);
        copy.put(0, this, 0, Math.min(size(), newSize));
        return copy;
    }

    public void free()
    {
        throw new AssertionError();
    }

    public void close()
    {
        unreference();
    }
}
