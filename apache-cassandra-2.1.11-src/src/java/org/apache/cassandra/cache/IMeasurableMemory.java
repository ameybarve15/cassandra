


public interface IMeasurableMemory
{
    /**
     * @return the amount of on-heap memory retained by the object that might be reclaimed if the object were reclaimed,
     * i.e. it should try to exclude globally cached data where possible, or counting portions of arrays that are
     * referenced by the object but used by other objects only (e.g. slabbed byte-buffers), etc.
     */
    public long unsharedHeapSize();
}
