

/**
 * Implemented by the RepairSession to accept callbacks from sequential snapshot creation failure.
 */

public interface IRepairJobEventListener
{
    /**
     * Signal that there was a failure during the snapshot creation process.
     *
     */
    public void failedSnapshot();
}
