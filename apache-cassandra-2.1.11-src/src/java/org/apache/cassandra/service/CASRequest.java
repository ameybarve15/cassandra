

/**
 * Abstract the conditions and updates for a CAS operation.
 */
public interface CASRequest
{
    /**
     * The filter to use to fetch the value to compare for the CAS.
     */
    public IDiskAtomFilter readFilter();

    /**
     * Returns whether the provided CF, that represents the values fetched using the
     * readFilter(), match the CAS conditions this object stands for.
     */
    public boolean appliesTo(ColumnFamily current) throws InvalidRequestException;

    /**
     * The updates to perform of a CAS success. The values fetched using the readFilter()
     * are passed as argument.
     */
    public ColumnFamily makeUpdates(ColumnFamily current) throws InvalidRequestException;
}
