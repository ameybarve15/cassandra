

/**
 * Perform a query, paging it by page of a given size.
 *
 * This is essentially an iterator of pages. Each call to fetchPage() will
 * return the next page (i.e. the next list of rows) and isExhausted()
 * indicates whether there is more page to fetch. The pageSize will
 * either be in term of cells or in term of CQL3 row, depending on the
 * parameters of the command we page.
 *
 * Please note that the pager might page within rows, so there is no guarantee
 * that successive pages won't return the same row (though with different
 * columns every time).
 *
 * Also, there is no guarantee that fetchPage() won't return an empty list,
 * even if isExhausted() return false (but it is guaranteed to return an empty
 * list *if* isExhausted() return true). Indeed, isExhausted() does *not*
 * trigger a query so in some (fairly rare) case we might not know the paging
 * is done even though it is.
 */
public interface QueryPager
{
    /**
     * Fetches the next page.
     *
     * @param pageSize the maximum number of elements to return in the next page.
     * @return the page of result.
     */
    public List<Row> fetchPage(int pageSize) throws RequestValidationException, RequestExecutionException;

    /**
     * Whether or not this pager is exhausted, i.e. whether or not a call to
     * fetchPage may return more result.
     *
     * @return whether the pager is exhausted.
     */
    public boolean isExhausted();

    /**
     * The maximum number of cells/CQL3 row that we may still have to return.
     * In other words, that's the initial user limit minus what we've already
     * returned (note that it's not how many we *will* return, just the upper
     * limit on it).
     */
    public int maxRemaining();

    /**
     * Get the current state of the pager. The state can allow to restart the
     * paging on another host from where we are at this point.
     *
     * @return the current paging state. Will return null if paging is at the
     * beginning. If the pager is exhausted, the result is undefined.
     */
    public PagingState state();
}
