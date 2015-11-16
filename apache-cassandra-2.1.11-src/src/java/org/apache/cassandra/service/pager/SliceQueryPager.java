

/**
 * Pager over a SliceFromReadCommand.
 */
public class SliceQueryPager extends AbstractQueryPager implements SinglePartitionPager
{
    private static final Logger logger = LoggerFactory.getLogger(SliceQueryPager.class);

    private final SliceFromReadCommand command;
    private final ClientState cstate;

    private volatile CellName lastReturned;

    // Don't use directly, use QueryPagers method instead
    SliceQueryPager(SliceFromReadCommand command, ConsistencyLevel consistencyLevel, ClientState cstate, boolean localQuery)
    {
        super(consistencyLevel, command.filter.count, localQuery, command.ksName, command.cfName, command.filter, command.timestamp);
        this.command = command;
        this.cstate = cstate;
    }

    SliceQueryPager(SliceFromReadCommand command, ConsistencyLevel consistencyLevel, ClientState cstate, boolean localQuery, PagingState state)
    {
        this(command, consistencyLevel, cstate, localQuery);

        if (state != null)
        {
            // The cellname can be empty if this is used in a MultiPartitionPager and we're supposed to start reading this row
            // (because the previous page has exhausted the previous pager). See #10352 for details.
            if (state.cellName.hasRemaining())
                lastReturned = (CellName) cfm.comparator.fromByteBuffer(state.cellName);
            restoreState(state.remaining, true);
        }
    }

    public ByteBuffer key()
    {
        return command.key;
    }

    public PagingState state()
    {
        return lastReturned == null
             ? null
             : new PagingState(null, lastReturned.toByteBuffer(), maxRemaining());
    }

    protected List<Row> queryNextPage(int pageSize, ConsistencyLevel consistencyLevel, boolean localQuery)
    throws RequestValidationException, RequestExecutionException
    {
        // For some queries, such as a DISTINCT query on static columns, the limit for slice queries will be lower
        // than the page size (in the static example, it will be 1).  We use the min here to ensure we don't fetch
        // more rows than we're supposed to.  See CASSANDRA-8108 for more details.
        SliceQueryFilter filter = command.filter.withUpdatedCount(Math.min(command.filter.count, pageSize));
        if (lastReturned != null)
            filter = filter.withUpdatedStart(lastReturned, cfm);

        logger.debug("Querying next page of slice query; new filter: {}", filter);
        ReadCommand pageCmd = command.withUpdatedFilter(filter);
        return localQuery
             ? Collections.singletonList(pageCmd.getRow(Keyspace.open(command.ksName)))
             : StorageProxy.read(Collections.singletonList(pageCmd), consistencyLevel, cstate);
    }

    protected boolean containsPreviousLast(Row first)
    {
        if (lastReturned == null)
            return false;

        Cell firstCell = isReversed() ? lastCell(first.cf) : firstNonStaticCell(first.cf);
        CFMetaData metadata = Schema.instance.getCFMetaData(command.getKeyspace(), command.getColumnFamilyName());
        // Note: we only return true if the column is the lastReturned *and* it is live. If it is deleted, it is ignored by the
        // rest of the paging code (it hasn't been counted as live in particular) and we want to act as if it wasn't there.
        return !first.cf.deletionInfo().isDeleted(firstCell)
            && firstCell.isLive(timestamp())
            && firstCell.name().isSameCQL3RowAs(metadata.comparator, lastReturned);
    }

    protected boolean recordLast(Row last)
    {
        Cell lastCell = isReversed() ? firstNonStaticCell(last.cf) : lastCell(last.cf);
        lastReturned = lastCell.name();
        return true;
    }

    protected boolean isReversed()
    {
        return command.filter.reversed;
    }
}
