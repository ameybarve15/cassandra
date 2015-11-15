

/**
 * Pages a RangeSliceCommand whose predicate is a slice query.
 *
 * Note: this only work for CQL3 queries for now (because thrift queries expect
 * a different limit on the rows than on the columns, which complicates it).
 */
public class RangeSliceQueryPager extends AbstractQueryPager
{
    private final RangeSliceCommand command;
    private volatile DecoratedKey lastReturnedKey;
    private volatile CellName lastReturnedName;

    // Don't use directly, use QueryPagers method instead
    RangeSliceQueryPager(RangeSliceCommand command, ConsistencyLevel consistencyLevel, boolean localQuery)
    {
        super(consistencyLevel, command.maxResults, localQuery, command.keyspace, command.columnFamily, command.predicate, command.timestamp);
        this.command = command;
        assert columnFilter instanceof SliceQueryFilter;
    }

    RangeSliceQueryPager(RangeSliceCommand command, ConsistencyLevel consistencyLevel, boolean localQuery, PagingState state)
    {
        this(command, consistencyLevel, localQuery);

        if (state != null)
        {
            lastReturnedKey = StorageService.getPartitioner().decorateKey(state.partitionKey);
            lastReturnedName = cfm.comparator.cellFromByteBuffer(state.cellName);
            restoreState(state.remaining, true);
        }
    }

    public PagingState state()
    {
        return lastReturnedKey == null
             ? null
             : new PagingState(lastReturnedKey.getKey(), lastReturnedName.toByteBuffer(), maxRemaining());
    }

    protected List<Row> queryNextPage(int pageSize, ConsistencyLevel consistencyLevel, boolean localQuery)
    throws RequestExecutionException
    {
        SliceQueryFilter sf = (SliceQueryFilter)columnFilter;
        AbstractBounds<RowPosition> keyRange = lastReturnedKey == null ? command.keyRange : makeIncludingKeyBounds(lastReturnedKey);
        Composite start = lastReturnedName == null ? sf.start() : lastReturnedName;
        PagedRangeCommand pageCmd = new PagedRangeCommand(command.keyspace,
                                                          command.columnFamily,
                                                          command.timestamp,
                                                          keyRange,
                                                          sf,
                                                          start,
                                                          sf.finish(),
                                                          command.rowFilter,
                                                          pageSize,
                                                          command.countCQL3Rows);

        return localQuery
             ? pageCmd.executeLocally()
             : StorageProxy.getRangeSlice(pageCmd, consistencyLevel);
    }

    protected boolean containsPreviousLast(Row first)
    {
        if (lastReturnedKey == null || !lastReturnedKey.equals(first.key))
            return false;

        // Same as SliceQueryPager, we ignore a deleted column
        Cell firstCell = isReversed() ? lastCell(first.cf) : firstNonStaticCell(first.cf);
        CFMetaData metadata = Schema.instance.getCFMetaData(command.keyspace, command.columnFamily);
        return !first.cf.deletionInfo().isDeleted(firstCell)
            && firstCell.isLive(timestamp())
            && firstCell.name().isSameCQL3RowAs(metadata.comparator, lastReturnedName);
    }

    protected boolean recordLast(Row last)
    {
        lastReturnedKey = last.key;
        lastReturnedName = (isReversed() ? firstNonStaticCell(last.cf) : lastCell(last.cf)).name();
        return true;
    }

    protected boolean isReversed()
    {
        return ((SliceQueryFilter)command.predicate).reversed;
    }

    private AbstractBounds<RowPosition> makeIncludingKeyBounds(RowPosition lastReturnedKey)
    {
        // We always include lastReturnedKey since we may still be paging within a row,
        // and PagedRangeCommand will move over if we're not anyway
        AbstractBounds<RowPosition> bounds = command.keyRange;
        if (bounds instanceof Range || bounds instanceof Bounds)
        {
            return new Bounds<RowPosition>(lastReturnedKey, bounds.right);
        }
        else
        {
            return new IncludingExcludingBounds<RowPosition>(lastReturnedKey, bounds.right);
        }
    }
}
