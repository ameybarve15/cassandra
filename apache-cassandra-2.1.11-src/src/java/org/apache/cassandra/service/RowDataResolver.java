

public class RowDataResolver extends AbstractRowResolver
{
    private int maxLiveCount = 0;
    public List<AsyncOneResponse> repairResults = Collections.emptyList();
    private final IDiskAtomFilter filter;
    private final long timestamp;

    public RowDataResolver(String keyspaceName, ByteBuffer key, IDiskAtomFilter qFilter, long timestamp)
    {
        super(key, keyspaceName);
        this.filter = qFilter;
        this.timestamp = timestamp;
    }

    /*
    * This method handles the following scenario:
    *
    * there was a mismatch on the initial read, so we redid the digest requests
    * as full data reads.  In this case we need to compute the most recent version
    * of each column, and send diffs to out-of-date replicas.
    */
    public Row resolve() throws DigestMismatchException
    {
        int replyCount = replies.size();
        if (logger.isDebugEnabled())
            logger.debug("resolving {} responses", replyCount);
        long start = System.nanoTime();

        ColumnFamily resolved;
        if (replyCount > 1)
        {
            List<ColumnFamily> versions = new ArrayList<>(replyCount);
            List<InetAddress> endpoints = new ArrayList<>(replyCount);

            for (MessageIn<ReadResponse> message : replies)
            {
                ReadResponse response = message.payload;
                ColumnFamily cf = response.row().cf;
                assert !response.isDigestQuery() : "Received digest response to repair read from " + message.from;
                versions.add(cf);
                endpoints.add(message.from);

                // compute maxLiveCount to prevent short reads -- see https://issues.apache.org/jira/browse/CASSANDRA-2643
                int liveCount = cf == null ? 0 : filter.getLiveCount(cf, timestamp);
                if (liveCount > maxLiveCount)
                    maxLiveCount = liveCount;
            }

            resolved = resolveSuperset(versions, timestamp);
            if (logger.isDebugEnabled())
                logger.debug("versions merged");

            // send updates to any replica that was missing part of the full row
            // (resolved can be null even if versions doesn't have all nulls because of the call to removeDeleted in resolveSuperSet)
            if (resolved != null)
                repairResults = scheduleRepairs(resolved, keyspaceName, key, versions, endpoints);
        }
        else
        {
            resolved = replies.iterator().next().payload.row().cf;
        }

        if (logger.isDebugEnabled())
            logger.debug("resolve: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        return new Row(key, resolved);
    }

    /**
     * For each row version, compare with resolved (the superset of all row versions);
     * if it is missing anything, send a mutation to the endpoint it come from.
     */
    public static List<AsyncOneResponse> scheduleRepairs(ColumnFamily resolved, String keyspaceName, DecoratedKey key, List<ColumnFamily> versions, List<InetAddress> endpoints)
    {
        List<AsyncOneResponse> results = new ArrayList<AsyncOneResponse>(versions.size());

        for (int i = 0; i < versions.size(); i++)
        {
            ColumnFamily diffCf = ColumnFamily.diff(versions.get(i), resolved);
            if (diffCf == null) // no repair needs to happen
                continue;

            // create and send the mutation message based on the diff
            Mutation mutation = new Mutation(keyspaceName, key.getKey(), diffCf);
            // use a separate verb here because we don't want these to be get the white glove hint-
            // on-timeout behavior that a "real" mutation gets
            Tracing.trace("Sending read-repair-mutation to {}", endpoints.get(i));
            results.add(MessagingService.instance().sendRR(mutation.createMessage(MessagingService.Verb.READ_REPAIR),
                                                           endpoints.get(i)));
        }

        return results;
    }

    static ColumnFamily resolveSuperset(Iterable<ColumnFamily> versions, long now)
    {
        assert Iterables.size(versions) > 0;

        ColumnFamily resolved = null;
        for (ColumnFamily cf : versions)
        {
            if (cf == null)
                continue;

            if (resolved == null)
                resolved = cf.cloneMeShallow();
            else
                resolved.delete(cf);
        }
        if (resolved == null)
            return null;

        // mimic the collectCollatedColumn + removeDeleted path that getColumnFamily takes.
        // this will handle removing columns and subcolumns that are suppressed by a row or
        // supercolumn tombstone.
        QueryFilter filter = new QueryFilter(null, resolved.metadata().cfName, new IdentityQueryFilter(), now);
        List<CloseableIterator<Cell>> iters = new ArrayList<>(Iterables.size(versions));
        for (ColumnFamily version : versions)
            if (version != null)
                iters.add(FBUtilities.closeableIterator(version.iterator()));
        filter.collateColumns(resolved, iters, Integer.MIN_VALUE);
        return ColumnFamilyStore.removeDeleted(resolved, Integer.MIN_VALUE);
    }

    public Row getData()
    {
        assert !replies.isEmpty();
        return replies.peek().payload.row();
    }

    public boolean isDataPresent()
    {
        return !replies.isEmpty();
    }

    public int getMaxLiveCount()
    {
        return maxLiveCount;
    }
}
