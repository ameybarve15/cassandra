

/**
 * During compaction we can drop entire sstables if they only contain expired tombstones and if it is guaranteed
 * to not cover anything in other sstables. An expired sstable can be blocked from getting dropped if its newest
 * timestamp is newer than the oldest data in another sstable.
 *
 * This class outputs all sstables that are blocking other sstables from getting dropped so that a user can
 * figure out why certain sstables are still on disk.
 */
public class SSTableExpiredBlockers
{
    public static void main(String[] args) throws IOException
    {
        PrintStream out = System.out;
        if (args.length < 2)
        {
            out.println("Usage: sstableexpiredblockers <keyspace> <table>");
            System.exit(1);
        }
        String keyspace = args[args.length - 2];
        String columnfamily = args[args.length - 1];
        DatabaseDescriptor.loadSchemas();

        CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnfamily);
        if (metadata == null)
            throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s",
                    keyspace,
                    columnfamily));

        Keyspace ks = Keyspace.openWithoutSSTables(keyspace);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(columnfamily);
        Directories.SSTableLister lister = cfs.directories.sstableLister().skipTemporary(true);
        Set<SSTableReader> sstables = new HashSet<>();
        for (Map.Entry<Descriptor, Set<Component>> sstable : lister.list().entrySet())
        {
            if (sstable.getKey() != null)
            {
                try
                {
                    SSTableReader reader = SSTableReader.open(sstable.getKey());
                    sstables.add(reader);
                }
                catch (Throwable t)
                {
                    out.println("Couldn't open sstable: " + sstable.getKey().filenameFor(Component.DATA)+" ("+t.getMessage()+")");
                }
            }
        }
        if (sstables.isEmpty())
        {
            out.println("No sstables for " + keyspace + "." + columnfamily);
            System.exit(1);
        }

        int gcBefore = (int)(System.currentTimeMillis()/1000) - metadata.getGcGraceSeconds();
        Multimap<SSTableReader, SSTableReader> blockers = checkForExpiredSSTableBlockers(sstables, gcBefore);
        for (SSTableReader blocker : blockers.keySet())
        {
            out.println(String.format("%s blocks %d expired sstables from getting dropped: %s%n",
                                    formatForExpiryTracing(Collections.singleton(blocker)),
                                    blockers.get(blocker).size(),
                                    formatForExpiryTracing(blockers.get(blocker))));
        }

        System.exit(0);
    }

    public static Multimap<SSTableReader, SSTableReader> checkForExpiredSSTableBlockers(Iterable<SSTableReader> sstables, int gcBefore)
    {
        Multimap<SSTableReader, SSTableReader> blockers = ArrayListMultimap.create();
        for (SSTableReader sstable : sstables)
        {
            if (sstable.getSSTableMetadata().maxLocalDeletionTime < gcBefore)
            {
                for (SSTableReader potentialBlocker : sstables)
                {
                    if (!potentialBlocker.equals(sstable) &&
                        potentialBlocker.getMinTimestamp() <= sstable.getMaxTimestamp() &&
                        potentialBlocker.getSSTableMetadata().maxLocalDeletionTime > gcBefore)
                        blockers.put(potentialBlocker, sstable);
                }
            }
        }
        return blockers;
    }

    private static String formatForExpiryTracing(Iterable<SSTableReader> sstables)
    {
        StringBuilder sb = new StringBuilder();

        for (SSTableReader sstable : sstables)
            sb.append(String.format("[%s (minTS = %d, maxTS = %d, maxLDT = %d)]", sstable, sstable.getMinTimestamp(), sstable.getMaxTimestamp(), sstable.getSSTableMetadata().maxLocalDeletionTime)).append(", ");

        return sb.toString();
    }
}
