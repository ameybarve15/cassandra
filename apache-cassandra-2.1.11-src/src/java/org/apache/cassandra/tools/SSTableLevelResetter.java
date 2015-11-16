
/**
 * Reset level to 0 on a given set of sstables
 */
public class SSTableLevelResetter
{
    /**
     * @param args a list of sstables whose metadata we are changing
     */
    public static void main(String[] args) throws IOException
    {
        PrintStream out = System.out;
        if (args.length == 0)
        {
            out.println("This command should be run with Cassandra stopped!");
            out.println("Usage: sstablelevelreset <keyspace> <columnfamily>");
            System.exit(1);
        }

        if (!args[0].equals("--really-reset") || args.length != 3)
        {
            out.println("This command should be run with Cassandra stopped, otherwise you will get very strange behavior");
            out.println("Verify that Cassandra is not running and then execute the command like this:");
            out.println("Usage: sstablelevelreset --really-reset <keyspace> <columnfamily>");
            System.exit(1);
        }

        // TODO several daemon threads will run from here.
        // So we have to explicitly call System.exit.
        try
        {
            // load keyspace descriptions.
            DatabaseDescriptor.loadSchemas();

            String keyspaceName = args[1];
            String columnfamily = args[2];
            // validate columnfamily
            if (Schema.instance.getCFMetaData(keyspaceName, columnfamily) == null)
            {
                System.err.println("ColumnFamily not found: " + keyspaceName + "/" + columnfamily);
                System.exit(1);
            }

            Keyspace keyspace = Keyspace.openWithoutSSTables(keyspaceName);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnfamily);
            boolean foundSSTable = false;
            for (Map.Entry<Descriptor, Set<Component>> sstable : cfs.directories.sstableLister().list().entrySet())
            {
                if (sstable.getValue().contains(Component.STATS))
                {
                    foundSSTable = true;
                    Descriptor descriptor = sstable.getKey();
                    StatsMetadata metadata = (StatsMetadata) descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.STATS);
                    if (metadata.sstableLevel > 0)
                    {
                        out.println("Changing level from " + metadata.sstableLevel + " to 0 on " + descriptor.filenameFor(Component.DATA));
                        descriptor.getMetadataSerializer().mutateLevel(descriptor, 0);
                    }
                    else
                    {
                        out.println("Skipped " + descriptor.filenameFor(Component.DATA) + " since it is already on level 0");
                    }
                }
            }

            if (!foundSSTable)
            {
                out.println("Found no sstables, did you give the correct keyspace/columnfamily?");
            }
        }
        
        System.exit(0);
    }
}
