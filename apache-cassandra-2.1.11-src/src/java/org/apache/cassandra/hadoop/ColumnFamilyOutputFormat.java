
/**
 * The <code>ColumnFamilyOutputFormat</code> acts as a Hadoop-specific
 * OutputFormat that allows reduce tasks to store keys (and corresponding
 * values) as Cassandra rows (and respective columns) in a given
 * ColumnFamily.
 *
 * <p>
 * As is the case with the {@link ColumnFamilyInputFormat}, you need to set the
 * Keyspace and ColumnFamily in your
 * Hadoop job Configuration. The {@link ConfigHelper} class, through its
 * {@link ConfigHelper#setOutputColumnFamily} method, is provided to make this
 * simple.
 * </p>
 *
 * <p>
 * For the sake of performance, this class employs a lazy write-back caching
 * mechanism, where its record writer batches mutations created based on the
 * reduce's inputs (in a task-specific map), and periodically makes the changes
 * official by sending a batch mutate request to Cassandra.
 * </p>
 */
public class ColumnFamilyOutputFormat extends AbstractColumnFamilyOutputFormat<ByteBuffer,List<Mutation>>
{
    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public ColumnFamilyRecordWriter getRecordWriter(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job, String name, org.apache.hadoop.util.Progressable progress)
    {
        return new ColumnFamilyRecordWriter(job, progress);
    }

    /**
     * Get the {@link RecordWriter} for the given task.
     *
     * @param context
     *            the information about the current task.
     * @return a {@link RecordWriter} to write the output for the job.
     * @throws IOException
     */
    public ColumnFamilyRecordWriter getRecordWriter(final TaskAttemptContext context) throws InterruptedException
    {
        return new ColumnFamilyRecordWriter(context);
    }
}
