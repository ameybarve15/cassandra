

public abstract class AbstractBulkOutputFormat<K, V> extends OutputFormat<K, V>
    implements org.apache.hadoop.mapred.OutputFormat<K, V>
{
    @Override
    public void checkOutputSpecs(JobContext context)
    {
        checkOutputSpecs(HadoopCompat.getConfiguration(context));
    }

    private void checkOutputSpecs(Configuration conf)
    {
        if (ConfigHelper.getOutputKeyspace(conf) == null)
        {
            throw new UnsupportedOperationException("you must set the keyspace with setColumnFamily()");
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new NullOutputCommitter();
    }

    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public void checkOutputSpecs(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job) throws IOException
    {
        checkOutputSpecs(job);
    }

    public static class NullOutputCommitter extends OutputCommitter
    {
        public void abortTask(TaskAttemptContext taskContext) { }

        public void cleanupJob(JobContext jobContext) { }

        public void commitTask(TaskAttemptContext taskContext) { }

        public boolean needsTaskCommit(TaskAttemptContext taskContext)
        {
            return false;
        }

        public void setupJob(JobContext jobContext) { }

        public void setupTask(TaskAttemptContext taskContext) { }
    }

    /**
     * Set the hosts to ignore as comma delimited values.
     * Data will not be bulk loaded onto the ignored nodes.
     * @param conf job configuration
     * @param ignoreNodesCsv a comma delimited list of nodes to ignore
     */
    public static void setIgnoreHosts(Configuration conf, String ignoreNodesCsv)
    {
        conf.set(AbstractBulkRecordWriter.IGNORE_HOSTS, ignoreNodesCsv);
    }

    /**
     * Set the hosts to ignore. Data will not be bulk loaded onto the ignored nodes.
     * @param conf job configuration
     * @param ignoreNodes the nodes to ignore
     */
    public static void setIgnoreHosts(Configuration conf, String... ignoreNodes)
    {
        conf.setStrings(AbstractBulkRecordWriter.IGNORE_HOSTS, ignoreNodes);
    }

    /**
     * Get the hosts to ignore as a collection of strings
     * @param conf job configuration
     * @return the nodes to ignore as a collection of stirngs
     */
    public static Collection<String> getIgnoreHosts(Configuration conf)
    {
        return conf.getStringCollection(AbstractBulkRecordWriter.IGNORE_HOSTS);
    }
}
