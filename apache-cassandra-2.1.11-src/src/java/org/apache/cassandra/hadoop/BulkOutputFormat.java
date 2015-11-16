

public class BulkOutputFormat extends AbstractBulkOutputFormat<ByteBuffer,List<Mutation>>
{
    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public BulkRecordWriter getRecordWriter(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job, String name, org.apache.hadoop.util.Progressable progress) throws IOException
    {
        return new BulkRecordWriter(job, progress);
    }

    @Override
    public BulkRecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new BulkRecordWriter(context);
    }
}
