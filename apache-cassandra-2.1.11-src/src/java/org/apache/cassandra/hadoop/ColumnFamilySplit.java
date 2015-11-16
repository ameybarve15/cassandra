
public class ColumnFamilySplit extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit
{
    private String startToken;
    private String endToken;
    private long length;
    private String[] dataNodes;

    @Deprecated
    public ColumnFamilySplit(String startToken, String endToken, String[] dataNodes)
    {
        this(startToken, endToken, Long.MAX_VALUE, dataNodes);
    }

    public ColumnFamilySplit(String startToken, String endToken, long length, String[] dataNodes)
    {
        assert startToken != null;
        assert endToken != null;
        this.startToken = startToken;
        this.endToken = endToken;
        this.length = length;
        this.dataNodes = dataNodes;
    }

    public String getStartToken()
    {
        return startToken;
    }

    public String getEndToken()
    {
        return endToken;
    }

    // getLength and getLocations satisfy the InputSplit abstraction

    public long getLength()
    {
        return length;
    }

    public String[] getLocations()
    {
        return dataNodes;
    }

    // This should only be used by KeyspaceSplit.read();
    protected ColumnFamilySplit() {}

    // These three methods are for serializing and deserializing
    // KeyspaceSplits as needed by the Writable interface.
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(startToken);
        out.writeUTF(endToken);
        out.writeInt(dataNodes.length);
        for (String endpoint : dataNodes)
        {
            out.writeUTF(endpoint);
        }
    }

    public void readFields(DataInput in) throws IOException
    {
        startToken = in.readUTF();
        endToken = in.readUTF();
        int numOfEndpoints = in.readInt();
        dataNodes = new String[numOfEndpoints];
        for(int i = 0; i < numOfEndpoints; i++)
        {
            dataNodes[i] = in.readUTF();
        }
    }

    @Override
    public String toString()
    {
        return "ColumnFamilySplit(" +
               "(" + startToken
               + ", '" + endToken + ']'
               + " @" + (dataNodes == null ? null : Arrays.asList(dataNodes)) + ')';
    }

    public static ColumnFamilySplit read(DataInput in) throws IOException
    {
        ColumnFamilySplit w = new ColumnFamilySplit();
        w.readFields(in);
        return w;
    }
}
