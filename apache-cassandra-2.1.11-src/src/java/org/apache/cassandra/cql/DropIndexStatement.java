

public class DropIndexStatement
{
    public final String indexName;
    private String keyspace;

    public DropIndexStatement(String indexName)
    {
        this.indexName = indexName;
    }

    public void setKeyspace(String keyspace)
    {
        this.keyspace = keyspace;
    }

    public String getColumnFamily() throws InvalidRequestException
    {
        return findIndexedCF().cfName;
    }

    public CFMetaData generateCFMetadataUpdate() throws InvalidRequestException
    {
        return updateCFMetadata(findIndexedCF());
    }

    private CFMetaData updateCFMetadata(CFMetaData cfm)
    {
        ColumnDefinition column = findIndexedColumn(cfm);
        assert column != null;
        CFMetaData cloned = cfm.copy();
        ColumnDefinition toChange = cloned.getColumnDefinition(column.name);
        assert toChange.getIndexName() != null && toChange.getIndexName().equals(indexName);
        toChange.setIndexName(null);
        toChange.setIndexType(null, null);
        return cloned;
    }

    private CFMetaData findIndexedCF() throws InvalidRequestException
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(keyspace);
        for (CFMetaData cfm : ksm.cfMetaData().values())
        {
            if (findIndexedColumn(cfm) != null)
                return cfm;
        }
        throw new InvalidRequestException("Index '" + indexName + "' could not be found in any of the column families of keyspace '" + keyspace + "'");
    }

    private ColumnDefinition findIndexedColumn(CFMetaData cfm)
    {
        for (ColumnDefinition column : cfm.regularColumns())
        {
            if (column.getIndexType() != null && column.getIndexName() != null && column.getIndexName().equals(indexName))
                return column;
        }
        return null;
    }
}
