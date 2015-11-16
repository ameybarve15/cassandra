
/**
 * A <code>DELETE</code> parsed from a CQL query statement.
 *
 */
public class DeleteStatement extends AbstractModification
{
    private List<Term> columns;
    private List<Term> keys;

    public DeleteStatement(List<Term> columns, String keyspace, String columnFamily, String keyName, List<Term> keys, Attributes attrs)
    {
        super(keyspace, columnFamily, keyName, attrs);

        this.columns = columns;
        this.keys = keys;
    }

    public List<IMutation> prepareRowMutations(String keyspace, ThriftClientState clientState, List<ByteBuffer> variables)
    throws InvalidRequestException, UnauthorizedException
    {
        return prepareRowMutations(keyspace, clientState, null, variables);
    }

    public List<IMutation> prepareRowMutations(String keyspace, ThriftClientState clientState, Long timestamp, List<ByteBuffer> variables)
    throws InvalidRequestException, UnauthorizedException
    {
        CFMetaData metadata = validateColumnFamily(keyspace, columnFamily);

        clientState.hasColumnFamilyAccess(keyspace, columnFamily, Permission.MODIFY);
        AbstractType<?> keyType = Schema.instance.getCFMetaData(keyspace, columnFamily).getKeyValidator();

        List<IMutation> mutations = new ArrayList<IMutation>(keys.size());

        for (Term key : keys)
            mutations.add(mutationForKey(key.getByteBuffer(keyType, variables), keyspace, timestamp, clientState, variables, metadata));

        return mutations;
    }

    public Mutation mutationForKey(ByteBuffer key, String keyspace, Long timestamp, ThriftClientState clientState, List<ByteBuffer> variables, CFMetaData metadata)
    throws InvalidRequestException
    {
        Mutation mutation = new Mutation(keyspace, key);

        QueryProcessor.validateKeyAlias(metadata, keyName);

        if (columns.size() < 1)
        {
            // No columns, delete the partition
            mutation.delete(columnFamily, (timestamp == null) ? getTimestamp(clientState) : timestamp);
        }
        else
        {
            // Delete specific columns
            AbstractType<?> at = metadata.comparator.asAbstractType();
            for (Term column : columns)
            {
                CellName columnName = metadata.comparator.cellFromByteBuffer(column.getByteBuffer(at, variables));
                validateColumnName(columnName);
                mutation.delete(columnFamily, columnName, (timestamp == null) ? getTimestamp(clientState) : timestamp);
            }
        }

        return mutation;
    }

}
