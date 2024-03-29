

/**
 * Defined (and loaded) user types.
 *
 * In practice, because user types are global, we have only one instance of
 * this class that retrieve through the Schema class.
 */
public final class UTMetaData
{
    private final Map<ByteBuffer, UserType> userTypes;

    public UTMetaData()
    {
        this(new HashMap<ByteBuffer, UserType>());
    }

    UTMetaData(Map<ByteBuffer, UserType> types)
    {
        this.userTypes = types;
    }

    private static UserType fromSchema(UntypedResultSet.Row row)
    {
        String keyspace = row.getString("keyspace_name");
        ByteBuffer name = ByteBufferUtil.bytes(row.getString("type_name"));
        List<String> rawColumns = row.getList("field_names", UTF8Type.instance);
        List<String> rawTypes = row.getList("field_types", UTF8Type.instance);

        List<ByteBuffer> columns = new ArrayList<>(rawColumns.size());
        for (String rawColumn : rawColumns)
            columns.add(ByteBufferUtil.bytes(rawColumn));

        List<AbstractType<?>> types = new ArrayList<>(rawTypes.size());
        for (String rawType : rawTypes)
            types.add(TypeParser.parse(rawType));

        return new UserType(keyspace, name, columns, types);
    }

    public static Map<ByteBuffer, UserType> fromSchema(Row row)
    {
        UntypedResultSet results = QueryProcessor.resultify("SELECT * FROM system." + SystemKeyspace.SCHEMA_USER_TYPES_CF, row);
        Map<ByteBuffer, UserType> types = new HashMap<>(results.size());
        for (UntypedResultSet.Row result : results)
        {
            UserType type = fromSchema(result);
            types.put(type.name, type);
        }
        return types;
    }

    public static Mutation toSchema(UserType newType, long timestamp)
    {
        return toSchema(new Mutation(Keyspace.SYSTEM_KS, SystemKeyspace.getSchemaKSKey(newType.keyspace)), newType, timestamp);
    }

    public static Mutation toSchema(Mutation mutation, UserType newType, long timestamp)
    {
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SCHEMA_USER_TYPES_CF);

        Composite prefix = CFMetaData.SchemaUserTypesCf.comparator.make(newType.name);
        CFRowAdder adder = new CFRowAdder(cf, prefix, timestamp);

        adder.resetCollection("field_names");
        adder.resetCollection("field_types");

        for (int i = 0; i < newType.size(); i++)
        {
            adder.addListEntry("field_names", newType.fieldName(i));
            adder.addListEntry("field_types", newType.fieldType(i).toString());
        }
        return mutation;
    }

    public Mutation toSchema(Mutation mutation, long timestamp)
    {
        for (UserType ut : userTypes.values())
            toSchema(mutation, ut, timestamp);
        return mutation;
    }

    public static Mutation dropFromSchema(UserType droppedType, long timestamp)
    {
        Mutation mutation = new Mutation(Keyspace.SYSTEM_KS, SystemKeyspace.getSchemaKSKey(droppedType.keyspace));
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SCHEMA_USER_TYPES_CF);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        Composite prefix = CFMetaData.SchemaUserTypesCf.comparator.make(droppedType.name);
        cf.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));

        return mutation;
    }

    public UserType getType(ByteBuffer typeName)
    {
        return userTypes.get(typeName);
    }

    public Map<ByteBuffer, UserType> getAllTypes()
    {
        // Copy to avoid concurrent modification while iterating. Not intended to be called on a criticial path anyway
        return new HashMap<>(userTypes);
    }

    // This is *not* thread safe but is only called in DefsTables that is synchronized.
    public void addType(UserType type)
    {
        UserType old = userTypes.get(type.name);
        assert old == null || type.isCompatibleWith(old);
        userTypes.put(type.name, type);
    }

    // Same remarks than for addType
    public void removeType(UserType type)
    {
        userTypes.remove(type.name);
    }

    public boolean equals(Object that)
    {
        if (!(that instanceof UTMetaData))
            return false;
        return userTypes.equals(((UTMetaData) that).userTypes);
    }
}
