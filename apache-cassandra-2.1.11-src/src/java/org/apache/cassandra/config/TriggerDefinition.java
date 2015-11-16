

public class TriggerDefinition
{
    private static final String TRIGGER_NAME = "trigger_name";
    private static final String TRIGGER_OPTIONS = "trigger_options";
    private static final String CLASS = "class";

    public final String name;

    // For now, the only supported option is 'class'.
    // Proper trigger parametrization will be added later.
    public final String classOption;

    TriggerDefinition(String name, String classOption)
    {
        this.name = name;
        this.classOption = classOption;
    }

    public static TriggerDefinition create(String name, String classOption)
    {
        return new TriggerDefinition(name, classOption);
    }

    /**
     * Deserialize triggers from storage-level representation.
     *
     * @param serializedTriggers storage-level partition containing the trigger definitions
     * @return the list of processed TriggerDefinitions
     */
    public static List<TriggerDefinition> fromSchema(Row serializedTriggers)
    {
        List<TriggerDefinition> triggers = new ArrayList<>();
        String query = String.format("SELECT * FROM %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.SCHEMA_TRIGGERS_CF);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, serializedTriggers))
        {
            String name = row.getString(TRIGGER_NAME);
            String classOption = row.getMap(TRIGGER_OPTIONS, UTF8Type.instance, UTF8Type.instance).get(CLASS);
            triggers.add(new TriggerDefinition(name, classOption));
        }
        return triggers;
    }

    /**
     * Add specified trigger to the schema using given mutation.
     *
     * @param mutation  The schema mutation
     * @param cfName    The name of the parent ColumnFamily
     * @param timestamp The timestamp to use for the columns
     */
    public void toSchema(Mutation mutation, String cfName, long timestamp)
    {
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SCHEMA_TRIGGERS_CF);

        CFMetaData cfm = CFMetaData.SchemaTriggersCf;
        Composite prefix = cfm.comparator.make(cfName, name);
        CFRowAdder adder = new CFRowAdder(cf, prefix, timestamp);

        adder.addMapEntry(TRIGGER_OPTIONS, CLASS, classOption);
    }

    /**
     * Drop specified trigger from the schema using given mutation.
     *
     * @param mutation  The schema mutation
     * @param cfName    The name of the parent ColumnFamily
     * @param timestamp The timestamp to use for the tombstone
     */
    public void deleteFromSchema(Mutation mutation, String cfName, long timestamp)
    {
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SCHEMA_TRIGGERS_CF);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        Composite prefix = CFMetaData.SchemaTriggersCf.comparator.make(cfName, name);
        cf.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));
    }

    public static TriggerDefinition fromThrift(TriggerDef thriftDef)
    {
        return new TriggerDefinition(thriftDef.getName(), thriftDef.getOptions().get(CLASS));
    }

    public TriggerDef toThrift()
    {
        TriggerDef td = new TriggerDef();
        td.setName(name);
        td.setOptions(Collections.singletonMap(CLASS, classOption));
        return td;
    }

    public static Map<String, TriggerDefinition> fromThrift(List<TriggerDef> thriftDefs)
    {
        Map<String, TriggerDefinition> triggerDefinitions = new HashMap<>();
        for (TriggerDef thriftDef : thriftDefs)
            triggerDefinitions.put(thriftDef.getName(), fromThrift(thriftDef));
        return triggerDefinitions;
    }

    public static List<TriggerDef> toThrift(Map<String, TriggerDefinition> triggers)
    {
        List<TriggerDef> thriftDefs = new ArrayList<>(triggers.size());
        for (TriggerDefinition def : triggers.values())
            thriftDefs.add(def.toThrift());
        return thriftDefs;
    }
}
