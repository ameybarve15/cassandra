

public class AlterTableStatement
{
    public static enum OperationType
    {
        ADD, ALTER, DROP, OPTS
    }

    public final OperationType oType;
    public final String columnFamily, columnName, validator;
    private final CFPropDefs cfProps = new CFPropDefs();

    public AlterTableStatement(String columnFamily, OperationType type, String columnName)
    {
        this(columnFamily, type, columnName, null);
    }

    public AlterTableStatement(String columnFamily, OperationType type, String columnName, String validator)
    {
        this(columnFamily, type, columnName, validator, null);
    }

    public AlterTableStatement(String columnFamily, OperationType type, String columnName, String validator, Map<String, String> propertyMap)
    {
        this.columnFamily = columnFamily;
        this.oType = type;
        this.columnName = columnName;
        this.validator = CFPropDefs.comparators.get(validator); // used only for ADD/ALTER commands

        if (propertyMap != null)
        {
            for (Map.Entry<String, String> prop : propertyMap.entrySet())
            {
                cfProps.addProperty(prop.getKey(), prop.getValue());
            }
        }
    }

    public CFMetaData getCFMetaData(String keyspace) throws ConfigurationException, InvalidRequestException, SyntaxException
    {
        CFMetaData meta = Schema.instance.getCFMetaData(keyspace, columnFamily);
        CFMetaData cfm = meta.copy();

        ByteBuffer columnName = this.oType == OperationType.OPTS ? null
                                                                 : meta.comparator.subtype(0).fromStringCQL2(this.columnName);

        switch (oType)
        {
            case ADD:
                cfm.addColumnDefinition(ColumnDefinition.regularDef(cfm, columnName, TypeParser.parse(validator), null));
                break;

            case ALTER:
                // We only look for the first key alias which is ok for CQL2
                ColumnDefinition partionKeyDef = cfm.partitionKeyColumns().get(0);
                if (partionKeyDef.name.bytes.equals(columnName))
                {
                    cfm.keyValidator(TypeParser.parse(validator));
                }
                else
                {
                    ColumnDefinition toUpdate = null;

                    for (ColumnDefinition columnDef : cfm.regularColumns())
                    {
                        if (columnDef.name.bytes.equals(columnName))
                        {
                            toUpdate = columnDef;
                            break;
                        }
                    }

                    cfm.addOrReplaceColumnDefinition(toUpdate.withNewType(TypeParser.parse(validator)));
                }
                break;

            case DROP:
                ColumnDefinition toDelete = null;

                for (ColumnDefinition columnDef : cfm.regularColumns())
                {
                    if (columnDef.name.bytes.equals(columnName))
                    {
                        toDelete = columnDef;
                    }
                }

                cfm.removeColumnDefinition(toDelete);
                break;

            case OPTS:
                cfProps.validate();
                applyPropertiesToCFMetadata(cfm, cfProps);
                break;
        }

        return cfm;
    }


    public static void applyPropertiesToCFMetadata(CFMetaData cfm, CFPropDefs cfProps) throws InvalidRequestException, ConfigurationException
    {
        if (cfProps.hasProperty(CFPropDefs.KW_COMPACTION_STRATEGY_CLASS))
            cfm.compactionStrategyClass(cfProps.compactionStrategyClass);

        if (cfProps.hasProperty(CFPropDefs.KW_COMPARATOR))
            throw new InvalidRequestException("Can't change CF comparator after creation");

        if (cfProps.hasProperty(CFPropDefs.KW_COMMENT))
            cfm.comment(cfProps.getProperty(CFPropDefs.KW_COMMENT));

        if (cfProps.hasProperty(CFPropDefs.KW_DEFAULTVALIDATION))
        {
            try
            {
                cfm.defaultValidator(cfProps.getValidator());
            }
            catch (RequestValidationException e)
            {
                throw new InvalidRequestException(String.format("Invalid validation type %s",
                                                                cfProps.getProperty(CFPropDefs.KW_DEFAULTVALIDATION)));
            }
        }

        cfm.readRepairChance(cfProps.getPropertyDouble(CFPropDefs.KW_READREPAIRCHANCE, cfm.getReadRepairChance()));
        cfm.dcLocalReadRepairChance(cfProps.getPropertyDouble(CFPropDefs.KW_DCLOCALREADREPAIRCHANCE, cfm.getDcLocalReadRepair()));
        cfm.gcGraceSeconds(cfProps.getPropertyInt(CFPropDefs.KW_GCGRACESECONDS, cfm.getGcGraceSeconds()));
        int minCompactionThreshold = cfProps.getPropertyInt(CFPropDefs.KW_MINCOMPACTIONTHRESHOLD, cfm.getMinCompactionThreshold());
        int maxCompactionThreshold = cfProps.getPropertyInt(CFPropDefs.KW_MAXCOMPACTIONTHRESHOLD, cfm.getMaxCompactionThreshold());
        if (minCompactionThreshold <= 0 || maxCompactionThreshold <= 0)
            throw new ConfigurationException("Disabling compaction by setting compaction thresholds to 0 has been deprecated, set the compaction option 'enabled' to false instead.");
        cfm.minCompactionThreshold(minCompactionThreshold);
        cfm.maxCompactionThreshold(maxCompactionThreshold);
        cfm.caching(CachingOptions.fromString(cfProps.getPropertyString(CFPropDefs.KW_CACHING, cfm.getCaching().toString())));
        cfm.defaultTimeToLive(cfProps.getPropertyInt(CFPropDefs.KW_DEFAULT_TIME_TO_LIVE, cfm.getDefaultTimeToLive()));
        cfm.speculativeRetry(CFMetaData.SpeculativeRetry.fromString(cfProps.getPropertyString(CFPropDefs.KW_SPECULATIVE_RETRY, cfm.getSpeculativeRetry().toString())));
        cfm.bloomFilterFpChance(cfProps.getPropertyDouble(CFPropDefs.KW_BF_FP_CHANCE, cfm.getBloomFilterFpChance()));
        cfm.memtableFlushPeriod(cfProps.getPropertyInt(CFPropDefs.KW_MEMTABLE_FLUSH_PERIOD, cfm.getMemtableFlushPeriod()));

        if (!cfProps.compactionStrategyOptions.isEmpty())
        {
            cfm.compactionStrategyOptions(new HashMap<String, String>());
            for (Map.Entry<String, String> entry : cfProps.compactionStrategyOptions.entrySet())
                cfm.compactionStrategyOptions.put(entry.getKey(), entry.getValue());
        }

        if (!cfProps.compressionParameters.isEmpty())
        {
            cfm.compressionParameters(CompressionParameters.create(cfProps.compressionParameters));
        }
    }
}
