

public class CFPropDefs {
    private static final Logger logger = LoggerFactory.getLogger(CFPropDefs.class);

    public static final String KW_COMPARATOR = "comparator";
    public static final String KW_COMMENT = "comment";
    public static final String KW_READREPAIRCHANCE = "read_repair_chance";
    public static final String KW_DCLOCALREADREPAIRCHANCE = "dclocal_read_repair_chance";
    public static final String KW_GCGRACESECONDS = "gc_grace_seconds";
    public static final String KW_DEFAULTVALIDATION = "default_validation";
    public static final String KW_MINCOMPACTIONTHRESHOLD = "min_compaction_threshold";
    public static final String KW_MAXCOMPACTIONTHRESHOLD = "max_compaction_threshold";
    public static final String KW_COMPACTION_STRATEGY_CLASS = "compaction_strategy_class";
    public static final String KW_CACHING = "caching";
    public static final String KW_ROWS_PER_PARTITION_TO_CACHE = "rows_per_partition_to_cache";
    public static final String KW_DEFAULT_TIME_TO_LIVE = "default_time_to_live";
    public static final String KW_SPECULATIVE_RETRY = "speculative_retry";
    public static final String KW_BF_FP_CHANCE = "bloom_filter_fp_chance";
    public static final String KW_MEMTABLE_FLUSH_PERIOD = "memtable_flush_period_in_ms";

    // Maps CQL short names to the respective Cassandra comparator/validator class names
    public static final Map<String, String> comparators = new HashMap<String, String>();
    public static final Set<String> keywords = new HashSet<String>();
    public static final Set<String> obsoleteKeywords = new HashSet<String>();
    public static final Set<String> allowedKeywords = new HashSet<String>();

    public static final String COMPACTION_OPTIONS_PREFIX = "compaction_strategy_options";
    public static final String COMPRESSION_PARAMETERS_PREFIX = "compression_parameters";

    static
    {
        comparators.put("ascii", "AsciiType");
        comparators.put("bigint", "LongType");
        comparators.put("blob", "BytesType");
        comparators.put("boolean", "BooleanType");
        comparators.put("counter", "CounterColumnType");
        comparators.put("decimal", "DecimalType");
        comparators.put("double", "DoubleType");
        comparators.put("float", "FloatType");
        comparators.put("int", "Int32Type");
        comparators.put("text", "UTF8Type");
        comparators.put("timestamp", "DateType");
        comparators.put("uuid", "UUIDType");
        comparators.put("varchar", "UTF8Type");
        comparators.put("varint", "IntegerType");

        keywords.add(KW_COMPARATOR);
        keywords.add(KW_COMMENT);
        keywords.add(KW_READREPAIRCHANCE);
        keywords.add(KW_DCLOCALREADREPAIRCHANCE);
        keywords.add(KW_GCGRACESECONDS);
        keywords.add(KW_DEFAULTVALIDATION);
        keywords.add(KW_MINCOMPACTIONTHRESHOLD);
        keywords.add(KW_MAXCOMPACTIONTHRESHOLD);
        keywords.add(KW_COMPACTION_STRATEGY_CLASS);
        keywords.add(KW_CACHING);
        keywords.add(KW_ROWS_PER_PARTITION_TO_CACHE);
        keywords.add(KW_DEFAULT_TIME_TO_LIVE);
        keywords.add(KW_SPECULATIVE_RETRY);
        keywords.add(KW_BF_FP_CHANCE);
        keywords.add(KW_MEMTABLE_FLUSH_PERIOD);

        obsoleteKeywords.add("row_cache_size");
        obsoleteKeywords.add("key_cache_size");
        obsoleteKeywords.add("row_cache_save_period_in_seconds");
        obsoleteKeywords.add("key_cache_save_period_in_seconds");
        obsoleteKeywords.add("memtable_throughput_in_mb");
        obsoleteKeywords.add("memtable_operations_in_millions");
        obsoleteKeywords.add("memtable_flush_after_mins");
        obsoleteKeywords.add("row_cache_provider");
        obsoleteKeywords.add("replicate_on_write");
        obsoleteKeywords.add("populate_io_cache_on_flush");

        allowedKeywords.addAll(keywords);
        allowedKeywords.addAll(obsoleteKeywords);
    }

    public final Map<String, String> properties = new HashMap<String, String>();
    public Class<? extends AbstractCompactionStrategy> compactionStrategyClass;
    public final Map<String, String> compactionStrategyOptions = new HashMap<String, String>();
    public final Map<String, String> compressionParameters = new HashMap<String, String>();

    public void validate() throws InvalidRequestException, ConfigurationException
    {
        compactionStrategyClass = CFMetaData.DEFAULT_COMPACTION_STRATEGY_CLASS;

        // we need to remove parent:key = value pairs from the main properties
        Set<String> propsToRemove = new HashSet<String>();

        // check if we have compaction/compression options
        for (String property : properties.keySet())
        {
            if (!property.contains(":"))
                continue;

            String key = property.split(":")[1];
            String val = properties.get(property);

            if (property.startsWith(COMPACTION_OPTIONS_PREFIX))
            {
                compactionStrategyOptions.put(key, val);
                propsToRemove.add(property);
            }

            if (property.startsWith(COMPRESSION_PARAMETERS_PREFIX))
            {
                compressionParameters.put(key, val);
                propsToRemove.add(property);
            }
        }

        for (String property : propsToRemove)
            properties.remove(property);
        // Catch the case where someone passed a kwarg that is not recognized.
        for (String bogus : Sets.difference(properties.keySet(), allowedKeywords))
            throw new InvalidRequestException(bogus + " is not a valid keyword argument for CREATE COLUMNFAMILY");
        for (String obsolete : Sets.intersection(properties.keySet(), obsoleteKeywords))
            logger.warn("Ignoring obsolete property {}", obsolete);

        // Validate min/max compaction thresholds
        Integer minCompaction = getPropertyInt(KW_MINCOMPACTIONTHRESHOLD, null);
        Integer maxCompaction = getPropertyInt(KW_MAXCOMPACTIONTHRESHOLD, null);

        if ((minCompaction != null) && (maxCompaction != null))     // Both min and max are set
        {
            if ((minCompaction > maxCompaction) && (maxCompaction != 0))
                throw;
        }
        else if (minCompaction != null)     // Only the min threshold is set
        {
            if (minCompaction > CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD)
                throw ;
        }
        else if (maxCompaction != null)     // Only the max threshold is set
        {
            if ((maxCompaction < CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD) && (maxCompaction != 0))
                throw;
        }

        Integer defaultTimeToLive = getPropertyInt(KW_DEFAULT_TIME_TO_LIVE, null);

        if (defaultTimeToLive != null)
        {
            if (defaultTimeToLive < 0)
                throw;
        }

        CFMetaData.validateCompactionOptions(compactionStrategyClass, compactionStrategyOptions);
    }

    /** Map a keyword to the corresponding value */
    public void addProperty(String name, String value)
    {
        properties.put(name, value);
    }

    public Boolean hasProperty(String name)
    {
        return properties.containsKey(name);
    }

    /* If not comparator/validator is not specified, default to text (BytesType is the wrong default for CQL
     * since it uses hex terms).  If the value specified is not found in the comparators map, assume the user
     * knows what they are doing (a custom comparator/validator for example), and pass it on as-is.
     */

    public AbstractType<?> getComparator() throws ConfigurationException, SyntaxException
    {
        return TypeParser.parse((comparators.get(getPropertyString(KW_COMPARATOR, "text")) != null)
                                  ? comparators.get(getPropertyString(KW_COMPARATOR, "text"))
                                  : getPropertyString(KW_COMPARATOR, "text"));
    }

    public AbstractType<?> getValidator() throws ConfigurationException, SyntaxException
    {
        return TypeParser.parse((comparators.get(getPropertyString(KW_DEFAULTVALIDATION, "text")) != null)
                                  ? comparators.get(getPropertyString(KW_DEFAULTVALIDATION, "text"))
                                  : getPropertyString(KW_DEFAULTVALIDATION, "text"));
    }

    public String getProperty(String name)
    {
        return properties.get(name);
    }

    public String getPropertyString(String key, String defaultValue)
    {
        String value = properties.get(key);
        return value != null ? value : defaultValue;
    }

    // Return a property value, typed as a Boolean
    public Boolean getPropertyBoolean(String key, Boolean defaultValue)
    {
        String value = properties.get(key);
        return (value == null) ? defaultValue : value.toLowerCase().matches("(1|true|yes)");
    }

    // Return a property value, typed as a Double
    public Double getPropertyDouble(String key, Double defaultValue) throws InvalidRequestException
    {
        Double result;
        String value = properties.get(key);

        if (value == null)
            result = defaultValue;
        else
        {
            result = Double.valueOf(value);
        }
        return result;
    }

    // Return a property value, typed as an Integer
    public Integer getPropertyInt(String key, Integer defaultValue) throws InvalidRequestException
    {
        Integer result;
        String value = properties.get(key);

        if (value == null)
            result = defaultValue;
        else
        {
            result = Integer.valueOf(value);
        }
        return result;
    }

    public Set<String> getPropertySet(String key, Set<String> defaultValue)
    {
        String value = properties.get(key);
        if (Strings.isNullOrEmpty(value))
            return defaultValue;
        return Sets.newHashSet(StringUtils.split(value, ','));
    }
}
