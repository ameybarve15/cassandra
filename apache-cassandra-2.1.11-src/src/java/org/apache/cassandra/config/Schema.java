

public class Schema
{
    private static final Logger logger = LoggerFactory.getLogger(Schema.class);

    public static final Schema instance = new Schema();

    /**
     * longest permissible KS or CF name.  Our main concern is that filename not be more than 255 characters;
     * the filename will contain both the KS and CF names. Since non-schema-name components only take up
     * ~64 characters, we could allow longer names than this, but on Windows, the entire path should be not greater than
     * 255 characters, so a lower limit here helps avoid problems.  See CASSANDRA-4110.
     */
    public static final int NAME_LENGTH = 48;

    /* metadata map for faster keyspace lookup */
    private final Map<String, KSMetaData> keyspaces = new NonBlockingHashMap<String, KSMetaData>();

    /* Keyspace objects, one per keyspace. Only one instance should ever exist for any given keyspace. */
    private final Map<String, Keyspace> keyspaceInstances = new NonBlockingHashMap<String, Keyspace>();

    /* metadata map for faster ColumnFamily lookup */
    private final ConcurrentBiMap<Pair<String, String>, UUID> cfIdMap = new ConcurrentBiMap<>();

    private volatile UUID version;

    // 59adb24e-f3cd-3e02-97f0-5b395827453f
    public static final UUID emptyVersion;
    public static final ImmutableSet<String> systemKeyspaceNames = ImmutableSet.of(Keyspace.SYSTEM_KS);

    static
    {
        emptyVersion = UUID.nameUUIDFromBytes(MessageDigest.getInstance("MD5").digest());
    }

    /**
     * Initialize empty schema object
     */
    public Schema()
    {}

    /**
     * Load up non-system keyspaces
     *
     * @param keyspaceDefs The non-system keyspace definitions
     *
     * @return self to support chaining calls
     */
    public Schema load(Collection<KSMetaData> keyspaceDefs)
    {
        for (KSMetaData def : keyspaceDefs)
            load(def);

        return this;
    }

    /**
     * Load specific keyspace into Schema
     *
     * @param keyspaceDef The keyspace to load up
     *
     * @return self to support chaining calls
     */
    public Schema load(KSMetaData keyspaceDef)
    {
        for (CFMetaData cfm : keyspaceDef.cfMetaData().values())
            load(cfm);

        setKeyspaceDefinition(keyspaceDef);

        return this;
    }

    /**
     * Get keyspace instance by name
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return Keyspace object or null if keyspace was not found
     */
    public Keyspace getKeyspaceInstance(String keyspaceName)
    {
        return keyspaceInstances.get(keyspaceName);
    }

    /**
     * Retrieve a CFS by name even if that CFS is an index
     *
     * An index is identified by looking for '.' in the CF name and separating to find the base table
     * containing the index
     * @param ksNameAndCFName
     * @return The named CFS or null if the keyspace, base table, or index don't exist
     */
    public ColumnFamilyStore getColumnFamilyStoreIncludingIndexes(Pair<String, String> ksNameAndCFName) {
        String ksName = ksNameAndCFName.left;
        String cfName = ksNameAndCFName.right;
        Pair<String, String> baseTable;

        /*
         * Split does special case a one character regex, and it looks like it can detect
         * if you use two characters to escape '.', but it still allocates a useless array.
         */
        int indexOfSeparator = cfName.indexOf('.');
        if (indexOfSeparator > -1)
            baseTable = Pair.create(ksName, cfName.substring(0, indexOfSeparator));
        else
            baseTable = ksNameAndCFName;

        UUID cfId = cfIdMap.get(baseTable);
        if (cfId == null)
            return null;

        Keyspace ks = keyspaceInstances.get(ksName);
        if (ks == null)
            return null;

        ColumnFamilyStore baseCFS = ks.getColumnFamilyStore(cfId);

        //Not an index
        if (indexOfSeparator == -1)
            return baseCFS;

        if (baseCFS == null)
            return null;

        SecondaryIndex index = baseCFS.indexManager.getIndexByName(cfName);
        if (index == null)
            return null;

        return index.getIndexCfs();
    }

    public ColumnFamilyStore getColumnFamilyStoreInstance(UUID cfId)
    {
        Pair<String, String> pair = cfIdMap.inverse().get(cfId);
        if (pair == null)
            return null;
        Keyspace instance = getKeyspaceInstance(pair.left);
        if (instance == null)
            return null;
        return instance.getColumnFamilyStore(cfId);
    }

    /**
     * Store given Keyspace instance to the schema
     *
     * @param keyspace The Keyspace instance to store
     *
     * @throws IllegalArgumentException if Keyspace is already stored
     */
    public void storeKeyspaceInstance(Keyspace keyspace)
    {
        if (keyspaceInstances.containsKey(keyspace.getName()))
            throw new IllegalArgumentException(String.format("Keyspace %s was already initialized.", keyspace.getName()));

        keyspaceInstances.put(keyspace.getName(), keyspace);
    }

    /**
     * Remove keyspace from schema
     *
     * @param keyspaceName The name of the keyspace to remove
     *
     * @return removed keyspace instance or null if it wasn't found
     */
    public Keyspace removeKeyspaceInstance(String keyspaceName)
    {
        return keyspaceInstances.remove(keyspaceName);
    }

    /**
     * Remove keyspace definition from system
     *
     * @param ksm The keyspace definition to remove
     */
    public void clearKeyspaceDefinition(KSMetaData ksm)
    {
        keyspaces.remove(ksm.name);
    }

    /**
     * Given a keyspace name & column family name, get the column family
     * meta data. If the keyspace name or column family name is not valid
     * this function returns null.
     *
     * @param keyspaceName The keyspace name
     * @param cfName The ColumnFamily name
     *
     * @return ColumnFamily Metadata object or null if it wasn't found
     */
    public CFMetaData getCFMetaData(String keyspaceName, String cfName)
    {
        assert keyspaceName != null;
        KSMetaData ksm = keyspaces.get(keyspaceName);
        return (ksm == null) ? null : ksm.cfMetaData().get(cfName);
    }

    /**
     * Get ColumnFamily metadata by its identifier
     *
     * @param cfId The ColumnFamily identifier
     *
     * @return metadata about ColumnFamily
     */
    public CFMetaData getCFMetaData(UUID cfId)
    {
        Pair<String,String> cf = getCF(cfId);
        return (cf == null) ? null : getCFMetaData(cf.left, cf.right);
    }

    public CFMetaData getCFMetaData(Descriptor descriptor)
    {
        return getCFMetaData(descriptor.ksname, descriptor.cfname);
    }

    /**
     * Get type of the ColumnFamily but it's keyspace/name
     *
     * @param ksName The keyspace name
     * @param cfName The ColumnFamily name
     *
     * @return The type of the ColumnFamily
     */
    public ColumnFamilyType getColumnFamilyType(String ksName, String cfName)
    {
        assert ksName != null && cfName != null;
        CFMetaData cfMetaData = getCFMetaData(ksName, cfName);
        return (cfMetaData == null) ? null : cfMetaData.cfType;
    }

    /**
     * Get metadata about keyspace by its name
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return The keyspace metadata or null if it wasn't found
     */
    public KSMetaData getKSMetaData(String keyspaceName)
    {
        assert keyspaceName != null;
        return keyspaces.get(keyspaceName);
    }

    /**
     * @return collection of the non-system keyspaces
     */
    public List<String> getNonSystemKeyspaces()
    {
        return ImmutableList.copyOf(Sets.difference(keyspaces.keySet(), systemKeyspaceNames));
    }

    /**
     * Get metadata about keyspace inner ColumnFamilies
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return metadata about ColumnFamilies the belong to the given keyspace
     */
    public Map<String, CFMetaData> getKeyspaceMetaData(String keyspaceName)
    {
        assert keyspaceName != null;
        KSMetaData ksm = keyspaces.get(keyspaceName);
        assert ksm != null;
        return ksm.cfMetaData();
    }

    /**
     * @return collection of the all keyspace names registered in the system (system and non-system)
     */
    public Set<String> getKeyspaces()
    {
        return keyspaces.keySet();
    }

    /**
     * @return collection of the metadata about all keyspaces registered in the system (system and non-system)
     */
    public Collection<KSMetaData> getKeyspaceDefinitions()
    {
        return keyspaces.values();
    }

    /**
     * Update (or insert) new keyspace definition
     *
     * @param ksm The metadata about keyspace
     */
    public void setKeyspaceDefinition(KSMetaData ksm)
    {
        assert ksm != null;
        keyspaces.put(ksm.name, ksm);
    }

    /* ColumnFamily query/control methods */

    /**
     * @param cfId The identifier of the ColumnFamily to lookup
     * @return The (ksname,cfname) pair for the given id, or null if it has been dropped.
     */
    public Pair<String,String> getCF(UUID cfId)
    {
        return cfIdMap.inverse().get(cfId);
    }

    /**
     * @param ksAndCFName The identifier of the ColumnFamily to lookup
     * @return true if the KS and CF pair is a known one, false otherwise.
     */
    public boolean hasCF(Pair<String, String> ksAndCFName)
    {
        return cfIdMap.containsKey(ksAndCFName);
    }

    /**
     * Lookup keyspace/ColumnFamily identifier
     *
     * @param ksName The keyspace name
     * @param cfName The ColumnFamily name
     *
     * @return The id for the given (ksname,cfname) pair, or null if it has been dropped.
     */
    public UUID getId(String ksName, String cfName)
    {
        return cfIdMap.get(Pair.create(ksName, cfName));
    }

    /**
     * Load individual ColumnFamily Definition to the schema
     * (to make ColumnFamily lookup faster)
     *
     * @param cfm The ColumnFamily definition to load
     */
    public void load(CFMetaData cfm)
    {
        Pair<String, String> key = Pair.create(cfm.ksName, cfm.cfName);

        if (cfIdMap.containsKey(key))
            throw new RuntimeException(String.format("Attempting to load already loaded column family %s.%s", cfm.ksName, cfm.cfName));

        logger.debug("Adding {} to cfIdMap", cfm);
        cfIdMap.put(key, cfm.cfId);
    }

    /**
     * Used for ColumnFamily data eviction out from the schema
     *
     * @param cfm The ColumnFamily Definition to evict
     */
    public void purge(CFMetaData cfm)
    {
        cfIdMap.remove(Pair.create(cfm.ksName, cfm.cfName));
        cfm.markPurged();
    }

    /* Version control */

    /**
     * @return current schema version
     */
    public UUID getVersion()
    {
        return version;
    }

    /**
     * Read schema from system keyspace and calculate MD5 digest of every row, resulting digest
     * will be converted into UUID which would act as content-based version of the schema.
     */
    public void updateVersion()
    {
        try
        {
            MessageDigest versionDigest = MessageDigest.getInstance("MD5");

            for (Row row : SystemKeyspace.serializedSchema())
            {
                if (invalidSchemaRow(row) || ignoredSchemaRow(row))
                    continue;

                // we want to digest only live columns
                ColumnFamilyStore.removeDeletedColumnsOnly(row.cf, Integer.MAX_VALUE, SecondaryIndexManager.nullUpdater);
                row.cf.purgeTombstones(Integer.MAX_VALUE);
                row.cf.updateDigest(versionDigest);
            }

            version = UUID.nameUUIDFromBytes(versionDigest.digest());
            SystemKeyspace.updateSchemaVersion(version);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /*
     * Like updateVersion, but also announces via gossip
     */
    public void updateVersionAndAnnounce()
    {
        updateVersion();
        MigrationManager.passiveAnnounce(version);
    }

    /**
     * Clear all KS/CF metadata and reset version.
     */
    public synchronized void clear()
    {
        for (String keyspaceName : getNonSystemKeyspaces())
        {
            KSMetaData ksm = getKSMetaData(keyspaceName);
            for (CFMetaData cfm : ksm.cfMetaData().values())
                purge(cfm);
            clearKeyspaceDefinition(ksm);
        }

        updateVersionAndAnnounce();
    }

    public static boolean invalidSchemaRow(Row row)
    {
        return row.cf == null || (row.cf.isMarkedForDelete() && !row.cf.hasColumns());
    }

    public static boolean ignoredSchemaRow(Row row)
    {
        try
        {
            return systemKeyspaceNames.contains(ByteBufferUtil.string(row.key.getKey()));
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }
}
