

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql.CQLStatement;
import org.apache.cassandra.cql.QueryProcessor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.QueryPagers;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SemanticVersion;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.thrift.TException;

public class CassandraServer implements Cassandra.Iface
{
    private final static int COUNT_PAGE_SIZE = 1024;

    private final static List<ColumnOrSuperColumn> EMPTY_COLUMNS = Collections.emptyList();

    private volatile boolean loggedCQL2Warning = false;

    /*
     * RequestScheduler to perform the scheduling of incoming requests
     */
    private final IRequestScheduler requestScheduler;

    public CassandraServer()
    {
        requestScheduler = DatabaseDescriptor.getRequestScheduler();
        registerMetrics();
    }

    public ThriftClientState state()
    {
        return ThriftSessionManager.instance.currentSession();
    }

    protected Map<DecoratedKey, ColumnFamily> readColumnFamily(
            List<ReadCommand> commands, 
            org.apache.cassandra.db.ConsistencyLevel consistency_level, 
            ClientState cState)
    {
        schedule(DatabaseDescriptor.getReadRpcTimeout());
        List<Row> rows = StorageProxy.read(commands, consistency_level, cState);
        release();

       // TODO - Support multiple column families per row, right now row only contains 1 column family
        Map<DecoratedKey, ColumnFamily> columnFamilyKeyMap = new HashMap<DecoratedKey, ColumnFamily>();
        for (Row row: rows)
        {
            columnFamilyKeyMap.put(row.key, row.cf);
        }

        return columnFamilyKeyMap;
    }


    public List<ColumnOrSuperColumn> thriftifyColumns(Collection<Cell> cells, boolean reverseOrder, long now)
    {
        ArrayList<ColumnOrSuperColumn> thriftColumns = new ArrayList<ColumnOrSuperColumn>(cells.size());
        for (Cell cell : cells)
        {
            if (!cell.isLive(now)) continue;

            thriftColumns.add(thriftifyColumnWithName(cell, cell.name().toByteBuffer()));
        }

        // we have to do the reversing here, since internally we pass results around in ColumnFamily
        // objects, which always sort their cells in the "natural" order
        // TODO this is inconvenient for direct users of StorageProxy
        if (reverseOrder) Collections.reverse(thriftColumns);

        return thriftColumns;
    }

    private ColumnOrSuperColumn thriftifyColumnWithName(Cell cell, ByteBuffer newName)
    {
        if (cell instanceof CounterCell)
            return new ColumnOrSuperColumn().setCounter_column(thriftifySubCounter(cell).setName(newName));
        else
            return new ColumnOrSuperColumn().setColumn(thriftifySubColumn(cell).setName(newName));
    }

    private Column thriftifySubColumn(Cell cell)
    {
        Column thrift_column = new Column(cell.name().toByteBuffer())
                                    .setValue(cell.value())
                                    .setTimestamp(cell.timestamp());
        if (cell instanceof ExpiringCell)
        {
            thrift_column.setTtl(((ExpiringCell) cell).getTimeToLive());
        }

        return thrift_column;
    }

    private List<Column> thriftifyColumnsAsColumns(Collection<Cell> cells, long now)
    {
        List<Column> thriftColumns = new ArrayList<Column>(cells.size());
        for (Cell cell : cells)
        {
            if (!cell.isLive(now)) continue;

            thriftColumns.add(thriftifySubColumn(cell));
        }
        return thriftColumns;
    }

    private CounterColumn thriftifySubCounter(Cell cell)
    {
        return new CounterColumn(cell.name().toByteBuffer(), 
                                 CounterContext.instance().total(cell.value()));
    }

    private List<ColumnOrSuperColumn> thriftifySuperColumns(Collection<Cell> cells,
                                                            boolean reverseOrder,
                                                            long now,
                                                            boolean subcolumnsOnly,
                                                            boolean isCounterCF)
    {
        if (subcolumnsOnly)
        {
            ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<ColumnOrSuperColumn>(cells.size());
            for (Cell cell : cells)
            {
                if (!cell.isLive(now))
                    continue;

                thriftSuperColumns.add(thriftifyColumnWithName(cell, SuperColumns.subName(cell.name())));
            }
            if (reverseOrder)
                Collections.reverse(thriftSuperColumns);
            return thriftSuperColumns;
        }
        else
        {
            if (isCounterCF)
                return thriftifyCounterSuperColumns(cells, reverseOrder, now);
            else
                return thriftifySuperColumns(cells, reverseOrder, now);
        }
    }

    private List<ColumnOrSuperColumn> thriftifySuperColumns(Collection<Cell> cells, boolean reverseOrder, long now)
    {
        ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<ColumnOrSuperColumn>(cells.size());
        SuperColumn current = null;
        for (Cell cell : cells)
        {
            if (!cell.isLive(now))
                continue;

            ByteBuffer scName = SuperColumns.scName(cell.name());
            if (current == null || !scName.equals(current.bufferForName()))
            {
                current = new SuperColumn(scName, new ArrayList<Column>());
                thriftSuperColumns.add(new ColumnOrSuperColumn().setSuper_column(current));
            }

            current.getColumns()
                   .add(thriftifySubColumn(cell)
                   .setName(SuperColumns.subName(cell.name())));
        }

        if (reverseOrder)
            Collections.reverse(thriftSuperColumns);

        return thriftSuperColumns;
    }

    private List<ColumnOrSuperColumn> thriftifyCounterSuperColumns(Collection<Cell> cells, boolean reverseOrder, long now)
    {
        ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<ColumnOrSuperColumn>(cells.size());
        CounterSuperColumn current = null;
        for (Cell cell : cells)
        {
            if (!cell.isLive(now))
                continue;

            ByteBuffer scName = SuperColumns.scName(cell.name());
            if (current == null || !scName.equals(current.bufferForName()))
            {
                current = new CounterSuperColumn(scName, new ArrayList<CounterColumn>());
                thriftSuperColumns.add(new ColumnOrSuperColumn().setCounter_super_column(current));
            }
            current.getColumns().add(thriftifySubCounter(cell).setName(SuperColumns.subName(cell.name())));
        }

        if (reverseOrder)
            Collections.reverse(thriftSuperColumns);

        return thriftSuperColumns;
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> getSlice(
        List<ReadCommand> commands, 
        boolean subColumnsOnly, 
        ConsistencyLevel consistency_level, 
        ClientState cState)
    {
        Map<DecoratedKey, ColumnFamily> columnFamilies = readColumnFamily(commands, consistency_level, cState);
        Map<ByteBuffer, List<ColumnOrSuperColumn>> columnFamiliesMap = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
        for (ReadCommand command: commands)
        {
            ColumnFamily cf = columnFamilies.get(StorageService.getPartitioner().decorateKey(command.key));
            boolean reverseOrder = command instanceof SliceFromReadCommand && ((SliceFromReadCommand)command).filter.reversed;
            List<ColumnOrSuperColumn> thriftifiedColumns 
                = thriftifyColumnFamily(cf, subColumnsOnly, reverseOrder, command.timestamp);

            columnFamiliesMap.put(command.key, thriftifiedColumns);
        }

        return columnFamiliesMap;
    }

    private List<ColumnOrSuperColumn> thriftifyColumnFamily(
                ColumnFamily cf, 
                boolean subcolumnsOnly, 
                boolean reverseOrder, 
                long now)
    {
        if (cf == null || !cf.hasColumns())
            return EMPTY_COLUMNS;

        if (cf.metadata().isSuper())
        {
            boolean isCounterCF = cf.metadata().isCounter();
            return thriftifySuperColumns(cf.getSortedColumns(), reverseOrder, now, subcolumnsOnly, isCounterCF);
        }
        else
        {
            return thriftifyColumns(cf.getSortedColumns(), reverseOrder, now);
        }
    }

    public List<ColumnOrSuperColumn> get_slice(
            ByteBuffer key, 
            ColumnParent column_parent, 
            SlicePredicate predicate, 
            ConsistencyLevel consistency_level)
    {
        ClientState cState = state();
        String keyspace = cState.getKeyspace();
        state().hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);

        return getSliceInternal(
                    keyspace, 
                    key, 
                    column_parent, 
                    System.currentTimeMillis(), 
                    predicate, 
                    consistency_level, 
                    cState);
    }

    private List<ColumnOrSuperColumn> getSliceInternal(String keyspace,
                                                       ByteBuffer key,
                                                       ColumnParent column_parent,
                                                       long timestamp,
                                                       SlicePredicate predicate,
                                                       ConsistencyLevel consistency_level,
                                                       ClientState cState)
    {
        return multigetSliceInternal(keyspace, Collections.singletonList(key), 
            column_parent, timestamp, predicate, consistency_level, cState).get(key);
    }

    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, 
            ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    {
        ClientState cState = state();
        String keyspace = cState.getKeyspace();
        cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);

        return multigetSliceInternal(keyspace, keys, column_parent, System.currentTimeMillis(), 
                    predicate, consistency_level, cState);
    }

    private SliceQueryFilter toInternalFilter(CFMetaData metadata, ColumnParent parent, SliceRange range)
    {
        if (metadata.isSuper())
        {
            CellNameType columnType = new SimpleDenseCellNameType(metadata.comparator.subtype(parent.isSetSuper_column() ? 1 : 0));
            Composite start = columnType.fromByteBuffer(range.start);
            Composite finish = columnType.fromByteBuffer(range.finish);
            SliceQueryFilter filter = new SliceQueryFilter(start, finish, range.reversed, range.count);
            return SuperColumns.fromSCSliceFilter(metadata.comparator, parent.bufferForSuper_column(), filter);
        }

        Composite start = metadata.comparator.fromByteBuffer(range.start);
        Composite finish = metadata.comparator.fromByteBuffer(range.finish);
        return new SliceQueryFilter(start, finish, range.reversed, range.count);
    }

    private IDiskAtomFilter toInternalFilter(CFMetaData metadata, ColumnParent parent, SlicePredicate predicate)
    {
        IDiskAtomFilter filter;

        if (predicate.column_names != null)
        {
            if (metadata.isSuper())
            {
                CellNameType columnType = new SimpleDenseCellNameType(metadata.comparator.subtype(parent.isSetSuper_column() ? 1 : 0));
                SortedSet<CellName> s = new TreeSet<>(columnType);
                for (ByteBuffer bb : predicate.column_names)
                    s.add(columnType.cellFromByteBuffer(bb));
                filter = SuperColumns.fromSCNamesFilter(metadata.comparator, parent.bufferForSuper_column(), new NamesQueryFilter(s));
            }
            else
            {
                SortedSet<CellName> s = new TreeSet<CellName>(metadata.comparator);
                for (ByteBuffer bb : predicate.column_names)
                    s.add(metadata.comparator.cellFromByteBuffer(bb));
                filter = new NamesQueryFilter(s);
            }
        }
        else
        {
            filter = toInternalFilter(metadata, parent, predicate.slice_range);
        }
        return filter;
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetSliceInternal(String keyspace,
                                                                             List<ByteBuffer> keys,
                                                                             ColumnParent column_parent,
                                                                             long timestamp,
                                                                             SlicePredicate predicate,
                                                                             ConsistencyLevel consistency_level,
                                                                             ClientState cState)
    {
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        ThriftValidation.validatePredicate(metadata, column_parent, predicate);

        ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
        consistencyLevel.validateForRead(keyspace);

        List<ReadCommand> commands = new ArrayList<ReadCommand>(keys.size());
        IDiskAtomFilter filter = toInternalFilter(metadata, column_parent, predicate);

        for (ByteBuffer key: keys)
        {
            ThriftValidation.validateKey(metadata, key);
            // Note that we should not share a slice filter amongst the command, due to SliceQueryFilter not  being immutable
            // due to its columnCounter used by the lastCounted() method (also see SelectStatement.getSliceCommands)
            commands.add(ReadCommand.create(keyspace, key, column_parent.getColumn_family(), timestamp, filter.cloneShallow()));
        }

        return getSlice(commands, column_parent.isSetSuper_column(), consistencyLevel, cState);
    }

    public ColumnOrSuperColumn get(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level)
    {
        ThriftClientState cState = state();
        String keyspace = cState.getKeyspace();
        cState.hasColumnFamilyAccess(keyspace, column_path.column_family, Permission.SELECT);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_path.column_family);
        ThriftValidation.validateColumnPath(metadata, column_path);
        ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
        consistencyLevel.validateForRead(keyspace);

        ThriftValidation.validateKey(metadata, key);

        IDiskAtomFilter filter;
        if (metadata.isSuper())
        {
            CellNameType columnType = new SimpleDenseCellNameType(metadata.comparator.subtype(column_path.column == null ? 0 : 1));
            SortedSet<CellName> names = new TreeSet<CellName>(columnType);
            names.add(columnType.cellFromByteBuffer(column_path.column == null ? column_path.super_column : column_path.column));
            filter = SuperColumns.fromSCNamesFilter(metadata.comparator, column_path.column == null ? null : column_path.bufferForSuper_column(), new NamesQueryFilter(names));
        }
        else
        {
            SortedSet<CellName> names = new TreeSet<CellName>(metadata.comparator);
            names.add(metadata.comparator.cellFromByteBuffer(column_path.column));
            filter = new NamesQueryFilter(names);
        }

        long now = System.currentTimeMillis();
        ReadCommand command = ReadCommand.create(keyspace, key, column_path.column_family, now, filter);

        Map<DecoratedKey, ColumnFamily> cfamilies = readColumnFamily(Arrays.asList(command), consistencyLevel, cState);

        ColumnFamily cf = cfamilies.get(StorageService.getPartitioner().decorateKey(command.key));

        List<ColumnOrSuperColumn> tcolumns = thriftifyColumnFamily(cf, metadata.isSuper() && column_path.column != null, false, now);
 
        return tcolumns.get(0);
    }

    public int get_count(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    {
        ThriftClientState cState = state();
        String keyspace = cState.getKeyspace();
        cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);
        Keyspace keyspaceName = Keyspace.open(keyspace);
        ColumnFamilyStore cfs = keyspaceName.getColumnFamilyStore(column_parent.column_family);
        long timestamp = System.currentTimeMillis();

        if (predicate.column_names != null)
            return getSliceInternal(keyspace, key, column_parent, timestamp, predicate, consistency_level, cState).size();

        int pageSize;
        // request by page if this is a large row
        if (cfs.getMeanColumns() > 0)
        {
            int averageColumnSize = (int) (cfs.getMeanRowSize() / cfs.getMeanColumns());
            pageSize = Math.min(COUNT_PAGE_SIZE, 4 * 1024 * 1024 / averageColumnSize);
            pageSize = Math.max(2, pageSize);
        }
        else
        {
            pageSize = COUNT_PAGE_SIZE;
        }

        SliceRange sliceRange = predicate.slice_range == null
                              ? new SliceRange(
                                        ByteBufferUtil.EMPTY_BYTE_BUFFER, 
                                        ByteBufferUtil.EMPTY_BYTE_BUFFER, 
                                        false, 
                                        Integer.MAX_VALUE)
                              : predicate.slice_range;
        SliceQueryFilter filter = toInternalFilter(cfs.metadata, column_parent, sliceRange);

        return QueryPagers.countPaged(keyspace,
                                      column_parent.column_family,
                                      key,
                                      filter,
                                      ThriftConversion.fromThrift(consistency_level),
                                      cState,
                                      pageSize,
                                      timestamp);
    }

    private static ByteBuffer getName(ColumnOrSuperColumn cosc)
    {
        return cosc.isSetSuper_column() ? cosc.super_column.name :
                   (cosc.isSetColumn() ? cosc.column.name :
                       (cosc.isSetCounter_column() ? cosc.counter_column.name : 
                            cosc.counter_super_column.name));
    }

    public Map<ByteBuffer, Integer> multiget_count(
            List<ByteBuffer> keys, 
            ColumnParent column_parent, 
            SlicePredicate predicate, 
            ConsistencyLevel consistency_level)
    {
        ThriftClientState cState = state();
        String keyspace = cState.getKeyspace();
        cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);

        Map<ByteBuffer, Integer> counts = new HashMap<ByteBuffer, Integer>();
        Map<ByteBuffer, List<ColumnOrSuperColumn>> columnFamiliesMap = multigetSliceInternal(keyspace,
                                                                                             keys,
                                                                                             column_parent,
                                                                                             System.currentTimeMillis(),
                                                                                             predicate,
                                                                                             consistency_level,
                                                                                             cState);

        for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> cf : columnFamiliesMap.entrySet())
            counts.put(cf.getKey(), cf.getValue().size());
        return counts;
    }

    private void internal_insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level)
    {
        ThriftClientState cState = state();
        String keyspace = cState.getKeyspace();
        cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.MODIFY);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, false);
        {
            ThriftValidation.validateKey(metadata, key);
            ThriftValidation.validateColumnParent(metadata, column_parent);
            ThriftValidation.validateColumnNames(metadata, column_parent, Arrays.asList(column.name));
            ThriftValidation.validateColumnData(metadata, key, column_parent.super_column, column);
        }

        CellName name = metadata.isSuper()
                          ? metadata.comparator.makeCellName(column_parent.super_column, column.name)
                          : metadata.comparator.cellFromByteBuffer(column.name);

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cState.getKeyspace(), column_parent.column_family);
        cf.addColumn(name, column.value, column.timestamp, column.ttl);
        Mutation mutation = new org.apache.cassandra.db.Mutation(cState.getKeyspace(), key, cf);

        doInsert(consistency_level, Arrays.asList(mutation));
    }

    public void insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level)
    {
        internal_insert(key, column_parent, column, consistency_level);
    }

    public CASResult cas(ByteBuffer key,
                         String column_family,
                         List<Column> expected,
                         List<Column> updates,
                         ConsistencyLevel serial_consistency_level,
                         ConsistencyLevel commit_consistency_level)
    {
        try
        {
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_family, Permission.MODIFY);
            // CAS updates can be used to simulate a get request, so should require Permission.SELECT.
            cState.hasColumnFamilyAccess(keyspace, column_family, Permission.SELECT);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_family, false);
            ThriftValidation.validateKey(metadata, key);
            Iterable<ByteBuffer> names = Iterables.transform(updates, new Function<Column, ByteBuffer>()
            {
                public ByteBuffer apply(Column column)
                {
                    return column.name;
                }
            });

            ThriftValidation.validateColumnNames(metadata, new ColumnParent(column_family), names);
            for (Column column : updates)
                ThriftValidation.validateColumnData(metadata, key, null, column);

            CFMetaData cfm = Schema.instance.getCFMetaData(cState.getKeyspace(), column_family);
            ColumnFamily cfUpdates = ArrayBackedSortedColumns.factory.create(cfm);
            for (Column column : updates)
                cfUpdates.addColumn(cfm.comparator.cellFromByteBuffer(column.name), column.value, column.timestamp);

            ColumnFamily cfExpected = ArrayBackedSortedColumns.factory.create(cfm);
            for (Column column : expected)
                cfExpected.addColumn(
                    cfm.comparator.cellFromByteBuffer(column.name), 
                    column.value, 
                    column.timestamp);

            schedule(DatabaseDescriptor.getWriteRpcTimeout());
            ColumnFamily result = StorageProxy.cas(cState.getKeyspace(),
                                                   column_family,
                                                   key,
                                                   new ThriftCASRequest(cfExpected, cfUpdates),
                                                   ThriftConversion.fromThrift(serial_consistency_level),
                                                   ThriftConversion.fromThrift(commit_consistency_level),
                                                   cState);
            return result == null
                 ? new CASResult(true)
                 : new CASResult(false)
                        .setCurrent_values(
                            thriftifyColumnsAsColumns(
                                result.getSortedColumns(), 
                                System.currentTimeMillis()
                            )
                        );
        }
    }

    private List<IMutation> createMutationList(ConsistencyLevel consistency_level,
                                               Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map,
                                               boolean allowCounterMutations)
    {
        List<IMutation> mutations = new ArrayList<>();
        ThriftClientState cState = state();
        String keyspace = cState.getKeyspace();

        for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> mutationEntry: mutation_map.entrySet())
        {
            ByteBuffer key = mutationEntry.getKey();

            // We need to separate mutation for standard cf and counter cf (that will be encapsulated in a
            // CounterMutation) because it doesn't follow the same code path
            Mutation standardMutation = null;
            Mutation counterMutation = null;

            Map<String, List<Mutation>> columnFamilyToMutations = mutationEntry.getValue();
            for (Map.Entry<String, List<Mutation>> columnFamilyMutations : columnFamilyToMutations.entrySet())
            {
                String cfName = columnFamilyMutations.getKey();

                cState.hasColumnFamilyAccess(keyspace, cfName, Permission.MODIFY);

                CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, cfName);
                ThriftValidation.validateKey(metadata, key);

                org.apache.cassandra.db.Mutation mutation;
                if (metadata.isCounter())
                {
                    ThriftConversion.fromThrift(consistency_level).validateCounterForWrite(metadata);
                    counterMutation = counterMutation == null ? new org.apache.cassandra.db.Mutation(keyspace, key) 
                                                                : counterMutation;
                    mutation = counterMutation;
                }
                else
                {
                    standardMutation = standardMutation == null ? new org.apache.cassandra.db.Mutation(keyspace, key) 
                                                                    : standardMutation;
                    mutation = standardMutation;
                }

                for (Mutation m : columnFamilyMutations.getValue())
                {
                    ThriftValidation.validateMutation(metadata, key, m);

                    if (m.deletion != null)
                    {
                        deleteColumnOrSuperColumn(mutation, metadata, m.deletion);
                    }
                    if (m.column_or_supercolumn != null)
                    {
                        addColumnOrSuperColumn(mutation, metadata, m.column_or_supercolumn);
                    }
                }
            }
            if (standardMutation != null && !standardMutation.isEmpty())
                mutations.add(standardMutation);

            if (counterMutation != null && !counterMutation.isEmpty())
            {
                if (allowCounterMutations)
                    mutations.add(new CounterMutation(counterMutation, ThriftConversion.fromThrift(consistency_level)));
            }
        }

        return mutations;
    }

    private void addColumnOrSuperColumn(Mutation mutation, CFMetaData cfm, ColumnOrSuperColumn cosc)
    {
        if (cosc.super_column != null)
        {
            for (Column column : cosc.super_column.columns)
            {
                mutation.add(
                    cfm.cfName, 
                    cfm.comparator.makeCellName(cosc.super_column.name, column.name), 
                    column.value, 
                    column.timestamp, 
                    column.ttl);
            }
        }
        else if (cosc.column != null)
        {
            mutation.add(
                cfm.cfName, 
                cfm.comparator.cellFromByteBuffer(cosc.column.name), 
                cosc.column.value, 
                cosc.column.timestamp, 
                cosc.column.ttl);
        }
        else if (cosc.counter_super_column != null)
        {
            for (CounterColumn column : cosc.counter_super_column.columns)
            {
                mutation.addCounter(
                    cfm.cfName, 
                    cfm.comparator.makeCellName(cosc.counter_super_column.name, column.name), 
                    column.value);
            }
        }
        else // cosc.counter_column != null
        {
            mutation.addCounter(
                cfm.cfName, 
                cfm.comparator.cellFromByteBuffer(cosc.counter_column.name), 
                cosc.counter_column.value);
        }
    }

    private void deleteColumnOrSuperColumn(Mutation mutation, CFMetaData cfm, Deletion del)
    {
        if (del.predicate != null && del.predicate.column_names != null)
        {
            for (ByteBuffer c : del.predicate.column_names)
            {
                if (del.super_column == null && cfm.isSuper())
                    mutation.deleteRange(
                        cfm.cfName, 
                        SuperColumns.startOf(c), 
                        SuperColumns.endOf(c), 
                        del.timestamp);
                else if (del.super_column != null)
                    mutation.delete(cfm.cfName, cfm.comparator.makeCellName(del.super_column, c), del.timestamp);
                else
                    mutation.delete(cfm.cfName, cfm.comparator.cellFromByteBuffer(c), del.timestamp);
            }
        }
        else if (del.predicate != null && del.predicate.slice_range != null)
        {
            if (del.super_column == null && cfm.isSuper())
                mutation.deleteRange(cfm.cfName,
                                     SuperColumns.startOf(del.predicate.getSlice_range().start),
                                     SuperColumns.endOf(del.predicate.getSlice_range().finish),
                                     del.timestamp);
            else if (del.super_column != null)
                mutation.deleteRange(cfm.cfName,
                                     cfm.comparator.makeCellName(del.super_column, del.predicate.getSlice_range().start),
                                     cfm.comparator.makeCellName(del.super_column, del.predicate.getSlice_range().finish),
                                     del.timestamp);
            else
                mutation.deleteRange(cfm.cfName,
                                     cfm.comparator.fromByteBuffer(del.predicate.getSlice_range().start),
                                     cfm.comparator.fromByteBuffer(del.predicate.getSlice_range().finish),
                                     del.timestamp);
        }
        else
        {
            if (del.super_column != null)
                mutation.deleteRange(
                    cfm.cfName, 
                    SuperColumns.startOf(del.super_column), 
                    SuperColumns.endOf(del.super_column), 
                    del.timestamp);
            else
                mutation.delete(cfm.cfName, del.timestamp);
        }
    }

    public void batch_mutate(Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map, ConsistencyLevel consistency_level)
    {
        doInsert(consistency_level, createMutationList(consistency_level, mutation_map, true));
    }

    public void atomic_batch_mutate(Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map, ConsistencyLevel consistency_level)
    {
        doInsert(consistency_level, createMutationList(consistency_level, mutation_map, false), true);
    }

    private void internal_remove(
            ByteBuffer key, 
            ColumnPath column_path, 
            long timestamp, 
            ConsistencyLevel consistency_level, 
            boolean isCommutativeOp)
    {
        ThriftClientState cState = state();
        String keyspace = cState.getKeyspace();
        cState.hasColumnFamilyAccess(keyspace, column_path.column_family, Permission.MODIFY);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_path.column_family, isCommutativeOp);
        ThriftValidation.validateKey(metadata, key);
        ThriftValidation.validateColumnPathOrParent(metadata, column_path);
        if (isCommutativeOp)
            ThriftConversion.fromThrift(consistency_level).validateCounterForWrite(metadata);

        org.apache.cassandra.db.Mutation mutation = new org.apache.cassandra.db.Mutation(keyspace, key);
        if (column_path.super_column == null && column_path.column == null)
            mutation.delete(column_path.column_family, timestamp);
        else if (column_path.super_column == null)
            mutation.delete(column_path.column_family, metadata.comparator.cellFromByteBuffer(column_path.column), timestamp);
        else if (column_path.column == null)
            mutation.deleteRange(column_path.column_family, SuperColumns.startOf(column_path.super_column), SuperColumns.endOf(column_path.super_column), timestamp);
        else
            mutation.delete(column_path.column_family, metadata.comparator.makeCellName(column_path.super_column, column_path.column), timestamp);

        if (isCommutativeOp)
            doInsert(consistency_level, Arrays.asList(new CounterMutation(mutation, ThriftConversion.fromThrift(consistency_level))));
        else
            doInsert(consistency_level, Arrays.asList(mutation));
    }

    public void remove(ByteBuffer key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level)
    {
        internal_remove(key, column_path, timestamp, consistency_level, false);
    }

    private void doInsert(ConsistencyLevel consistency_level, List<? extends IMutation> mutations)
    {
        doInsert(consistency_level, mutations, false);
    }

    private void doInsert(ConsistencyLevel consistency_level, List<? extends IMutation> mutations, boolean mutateAtomically)
    {
        org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
        consistencyLevel.validateForWrite(state().getKeyspace());
        if (mutations.isEmpty())
            return;

        long timeout = Long.MAX_VALUE;
        for (IMutation m : mutations)
            timeout = Longs.min(timeout, m.getTimeout());

        schedule(timeout);
        StorageProxy.mutateWithTriggers(mutations, consistencyLevel, mutateAtomically);
        release();
    }

    private void validateLogin() 
    {
        state().validateLogin();
    }

    public KsDef describe_keyspace(String keyspaceName) 
    {
        validateLogin();

        KSMetaData ksm = Schema.instance.getKSMetaData(keyspaceName);

        return ksm.toThrift();
    }

    public List<KeySlice> get_range_slices(ColumnParent column_parent, 
            SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level)
    {
        try
        {
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
            ThriftValidation.validateColumnParent(metadata, column_parent);
            ThriftValidation.validatePredicate(metadata, column_parent, predicate);
            ThriftValidation.validateKeyRange(metadata, column_parent.super_column, range);

            org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
            consistencyLevel.validateForRead(keyspace);

            List<Row> rows = null;

            IPartitioner p = StorageService.getPartitioner();
            AbstractBounds<RowPosition> bounds;
            if (range.start_key == null)
            {
                Token.TokenFactory tokenFactory = p.getTokenFactory();
                Token left = tokenFactory.fromString(range.start_token);
                Token right = tokenFactory.fromString(range.end_token);
                bounds = Range.makeRowRange(left, right, p);
            }
            else
            {
                RowPosition end = range.end_key == null
                                ? p.getTokenFactory().fromString(range.end_token).maxKeyBound(p)
                                : RowPosition.ForKey.get(range.end_key, p);
                bounds = new Bounds<RowPosition>(RowPosition.ForKey.get(range.start_key, p), end);
            }
            long now = System.currentTimeMillis();
            schedule(DatabaseDescriptor.getRangeRpcTimeout());
            try
            {
                IDiskAtomFilter filter = ThriftValidation.asIFilter(predicate, metadata, column_parent.super_column);
                rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace,
                                                                        column_parent.column_family,
                                                                        now,
                                                                        filter,
                                                                        bounds,
                                                                        ThriftConversion.fromThrift(range.row_filter),
                                                                        range.count),
                                                  consistencyLevel);
            }
            finally
            {
                release();
            }

            return thriftifyKeySlices(rows, column_parent, predicate, now);
        }
    }

    public List<KeySlice> get_paged_slice(String column_family, KeyRange range, 
            ByteBuffer start_column, ConsistencyLevel consistency_level)
    {
        try
        {

            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_family, Permission.SELECT);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_family);
            ThriftValidation.validateKeyRange(metadata, null, range);

            org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
            consistencyLevel.validateForRead(keyspace);

            SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(start_column, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, -1));

            IPartitioner p = StorageService.getPartitioner();
            AbstractBounds<RowPosition> bounds;
            if (range.start_key == null)
            {
                // (token, key) is unsupported, assume (token, token)
                Token.TokenFactory tokenFactory = p.getTokenFactory();
                Token left = tokenFactory.fromString(range.start_token);
                Token right = tokenFactory.fromString(range.end_token);
                bounds = Range.makeRowRange(left, right, p);
            }
            else
            {
                RowPosition end = range.end_key == null
                                ? p.getTokenFactory().fromString(range.end_token).maxKeyBound(p)
                                : RowPosition.ForKey.get(range.end_key, p);
                bounds = new Bounds<RowPosition>(RowPosition.ForKey.get(range.start_key, p), end);
            }

            if (range.row_filter != null && !range.row_filter.isEmpty())
                throw new InvalidRequestException("Cross-row paging is not supported along with index clauses");

            List<Row> rows;
            long now = System.currentTimeMillis();
            schedule(DatabaseDescriptor.getRangeRpcTimeout());
            try
            {
                IDiskAtomFilter filter = ThriftValidation.asIFilter(predicate, metadata, null);
                rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace, column_family, now, filter, bounds, null, range.count, true, true), consistencyLevel);
            }
            finally
            {
                release();
            }

            return thriftifyKeySlices(rows, new ColumnParent(column_family), predicate, now);
        }
    }

    private List<KeySlice> thriftifyKeySlices(List<Row> rows, ColumnParent column_parent, SlicePredicate predicate, long now)
    {
        List<KeySlice> keySlices = new ArrayList<KeySlice>(rows.size());
        boolean reversed = predicate.slice_range != null && predicate.slice_range.reversed;
        for (Row row : rows)
        {
            List<ColumnOrSuperColumn> thriftifiedColumns = thriftifyColumnFamily(row.cf, column_parent.super_column != null, reversed, now);
            keySlices.add(new KeySlice(row.key.getKey(), thriftifiedColumns));
        }

        return keySlices;
    }

    public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        try
        {
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);
            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, false);
            ThriftValidation.validateColumnParent(metadata, column_parent);
            ThriftValidation.validatePredicate(metadata, column_parent, column_predicate);
            ThriftValidation.validateIndexClauses(metadata, index_clause);
            org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
            consistencyLevel.validateForRead(keyspace);

            IPartitioner p = StorageService.getPartitioner();
            AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(RowPosition.ForKey.get(index_clause.start_key, p),
                                                                         p.getMinimumToken().minKeyBound());

            IDiskAtomFilter filter = ThriftValidation.asIFilter(column_predicate, metadata, column_parent.super_column);
            long now = System.currentTimeMillis();
            RangeSliceCommand command = new RangeSliceCommand(keyspace,
                                                              column_parent.column_family,
                                                              now,
                                                              filter,
                                                              bounds,
                                                              ThriftConversion.fromThrift(index_clause.expressions),
                                                              index_clause.count);

            List<Row> rows = StorageProxy.getRangeSlice(command, consistencyLevel);
            return thriftifyKeySlices(rows, column_parent, column_predicate, now);
        }
    }

    public List<KsDef> describe_keyspaces() throws TException, InvalidRequestException
    {
        validateLogin();

        Set<String> keyspaces = Schema.instance.getKeyspaces();
        List<KsDef> ksset = new ArrayList<KsDef>(keyspaces.size());
        for (String ks : keyspaces)
        {
            ksset.add(describe_keyspace(ks));
        }
        return ksset;
    }

    public String describe_cluster_name() throws TException
    {
        return DatabaseDescriptor.getClusterName();
    }

    public String describe_version() throws TException
    {
        return cassandraConstants.VERSION;
    }

    public List<TokenRange> describe_ring(String keyspace) throws InvalidRequestException
    {
        return StorageService.instance.describeRing(keyspace);
    }

    @Override
    public List<TokenRange> describe_local_ring(String keyspace) throws InvalidRequestException, TException
    {
        return StorageService.instance.describeLocalRing(keyspace);
    }

    public Map<String, String> describe_token_map() throws InvalidRequestException
    {
        return StorageService.instance.getTokenToEndpointMap();
    }

    public String describe_partitioner() throws TException
    {
        return StorageService.getPartitioner().getClass().getName();
    }

    public String describe_snitch() throws TException
    {
        if (DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch)
            return ((DynamicEndpointSnitch)DatabaseDescriptor.getEndpointSnitch()).subsnitch.getClass().getName();
        return DatabaseDescriptor.getEndpointSnitch().getClass().getName();
    }

    @Deprecated
    public List<String> describe_splits(String cfName, String start_token, String end_token, int keys_per_split)
    {
        List<CfSplit> splits = describe_splits_ex(cfName, start_token, end_token, keys_per_split);
        List<String> result = new ArrayList<String>(splits.size() + 1);

        result.add(splits.get(0).getStart_token());
        for (CfSplit cfSplit : splits)
            result.add(cfSplit.getEnd_token());

        return result;
    }

    public List<CfSplit> describe_splits_ex(String cfName, String start_token, String end_token, int keys_per_split)
    {
        Token.TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
        Range<Token> tr = new Range<Token>(tf.fromString(start_token), tf.fromString(end_token));
        List<Pair<Range<Token>, Long>> splits =
                StorageService.instance.getSplits(state().getKeyspace(), cfName, tr, keys_per_split);
        List<CfSplit> result = new ArrayList<CfSplit>(splits.size());
        for (Pair<Range<Token>, Long> split : splits)
            result.add(new CfSplit(split.left.left.toString(), split.left.right.toString(), split.right));
        return result;
    }

    public void login(AuthenticationRequest auth_request)
    {
        AuthenticatedUser user = DatabaseDescriptor.getAuthenticator().authenticate(auth_request.getCredentials());
        state().login(user);
    }

    /**
     * Schedule the current thread for access to the required services
     */
    private void schedule(long timeoutMS)
    {
        requestScheduler.queue(Thread.currentThread(), state().getSchedulingValue(), timeoutMS);
    }

    /**
     * Release count for the used up resources
     */
    private void release()
    {
        requestScheduler.release();
    }

    public String system_add_column_family(CfDef cf_def)
    {
        ClientState cState = state();
        String keyspace = cState.getKeyspace();
        cState.hasKeyspaceAccess(keyspace, Permission.CREATE);
        cf_def.unsetId(); // explicitly ignore any id set by client (Hector likes to set zero)
        CFMetaData cfm = CFMetaData.fromThrift(cf_def);
        CFMetaData.validateCompactionOptions(cfm.compactionStrategyClass, cfm.compactionStrategyOptions);
        cfm.addDefaultIndexNames();

        MigrationManager.announceNewColumnFamily(cfm);
        return Schema.instance.getVersion().toString();
    }

    public String system_drop_column_family(String column_family)
    {
        ThriftClientState cState = state();

        String keyspace = cState.getKeyspace();
        cState.hasColumnFamilyAccess(keyspace, column_family, Permission.DROP);
        MigrationManager.announceColumnFamilyDrop(keyspace, column_family);
        return Schema.instance.getVersion().toString();
    }

    public String system_add_keyspace(KsDef ks_def)
    {
        ThriftValidation.validateKeyspaceNotSystem(ks_def.name);
        state().hasAllKeyspacesAccess(Permission.CREATE);
        ThriftValidation.validateKeyspaceNotYetExisting(ks_def.name);

        Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>(ks_def.cf_defs.size());
        for (CfDef cf_def : ks_def.cf_defs)
        {
            cf_def.unsetId(); // explicitly ignore any id set by client (same as system_add_column_family)
            CFMetaData cfm = CFMetaData.fromThrift(cf_def);
            cfm.addDefaultIndexNames();

            cfDefs.add(cfm);
        }
        MigrationManager.announceNewKeyspace(KSMetaData.fromThrift(ks_def, cfDefs.toArray(new CFMetaData[cfDefs.size()])));
        return Schema.instance.getVersion().toString();
    }

    public String system_drop_keyspace(String keyspace)
    {
        ThriftValidation.validateKeyspaceNotSystem(keyspace);
        state().hasKeyspaceAccess(keyspace, Permission.DROP);

        MigrationManager.announceKeyspaceDrop(keyspace);
        return Schema.instance.getVersion().toString();
    }

    /** update an existing keyspace, but do not allow column family modifications.
     * @throws SchemaDisagreementException
     */
    public String system_update_keyspace(KsDef ks_def)
    {
        ThriftValidation.validateKeyspaceNotSystem(ks_def.name);
        state().hasKeyspaceAccess(ks_def.name, Permission.ALTER);
        ThriftValidation.validateKeyspace(ks_def.name);
        
        MigrationManager.announceKeyspaceUpdate(KSMetaData.fromThrift(ks_def));
        return Schema.instance.getVersion().toString();
    }

    public String system_update_column_family(CfDef cf_def)
    {
        state().hasColumnFamilyAccess(cf_def.keyspace, cf_def.name, Permission.ALTER);
        CFMetaData oldCfm = Schema.instance.getCFMetaData(cf_def.keyspace, cf_def.name);

        CFMetaData cfm = CFMetaData.fromThriftForUpdate(cf_def, oldCfm);
        CFMetaData.validateCompactionOptions(cfm.compactionStrategyClass, cfm.compactionStrategyOptions);
        cfm.addDefaultIndexNames();

        MigrationManager.announceColumnFamilyUpdate(cfm, true);
        return Schema.instance.getVersion().toString();
    }

    public void truncate(String cfname) 
    {
        ClientState cState = state();

        String keyspace = cState.getKeyspace();
        cState.hasColumnFamilyAccess(keyspace, cfname, Permission.MODIFY);

        schedule(DatabaseDescriptor.getTruncateRpcTimeout());
        StorageProxy.truncateBlocking(cState.getKeyspace(), cfname);
        release();
    }

    public void set_keyspace(String keyspace) 
    {
        state().setKeyspace(keyspace);
    }

    public Map<String, List<String>> describe_schema_versions() 
    {
        return StorageProxy.describeSchemaVersions();
    }

    // counter methods

    public void add(ByteBuffer key, ColumnParent column_parent, CounterColumn column, ConsistencyLevel consistency_level)
    {
        try
        {
            ClientState cState = state();
            String keyspace = cState.getKeyspace();

            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.MODIFY);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, true);
            ThriftValidation.validateKey(metadata, key);
            ThriftConversion.fromThrift(consistency_level).validateCounterForWrite(metadata);
            ThriftValidation.validateColumnParent(metadata, column_parent);
            // SuperColumn field is usually optional, but not when we're adding
            if (metadata.cfType == ColumnFamilyType.Super && column_parent.super_column == null)
                throw new InvalidRequestException("missing mandatory super column name for super CF " + column_parent.column_family);

            ThriftValidation.validateColumnNames(metadata, column_parent, Arrays.asList(column.name));

            org.apache.cassandra.db.Mutation mutation = new org.apache.cassandra.db.Mutation(keyspace, key);
            try
            {
                if (metadata.isSuper())
                    mutation.addCounter(
                        column_parent.column_family, 
                        metadata.comparator.makeCellName(column_parent.super_column, column.name), 
                        column.value);
                else
                    mutation.addCounter(
                        column_parent.column_family, 
                        metadata.comparator.cellFromByteBuffer(column.name), 
                        column.value);
            }

            doInsert(
                consistency_level, 
                Arrays.asList(new CounterMutation(mutation, ThriftConversion.fromThrift(consistency_level))));
        }
    }

    public void remove_counter(ByteBuffer key, ColumnPath path, ConsistencyLevel consistency_level)
    {
        internal_remove(key, path, System.currentTimeMillis(), consistency_level, true);
    }

    private static String uncompress(ByteBuffer query, Compression compression)
    {
        String queryString = null;

        // Decompress the query string.
        try
        {
            switch (compression)
            {
                case GZIP:
                    DataOutputBuffer decompressed = new DataOutputBuffer();
                    byte[] outBuffer = new byte[1024], inBuffer = new byte[1024];

                    Inflater decompressor = new Inflater();

                    int lenRead = 0;
                    while (true)
                    {
                        if (decompressor.needsInput())
                            lenRead = query.remaining() < 1024 ? query.remaining() : 1024;
                        query.get(inBuffer, 0, lenRead);
                        decompressor.setInput(inBuffer, 0, lenRead);

                        int lenWrite = 0;
                        while ((lenWrite = decompressor.inflate(outBuffer)) != 0)
                            decompressed.write(outBuffer, 0, lenWrite);

                        if (decompressor.finished())
                            break;
                    }

                    decompressor.end();

                    queryString = new String(decompressed.getData(), 0, decompressed.getLength(), StandardCharsets.UTF_8);
                    break;
                case NONE:
                    queryString = ByteBufferUtil.string(query);

                    break;
            }
        }

        return queryString;
    }

    private void validateCQLVersion(int major) throws InvalidRequestException
    {
        /*
         * The rules are:
         *   - If no version are set, we don't validate anything. The reason is
         *     that 1) old CQL2 client might not have called set_cql_version
         *     and 2) some client may have removed the set_cql_version for CQL3
         *     when updating to 1.2.0. A CQL3 client upgrading from pre-1.2
         *     shouldn't be in that case however since set_cql_version uses to
         *     be mandatory (for CQL3).
         *   - Otherwise, checks the major matches whatever was set.
         */
        SemanticVersion versionSet = state().getCQLVersion();

        check @ (versionSet.major == major);
    }

    public CqlResult execute_cql_query(ByteBuffer query, Compression compression)
    {
        validateCQLVersion(2);
        maybeLogCQL2Warning();
        
        return QueryProcessor.process(queryString, state());
    }

    public CqlResult execute_cql3_query(ByteBuffer query, Compression compression, ConsistencyLevel cLevel)
    {
        validateCQLVersion(3);
        
        String queryString = uncompress(query, compression);

        ThriftClientState cState = state();
        return cState
                .getCQLQueryHandler()
                .process(
                    queryString, 
                    cState.getQueryState(), 
                    QueryOptions.fromProtocolV2(
                        ThriftConversion.fromThrift(cLevel), 
                        Collections.<ByteBuffer>emptyList())
                )
                .toThriftResult();
    }

    public CqlPreparedResult prepare_cql_query(ByteBuffer query, Compression compression)
    {
        validateCQLVersion(2);
        maybeLogCQL2Warning();

        String queryString = uncompress(query, compression);
        ThriftClientState cState = state();

        cState.validateLogin();
        return QueryProcessor.prepare(queryString, cState);
    }

    public CqlPreparedResult prepare_cql3_query(ByteBuffer query, Compression compression)
    {
        validateCQLVersion(3);

        String queryString = uncompress(query, compression);
        ThriftClientState cState = state();

        cState.validateLogin();
        return cState.getCQLQueryHandler().prepare(queryString, cState.getQueryState()).toThriftPreparedResult();
    }

    @Override
    public List<ColumnOrSuperColumn> get_multi_slice(MultiSliceRequest request)
    {
        ClientState cState = state();
        String keyspace = cState.getKeyspace();
        state().hasColumnFamilyAccess(keyspace, request.getColumn_parent().column_family, Permission.SELECT);
        
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, request.getColumn_parent().column_family);
        
        ThriftValidation.validateColumnParent(metadata, request.getColumn_parent());
        org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(request.getConsistency_level());
        consistencyLevel.validateForRead(keyspace);
        List<ReadCommand> commands = new ArrayList<>(1);
        
        ColumnSlice[] slices = new ColumnSlice[request.getColumn_slices().size()];
        for (int i = 0 ; i < request.getColumn_slices().size() ; i++)
        {
            fixOptionalSliceParameters(request.getColumn_slices().get(i));
            Composite start = metadata.comparator.fromByteBuffer(request.getColumn_slices().get(i).start);
            Composite finish = metadata.comparator.fromByteBuffer(request.getColumn_slices().get(i).finish);
            if (!start.isEmpty() && !finish.isEmpty())
            {
                int compare = metadata.comparator.compare(start, finish);
            }
            slices[i] = new ColumnSlice(start, finish);
        }

        ColumnSlice[] deoverlapped 
            = ColumnSlice.deoverlapSlices(
                slices, 
                request.reversed ? metadata.comparator.reverseComparator() : metadata.comparator);
        SliceQueryFilter filter = new SliceQueryFilter(deoverlapped, request.reversed, request.count);
        ThriftValidation.validateKey(metadata, request.key);
        commands.add(
            ReadCommand.create(
                keyspace, 
                request.key, 
                request.column_parent.getColumn_family(), 
                System.currentTimeMillis(), 
                filter)
            );

        return getSlice(
                commands, 
                request.column_parent.isSetSuper_column(), 
                consistencyLevel, 
                cState)
            .entrySet().iterator().next().getValue();
    }

    /**
     * Set the to start-of end-of value of "" for start and finish.
     * @param columnSlice
     */
    private static void fixOptionalSliceParameters(org.apache.cassandra.thrift.ColumnSlice columnSlice) {
        if (!columnSlice.isSetStart())
            columnSlice.setStart(new byte[0]);
        if (!columnSlice.isSetFinish())
            columnSlice.setFinish(new byte[0]);
    }

    public CqlResult execute_prepared_cql_query(int itemId, List<ByteBuffer> bindVariables)
    {
        validateCQLVersion(2);
        maybeLogCQL2Warning();

        ThriftClientState cState = state();
        CQLStatement statement = cState.getPrepared().get(itemId);

        return QueryProcessor.processPrepared(statement, cState, bindVariables);
    }

    public CqlResult execute_prepared_cql3_query(int itemId, List<ByteBuffer> bindVariables, ConsistencyLevel cLevel)
    {
        validateCQLVersion(3);

        ThriftClientState cState = state();
        ParsedStatement.Prepared prepared = cState.getCQLQueryHandler().getPreparedForThrift(itemId);

        return cState
                .getCQLQueryHandler()
                .processPrepared (
                    prepared.statement, 
                    cState.getQueryState(),
                    QueryOptions.fromProtocolV2(
                        ThriftConversion.fromThrift(cLevel), 
                        bindVariables
                    )
                )
                .toThriftResult();
    }

    /*
     * Deprecated, but if a client sets CQL2, it is a no-op for compatibility sake.
     * If it sets CQL3 however, we throw an IRE because this mean the client
     * hasn't been updated for Cassandra 1.2 and should start using the new
     * execute_cql3_query, etc... and there is no point no warning it early.
     */
    public void set_cql_version(String version)
    {
        state().setCQLVersion(version);
    }

    private void maybeLogCQL2Warning()
    {
        if (!loggedCQL2Warning)
        {
            logger.warn("CQL2 has been deprecated since Cassandra 2.0, and will be removed entirely in version 2.2."
                        + " Please switch to CQL3 before then.");
            loggedCQL2Warning = true;
        }
    }

    public ByteBuffer trace_next_query() 
    {
        UUID sessionId = UUIDGen.getTimeUUID();
        state().getQueryState().prepareTracingSession(sessionId);
        return TimeUUIDType.instance.decompose(sessionId);
    }

    private boolean startSessionIfRequested()
    {
        if (state().getQueryState().traceNextQuery())
        {
            state().getQueryState().createTracingSession();
            return true;
        }
        return false;
    }

    private void registerMetrics()
    {
        ClientMetrics.instance.addCounter("connectedThriftClients", new Callable<Integer>()
        {
            @Override
            public Integer call() throws Exception
            {
                return ThriftSessionManager.instance.getConnectedClients();
            }
        });
    }

    private static class ThriftCASRequest implements CASRequest
    {
        private final ColumnFamily expected;
        private final ColumnFamily updates;

        private ThriftCASRequest(ColumnFamily expected, ColumnFamily updates)
        {
            this.expected = expected;
            this.updates = updates;
        }

        public IDiskAtomFilter readFilter()
        {
            return expected == null || expected.isEmpty()
                 ? new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 1)
                 : new NamesQueryFilter(ImmutableSortedSet.copyOf(expected.getComparator(), expected.getColumnNames()));
        }

        public boolean appliesTo(ColumnFamily current)
        {
            long now = System.currentTimeMillis();

            if (!hasLiveCells(expected, now))
                return !hasLiveCells(current, now);
            else if (!hasLiveCells(current, now))
                return false;

            // current has been built from expected, so we know that it can't have columns
            // that excepted don't have. So we just check that for each columns in expected:
            //   - if it is a tombstone, whether current has no column or a tombstone;
            //   - otherwise, that current has a live column with the same value.
            for (Cell e : expected)
            {
                Cell c = current.getColumn(e.name());
                if (e.isLive(now))
                {
                    if (c == null || !c.isLive(now) || !c.value().equals(e.value()))
                        return false;
                }
                else
                {
                    if (c != null && c.isLive(now))
                        return false;
                }
            }
            return true;
        }

        private static boolean hasLiveCells(ColumnFamily cf, long now)
        {
            return cf != null && !cf.hasOnlyTombstones(now);
        }

        public ColumnFamily makeUpdates(ColumnFamily current)
        {
            return updates;
        }
    }
}
