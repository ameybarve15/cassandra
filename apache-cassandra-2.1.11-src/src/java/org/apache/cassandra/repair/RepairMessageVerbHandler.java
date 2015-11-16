
/**
 * Handles all repair related message.
 *
 * @since 2.0
 */
public class RepairMessageVerbHandler implements IVerbHandler<RepairMessage>
{
    private static final Logger logger = LoggerFactory.getLogger(RepairMessageVerbHandler.class);
    public void doVerb(final MessageIn<RepairMessage> message, final int id)
    {
        // TODO add cancel/interrupt message
        RepairJobDesc desc = message.payload.desc;
        try
        {
            switch (message.payload.messageType)
            {
                case PREPARE_MESSAGE:
                    PrepareMessage prepareMessage = (PrepareMessage) message.payload;
                    List<ColumnFamilyStore> columnFamilyStores = new ArrayList<>(prepareMessage.cfIds.size());
                    for (UUID cfId : prepareMessage.cfIds)
                    {
                        Pair<String, String> kscf = Schema.instance.getCF(cfId);
                        ColumnFamilyStore columnFamilyStore = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);
                        columnFamilyStores.add(columnFamilyStore);
                    }
                    ActiveRepairService.instance.registerParentRepairSession(prepareMessage.parentRepairSession,
                            columnFamilyStores,
                            prepareMessage.ranges);
                    MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), id, message.from);
                    break;

                case SNAPSHOT:
                    ColumnFamilyStore cfs = Keyspace.open(desc.keyspace).getColumnFamilyStore(desc.columnFamily);
                    final Range<Token> repairingRange = desc.range;
                    cfs.snapshot(desc.sessionId.toString(), new Predicate<SSTableReader>()
                    {
                        public boolean apply(SSTableReader sstable)
                        {
                            return sstable != null &&
                                    !(sstable.partitioner instanceof LocalPartitioner) && // exclude SSTables from 2i
                                    new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(Collections.singleton(repairingRange));
                        }
                    }, true); //ephemeral snapshot, if repair fails, it will be cleaned next startup

                    logger.debug("Enqueuing response to snapshot request {} to {}", desc.sessionId, message.from);
                    MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), id, message.from);
                    break;

                case VALIDATION_REQUEST:
                    ValidationRequest validationRequest = (ValidationRequest) message.payload;
                    // trigger read-only compaction
                    ColumnFamilyStore store = Keyspace.open(desc.keyspace).getColumnFamilyStore(desc.columnFamily);

                    Validator validator = new Validator(desc, message.from, validationRequest.gcBefore);
                    CompactionManager.instance.submitValidation(store, validator);
                    break;

                case SYNC_REQUEST:
                    // forwarded sync request
                    SyncRequest request = (SyncRequest) message.payload;
                    StreamingRepairTask task = new StreamingRepairTask(desc, request);
                    task.run();
                    break;

                case ANTICOMPACTION_REQUEST:
                    logger.debug("Got anticompaction request");
                    AnticompactionRequest anticompactionRequest = (AnticompactionRequest) message.payload;
                    ListenableFuture<?> compactionDone = ActiveRepairService.instance.doAntiCompaction(anticompactionRequest.parentRepairSession);
                    compactionDone.addListener(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), id, message.from);
                        }
                    }, MoreExecutors.sameThreadExecutor());
                    break;

                case CLEANUP:
                    logger.debug("cleaning up repair");
                    CleanupMessage cleanup = (CleanupMessage) message.payload;
                    ActiveRepairService.instance.removeParentRepairSession(cleanup.parentRepairSession);
                    MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), id, message.from);
                    break;

                default:
                    ActiveRepairService.instance.handleMessage(message.from, message.payload);
                    break;
            }
        }

    }
}
