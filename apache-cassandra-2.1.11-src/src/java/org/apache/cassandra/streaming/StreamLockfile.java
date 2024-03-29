
/**
 * Encapsulates the behavior for 'locking' any streamed sttables to a node.
 * If a process crashes while converting a set of SSTableWriters to SSTReaders
 * (meaning, some subset of SSTWs were converted, but not the entire set), we want
 * to disregard the entire set as we will surely have missing data (by definition).
 *
 * Basic behavior id to write out the names of all SSTWs to a file, one SSTW per line,
 * and then delete the file when complete (normal behavior). This should happen before
 * converting any SSTWs. Thus, the lockfile is created, some SSTWs are converted,
 * and if the process crashes, on restart, we look for any existing lockfile, and delete
 * any referenced SSTRs.
 */
public class StreamLockfile
{
    public static final String FILE_EXT = ".lockfile";
    private static final Logger logger = LoggerFactory.getLogger(StreamLockfile.class);

    private final File lockfile;

    public StreamLockfile(File directory, UUID uuid)
    {
        lockfile = new File(directory, uuid.toString() + FILE_EXT);
    }

    public StreamLockfile(File lockfile)
    {
        assert lockfile != null;
        this.lockfile = lockfile;
    }

    public void create(Collection<SSTableWriter> sstables)
    {
        List<String> sstablePaths = new ArrayList<>(sstables.size());
        for (SSTableWriter writer : sstables)
        {
            /* write out the file names *without* the 'tmp-file' flag in the file name.
               this class will not need to clean up tmp files (on restart), CassandraDaemon does that already,
               just make sure we delete the fully-formed SSTRs. */
            sstablePaths.add(writer.descriptor.asType(Descriptor.Type.FINAL).baseFilename());
        }

        try
        {
            Files.write(lockfile.toPath(), sstablePaths, Charsets.UTF_8,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.DSYNC);
        }
        catch (IOException e)
        {
            logger.warn(String.format("Could not create lockfile %s for stream session, nothing to worry too much about", lockfile), e);
        }
    }

    public void delete()
    {
        FileUtils.delete(lockfile);
    }

    public void cleanup()
    {
        List<String> files = readLockfile(lockfile);
        for (String file : files)
        {
            try
            {
                Descriptor desc = Descriptor.fromFilename(file, true);
                SSTable.delete(desc, SSTable.componentsFor(desc));
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.warn("failed to delete a potentially stale sstable {}", file);
            }
        }
    }

    private List<String> readLockfile(File lockfile)
    {
        try
        {
            return Files.readAllLines(lockfile.toPath(), Charsets.UTF_8);
        }
        catch (IOException e)
        {
            logger.info("couldn't read lockfile {}, ignoring", lockfile.getAbsolutePath());
            return Collections.emptyList();
        }
    }

}
