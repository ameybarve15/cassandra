

/**
 * ProgressInfo contains file transfer progress.
 */
public class ProgressInfo implements Serializable
{
    /**
     * Direction of the stream.
     */
    public static enum Direction
    {
        OUT(0),
        IN(1);

        public final byte code;

        private Direction(int code)
        {
            this.code = (byte) code;
        }

        public static Direction fromByte(byte direction)
        {
            return direction == 0 ? OUT : IN;
        }
    }

    public final InetAddress peer;
    public final int sessionIndex;
    public final String fileName;
    public final Direction direction;
    public final long currentBytes;
    public final long totalBytes;

    public ProgressInfo(InetAddress peer, int sessionIndex, String fileName, Direction direction, long currentBytes, long totalBytes)
    {
        assert totalBytes > 0;

        this.peer = peer;
        this.sessionIndex = sessionIndex;
        this.fileName = fileName;
        this.direction = direction;
        this.currentBytes = currentBytes;
        this.totalBytes = totalBytes;
    }

    /**
     * @return true if file transfer is completed
     */
    public boolean isCompleted()
    {
        return currentBytes >= totalBytes;
    }

    /**
     * ProgressInfo is considered to be equal only when all attributes except currentBytes are equal.
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProgressInfo that = (ProgressInfo) o;

        if (totalBytes != that.totalBytes) return false;
        if (direction != that.direction) return false;
        if (!fileName.equals(that.fileName)) return false;
        if (sessionIndex != that.sessionIndex) return false;
        return peer.equals(that.peer);
    }

}
