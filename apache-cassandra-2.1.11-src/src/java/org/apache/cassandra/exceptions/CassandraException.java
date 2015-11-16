
public abstract class CassandraException extends Exception implements TransportException
{
    private final ExceptionCode code;

    protected CassandraException(ExceptionCode code, String msg)
    {
        super(msg);
        this.code = code;
    }

    protected CassandraException(ExceptionCode code, String msg, Throwable cause)
    {
        super(msg, cause);
        this.code = code;
    }

    public ExceptionCode code()
    {
        return code;
    }
}
