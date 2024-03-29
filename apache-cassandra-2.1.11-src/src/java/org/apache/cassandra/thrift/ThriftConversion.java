

/**
 * Static utility methods to convert internal structure to and from thrift ones.
 */
public class ThriftConversion
{
    public static org.apache.cassandra.db.ConsistencyLevel fromThrift(ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ANY: return org.apache.cassandra.db.ConsistencyLevel.ANY;
            case ONE: return org.apache.cassandra.db.ConsistencyLevel.ONE;
            case TWO: return org.apache.cassandra.db.ConsistencyLevel.TWO;
            case THREE: return org.apache.cassandra.db.ConsistencyLevel.THREE;
            case QUORUM: return org.apache.cassandra.db.ConsistencyLevel.QUORUM;
            case ALL: return org.apache.cassandra.db.ConsistencyLevel.ALL;
            case LOCAL_QUORUM: return org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
            case EACH_QUORUM: return org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
            case SERIAL: return org.apache.cassandra.db.ConsistencyLevel.SERIAL;
            case LOCAL_SERIAL: return org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL;
            case LOCAL_ONE: return org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE;
        }
        throw new AssertionError();
    }

    public static ConsistencyLevel toThrift(org.apache.cassandra.db.ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ANY: return ConsistencyLevel.ANY;
            case ONE: return ConsistencyLevel.ONE;
            case TWO: return ConsistencyLevel.TWO;
            case THREE: return ConsistencyLevel.THREE;
            case QUORUM: return ConsistencyLevel.QUORUM;
            case ALL: return ConsistencyLevel.ALL;
            case LOCAL_QUORUM: return ConsistencyLevel.LOCAL_QUORUM;
            case EACH_QUORUM: return ConsistencyLevel.EACH_QUORUM;
            case SERIAL: return ConsistencyLevel.SERIAL;
            case LOCAL_SERIAL: return ConsistencyLevel.LOCAL_SERIAL;
            case LOCAL_ONE: return ConsistencyLevel.LOCAL_ONE;
        }
        throw new AssertionError();
    }

    // We never return, but returning a RuntimeException allows to write "throw rethrow(e)" without java complaining
    // for methods that have a return value.
    public static RuntimeException rethrow(RequestExecutionException e) throws UnavailableException, TimedOutException
    {
        if (e instanceof RequestTimeoutException)
            throw toThrift((RequestTimeoutException)e);
        else
            throw new UnavailableException();
    }

    public static InvalidRequestException toThrift(RequestValidationException e)
    {
        return new InvalidRequestException(e.getMessage());
    }

    public static UnavailableException toThrift(org.apache.cassandra.exceptions.UnavailableException e)
    {
        return new UnavailableException();
    }

    public static AuthenticationException toThrift(org.apache.cassandra.exceptions.AuthenticationException e)
    {
        return new AuthenticationException(e.getMessage());
    }

    public static TimedOutException toThrift(RequestTimeoutException e)
    {
        TimedOutException toe = new TimedOutException();
        if (e instanceof WriteTimeoutException)
        {
            WriteTimeoutException wte = (WriteTimeoutException)e;
            toe.setAcknowledged_by(wte.received);
            if (wte.writeType == WriteType.BATCH_LOG)
                toe.setAcknowledged_by_batchlog(false);
            else if (wte.writeType == WriteType.BATCH)
                toe.setAcknowledged_by_batchlog(true);
            else if (wte.writeType == WriteType.CAS)
                toe.setPaxos_in_progress(true);
        }
        return toe;
    }

    public static List<org.apache.cassandra.db.IndexExpression> fromThrift(List<IndexExpression> exprs)
    {
        List<org.apache.cassandra.db.IndexExpression> converted = new ArrayList<>(exprs.size());
        for (IndexExpression expr : exprs)
        {
            converted.add(new org.apache.cassandra.db.IndexExpression(expr.column_name,
                                                                      Operator.valueOf(expr.op.name()),
                                                                      expr.value));
        }
        return converted;
    }
}
