

public enum Stage
{
    READ,
    MUTATION,
    COUNTER_MUTATION,
    GOSSIP,
    REQUEST_RESPONSE,
    ANTI_ENTROPY,
    MIGRATION,
    MISC,
    TRACING,
    INTERNAL_RESPONSE,
    READ_REPAIR;

    public String getJmxType()
    {
        switch (this)
        {
            case ANTI_ENTROPY:
            case GOSSIP:
            case MIGRATION:
            case MISC:
            case TRACING:
            case INTERNAL_RESPONSE:
                return "internal";
            case MUTATION:
            case COUNTER_MUTATION:
            case READ:
            case REQUEST_RESPONSE:
            case READ_REPAIR:
                return "request";
            default:
                throw new AssertionError("Unknown stage " + this);
        }
    }
}
