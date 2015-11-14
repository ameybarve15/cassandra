

public class CQLStatement
{
    public final StatementType type;
    public final Object statement;
    public final int boundTerms;

    public CQLStatement(StatementType type, Object statement, int lastMarker)
    {
        this.type = type;
        this.statement = statement;
        this.boundTerms = lastMarker + 1;
    }
}
