
/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 */
public class SelectStatement
{
    private final SelectExpression expression;
    private final boolean isCountOper;
    private final String columnFamily;
    private final String keyspace;
    private final ConsistencyLevel cLevel;
    private final WhereClause clause;
    private final int numRecords;

    public SelectStatement(SelectExpression expression, boolean isCountOper, String keyspace, String columnFamily,
            ConsistencyLevel cLevel, WhereClause clause, int numRecords)
    {
        this.expression = expression;
        this.isCountOper = isCountOper;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.cLevel = cLevel;
        this.clause = (clause != null) ? clause : new WhereClause();
        this.numRecords = numRecords;
    }

    public boolean isKeyRange()
    {
        return clause.isKeyRange();
    }

    public Set<Term> getKeys()
    {
        return clause.getKeys();
    }

    public Term getKeyStart()
    {
        return clause.getStartKey();
    }

    public Term getKeyFinish()
    {
        return clause.getFinishKey();
    }

    public List<Relation> getColumnRelations()
    {
        return clause.getColumnRelations();
    }

    public boolean isColumnRange()
    {
        return expression.isColumnRange();
    }

    public boolean isWildcard()
    {
        return expression.isWildcard();
    }
    public boolean isFullWildcard()
    {
        return expression.isWildcard() && !expression.hasFirstSet();
    }

    public List<Term> getColumnNames()
    {
        return expression.getColumns();
    }

    public Term getColumnStart()
    {
        return expression.getStart();
    }

    public Term getColumnFinish()
    {
        return expression.getFinish();
    }

    public boolean isSetKeyspace()
    {
        return keyspace != null;
    }

    public boolean isColumnsReversed()
    {
        return expression.isColumnsReversed();
    }

    public int getColumnsLimit()
    {
        return expression.getColumnsLimit();
    }

    public boolean includeStartKey()
    {
        return clause.includeStartKey();
    }

    public boolean includeFinishKey()
    {
        return clause.includeFinishKey();
    }

    public String getKeyAlias()
    {
        return clause.getKeyAlias();
    }

    public boolean isMultiKey()
    {
        return clause.isMultiKey();
    }

    public void extractKeyAliasFromColumns(CFMetaData cfm)
    {
        clause.extractKeysFromColumns(cfm);
    }

    public List<Relation> getClauseRelations()
    {
        return clause.getClauseRelations();
    }
}
