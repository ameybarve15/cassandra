
/**
 * Select expressions are analogous to the projection in a SQL query. They
 * determine which columns will appear in the result set.  SelectExpression
 * instances encapsulate a parsed expression from a <code>SELECT</code>
 * statement.
 *
 * See: doc/cql/CQL.html#SpecifyingColumns
 */
public class SelectExpression
{
    public static final int MAX_COLUMNS_DEFAULT = 10000;

    private int numColumns = MAX_COLUMNS_DEFAULT;
    private boolean reverseColumns = false;
    private final boolean hasFirstSet;
    private final boolean wildcard;
    private final Term start, finish;
    private final List<Term> columns;

    /**
     * Create a new SelectExpression for a range (slice) of columns.
     *
     * @param start the starting column name
     * @param finish the finishing column name
     * @param count the number of columns to limit the results to
     * @param reverse true to reverse column order
     * @param wildcard determines weather this statement is wildcard
     * @param firstSet determines weather "FIRST" keyword was set
     */
    public SelectExpression(Term start, Term finish, int count, boolean reverse, boolean wildcard, boolean firstSet)
    {
        this.start = start;
        this.finish = finish;
        numColumns = count;
        reverseColumns = reverse;
        this.wildcard = wildcard;
        hasFirstSet = firstSet;
        this.columns = null;
    }

    /**
     * Create a new SelectExpression for a list of columns.
     *
     * @param first the first (possibly only) column name to select on.
     * @param count the number of columns to limit the results on
     * @param reverse true to reverse column order
     * @param firstSet determines weather "FIRST" keyword was set
     */
    public SelectExpression(Term first, int count, boolean reverse, boolean firstSet)
    {
        wildcard = false;
        columns = new ArrayList<Term>();
        columns.add(first);
        numColumns = count;
        reverseColumns = reverse;
        hasFirstSet = firstSet;
        start = null;
        finish = null;
    }

    /**
     * Add an additional column name to a SelectExpression.
     *
     * @param addTerm
     */
    public void and(Term addTerm)
    {
        assert !isColumnRange();    // Not possible when invoked by parser
        columns.add(addTerm);
    }

    public boolean isColumnRange()
    {
        return (start != null);
    }

    public boolean isColumnList()
    {
        return !isColumnRange();
    }
}
