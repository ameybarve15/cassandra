
/**
 * Relations encapsulate the relationship between an entity of some kind, and
 * a value (term). For example, <key> > "start" or "colname1" = "somevalue".
 *
 */
public class Relation
{
    private final Term entity;
    private final RelationType relationType;
    private final Term value;
}

enum RelationType
{
    EQ, LT, LTE, GTE, GT;

    public static RelationType forString(String s)
    {
        if (s.equals("="))
            return EQ;
        else if (s.equals("<"))
            return LT;
        else if (s.equals("<="))
            return LTE;
        else if (s.equals(">="))
            return GTE;
        else if (s.equals(">"))
            return GT;

        return null;
    }
}
