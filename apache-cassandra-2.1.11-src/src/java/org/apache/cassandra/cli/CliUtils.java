

public class CliUtils
{
    /**
     * Strips leading and trailing "'" characters, and handles
     * and escaped characters such as \n, \r, etc.
     * @param b - string to unescape
     * @return String - unexspaced string
     */
    public static String unescapeSQLString(String b)
    {
        if (b.charAt(0) == '\'' && b.charAt(b.length()-1) == '\'')
            b = b.substring(1, b.length()-1);
        return StringEscapeUtils.unescapeJava(b);
    }

    public static String escapeSQLString(String b)
    {
        // single quotes are not escaped in java, need to be for cli
        return StringEscapeUtils.escapeJava(b).replace("\'", "\\'");
    }

    public static String maybeEscapeName(String name)
    {
        return Character.isLetter(name.charAt(0)) ? name : "\'" + name + "\'";
    }

    /**
     * Returns IndexOperator from string representation
     * @param operator - string representing IndexOperator (=, >=, >, <, <=)
     * @return IndexOperator - enum value of IndexOperator or null if not found
     */
    public static IndexOperator getIndexOperator(String operator)
    {
        if (operator.equals("="))
        {
            return IndexOperator.EQ;
        }
        else if (operator.equals(">="))
        {
            return IndexOperator.GTE;
        }
        else if (operator.equals(">"))
        {
            return IndexOperator.GT;
        }
        else if (operator.equals("<"))
        {
            return IndexOperator.LT;
        }
        else if (operator.equals("<="))
        {
            return IndexOperator.LTE;
        }

        return null;
    }

    /**
     * Returns set of column family names in specified keySpace.
     * @param keySpace - keyspace definition to get column family names from.
     * @return Set - column family names
     */
    public static Set<String> getCfNamesByKeySpace(KsDef keySpace)
    {
        Set<String> names = new LinkedHashSet<String>();

        for (CfDef cfDef : keySpace.getCf_defs())
        {
            names.add(cfDef.getName());
        }

        return names;
    }

    /**
     * Parse the statement from cli and return KsDef
     *
     * @param keyspaceName - name of the keyspace to lookup
     * @param keyspaces - List of known keyspaces
     *
     * @return metadata about keyspace or null
     */
    public static KsDef getKeySpaceDef(String keyspaceName, List<KsDef> keyspaces)
    {
        keyspaceName = keyspaceName.toUpperCase();

        for (KsDef ksDef : keyspaces)
        {
            if (ksDef.name.toUpperCase().equals(keyspaceName))
                return ksDef;
        }

        return null;
    }
}
