


public class CliCompiler
{

    // ANTLR does not provide case-insensitive tokenization support
    // out of the box. So we override the LA (lookahead) function
    // of the ANTLRStringStream class. Note: This doesn't change the
    // token text-- but just relaxes the matching rules to match
    // in upper case. [Logic borrowed from Hive code.]
    //
    // Also see discussion on this topic in:
    // http://www.antlr.org/wiki/pages/viewpage.action?pageId=1782.
    public static class ANTLRNoCaseStringStream  extends ANTLRStringStream
    {
        public ANTLRNoCaseStringStream(String input)
        {
            super(input);
        }

        public int LA(int i)
        {
            int returnChar = super.LA(i);
            if (returnChar == CharStream.EOF)
            {
                return returnChar;
            }
            else if (returnChar == 0)
            {
                return returnChar;
            }

            return Character.toUpperCase((char)returnChar);
        }
    }

    public static Tree compileQuery(String query)
    {
        Tree queryTree;

        try
        {
            ANTLRStringStream input = new ANTLRNoCaseStringStream(query);

            CliLexer lexer = new CliLexer(input);
            CommonTokenStream tokens = new CommonTokenStream(lexer);

            CliParser parser = new CliParser(tokens);

            // start parsing...
            queryTree = (Tree)(parser.root().getTree());

            // semantic analysis if any...
            //  [tbd]

        }
        catch(Exception e)
        {
            // if there was an exception we don't want to process request any further
            throw new RuntimeException(e.getMessage(), e);
        }

        return queryTree;
    }
    /*
     * NODE_COLUMN_ACCESS related functions.
     */

    public static String getColumnFamily(Tree astNode, Iterable<CfDef> cfDefs)
    {
        return getColumnFamily(CliUtils.unescapeSQLString(astNode.getChild(0).getText()), cfDefs);
    }

    public static String getColumnFamily(String cfName, Iterable<CfDef> cfDefs)
    {
        int matches = 0;
        String lastMatchedName = "";

        for (CfDef cfDef : cfDefs)
        {
            if (cfDef.name.equals(cfName))
            {
                return cfName;
            }
            else if (cfDef.name.toUpperCase().equals(cfName.toUpperCase()))
            {
                lastMatchedName = cfDef.name;
                matches++;
            }
        }

        if (matches > 1 || matches == 0)
            throw new RuntimeException(cfName + " not found in current keyspace.");

        return lastMatchedName;
    }

    public static String getKeySpace(Tree statement, List<KsDef> keyspaces)
    {
        return getKeySpace(CliUtils.unescapeSQLString(statement.getChild(0).getText()), keyspaces);
    }

    public static String getKeySpace(String ksName, List<KsDef> keyspaces)
    {
        int matches = 0;
        String lastMatchedName = "";

        for (KsDef ksDef : keyspaces)
        {
            if (ksDef.name.equals(ksName))
            {
                return ksName;
            }
            else if (ksDef.name.toUpperCase().equals(ksName.toUpperCase()))
            {
                lastMatchedName = ksDef.name;
                matches++;
            }
        }

        if (matches > 1 || matches == 0)
            throw new RuntimeException("Keyspace '" + ksName + "' not found.");

        return lastMatchedName;
    }

    public static String getKey(Tree astNode)
    {
        return CliUtils.unescapeSQLString(astNode.getChild(1).getText());
    }

    public static int numColumnSpecifiers(Tree astNode)
    {
        // Skip over keyspace, column family and rowKey
        return astNode.getChildCount() - 2;
    }

    // Returns the pos'th (0-based index) column specifier in the astNode
    public static String getColumn(Tree astNode, int pos)
    {
        // Skip over keyspace, column family and rowKey
        return CliUtils.unescapeSQLString(astNode.getChild(pos + 2).getText());
    }

}
