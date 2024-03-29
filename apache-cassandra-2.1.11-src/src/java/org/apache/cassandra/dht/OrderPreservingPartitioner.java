

public class OrderPreservingPartitioner extends AbstractPartitioner
{
    public static final StringToken MINIMUM = new StringToken("");

    public static final BigInteger CHAR_MASK = new BigInteger("65535");

    private static final long EMPTY_SIZE = ObjectSizes.measure(MINIMUM);

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return new BufferDecoratedKey(getToken(key), key);
    }

    public StringToken midpoint(Token ltoken, Token rtoken)
    {
        int sigchars = Math.max(((StringToken)ltoken).token.length(), ((StringToken)rtoken).token.length());
        BigInteger left = bigForString(((StringToken)ltoken).token, sigchars);
        BigInteger right = bigForString(((StringToken)rtoken).token, sigchars);

        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(left, right, 16*sigchars);
        return new StringToken(stringForBig(midpair.left, sigchars, midpair.right));
    }

    /**
     * Copies the characters of the given string into a BigInteger.
     *
     * TODO: Does not acknowledge any codepoints above 0xFFFF... problem?
     */
    private static BigInteger bigForString(String str, int sigchars)
    {
        assert str.length() <= sigchars;

        BigInteger big = BigInteger.ZERO;
        for (int i = 0; i < str.length(); i++)
        {
            int charpos = 16 * (sigchars - (i + 1));
            BigInteger charbig = BigInteger.valueOf(str.charAt(i) & 0xFFFF);
            big = big.or(charbig.shiftLeft(charpos));
        }
        return big;
    }

    /**
     * Convert a (positive) BigInteger into a String.
     * If remainder is true, an additional char with the high order bit enabled
     * will be added to the end of the String.
     */
    private String stringForBig(BigInteger big, int sigchars, boolean remainder)
    {
        char[] chars = new char[sigchars + (remainder ? 1 : 0)];
        if (remainder)
            // remaining bit is the most significant in the last char
            chars[sigchars] |= 0x8000;
        for (int i = 0; i < sigchars; i++)
        {
            int maskpos = 16 * (sigchars - (i + 1));
            // apply bitmask and get char value
            chars[i] = (char)(big.and(CHAR_MASK.shiftLeft(maskpos)).shiftRight(maskpos).intValue() & 0xFFFF);
        }
        return new String(chars);
    }

    public StringToken getMinimumToken()
    {
        return MINIMUM;
    }

    public StringToken getRandomToken()
    {
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random r = new Random();
        StringBuilder buffer = new StringBuilder();
        for (int j = 0; j < 16; j++) {
            buffer.append(chars.charAt(r.nextInt(chars.length())));
        }
        return new StringToken(buffer.toString());
    }

    private final Token.TokenFactory tokenFactory = new Token.TokenFactory()
    {
        public ByteBuffer toByteArray(Token token)
        {
            StringToken stringToken = (StringToken) token;
            return ByteBufferUtil.bytes(stringToken.token);
        }

        public Token fromByteArray(ByteBuffer bytes)
        {
            try
            {
                return new StringToken(ByteBufferUtil.string(bytes));
            }
        }

        public String toString(Token token)
        {
            StringToken stringToken = (StringToken) token;
            return stringToken.token;
        }

        public void validate(String token) throws ConfigurationException
        {
            if (token.contains(VersionedValue.DELIMITER_STR))
                throw new ConfigurationException("Tokens may not contain the character " + VersionedValue.DELIMITER_STR);
        }

        public Token fromString(String string)
        {
            return new StringToken(string);
        }
    };

    public Token.TokenFactory getTokenFactory()
    {
        return tokenFactory;
    }

    public boolean preservesOrder()
    {
        return true;
    }

    public StringToken getToken(ByteBuffer key)
    {
        String skey;
        try
        {
            skey = ByteBufferUtil.string(key);
        }
        return new StringToken(skey);
    }

    public long getHeapSizeOf(Token token)
    {
        return EMPTY_SIZE + ObjectSizes.sizeOf(((StringToken) token).token);
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        // allTokens will contain the count and be returned, sorted_ranges is shorthand for token<->token math.
        Map<Token, Float> allTokens = new HashMap<Token, Float>();
        List<Range<Token>> sortedRanges = new ArrayList<Range<Token>>(sortedTokens.size());

        // this initializes the counts to 0 and calcs the ranges in order.
        Token lastToken = sortedTokens.get(sortedTokens.size() - 1);
        for (Token node : sortedTokens)
        {
            allTokens.put(node, new Float(0.0));
            sortedRanges.add(new Range<Token>(lastToken, node));
            lastToken = node;
        }

        for (String ks : Schema.instance.getKeyspaces())
        {
            for (CFMetaData cfmd : Schema.instance.getKSMetaData(ks).cfMetaData().values())
            {
                for (Range<Token> r : sortedRanges)
                {
                    // Looping over every KS:CF:Range, get the splits size and add it to the count
                    allTokens.put(r.right, allTokens.get(r.right) + StorageService.instance.getSplits(ks, cfmd.cfName, r, cfmd.getMinIndexInterval()).size());
                }
            }
        }

        // Sum every count up and divide count/total for the fractional ownership.
        Float total = new Float(0.0);
        for (Float f : allTokens.values())
            total += f;
        for (Map.Entry<Token, Float> row : allTokens.entrySet())
            allTokens.put(row.getKey(), row.getValue() / total);

        return allTokens;
    }

    public AbstractType<?> getTokenValidator()
    {
        return UTF8Type.instance;
    }
}
