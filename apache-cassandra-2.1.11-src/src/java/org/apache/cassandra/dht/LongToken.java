

public class LongToken extends Token
{
    static final long serialVersionUID = -5833580143318243006L;

    final long token;


    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null || this.getClass() != obj.getClass())
            return false;

        return token == (((LongToken)obj).token);
    }
}
