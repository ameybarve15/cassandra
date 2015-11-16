
public class Operation
{
    public static enum OperationType
    { PLUS, MINUS }

    public final OperationType type;
    public final Term a, b;

    // unary operation
    public Operation(Term a)
    {
        this.a = a;
        type = null;
        b = null;
    }

    // binary operation
    public Operation(Term a, OperationType type, Term b)
    {
        this.a = a;
        this.type = type;
        this.b = b;
    }

    public boolean isUnary()
    {
        return type == null && b == null;
    }
}
