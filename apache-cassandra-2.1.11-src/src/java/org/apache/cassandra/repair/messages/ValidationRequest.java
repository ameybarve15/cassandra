

/**
 * ValidationRequest
 *
 * @since 2.0
 */
public class ValidationRequest extends RepairMessage
{
    public static MessageSerializer serializer = new ValidationRequestSerializer();

    public final int gcBefore;

    public ValidationRequest(RepairJobDesc desc, int gcBefore)
    {
        super(Type.VALIDATION_REQUEST, desc);
        this.gcBefore = gcBefore;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValidationRequest that = (ValidationRequest) o;
        return gcBefore == that.gcBefore;
    }

    @Override
    public int hashCode()
    {
        return gcBefore;
    }

}
