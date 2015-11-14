
/**
 * ValidationComplete message is sent when validation compaction completed successfully.
 *
 * @since 2.0
 */
public class ValidationComplete extends RepairMessage
{
    public static MessageSerializer serializer = new ValidationCompleteSerializer();

    /** true if validation success, false otherwise */
    public final boolean success;
    /** Merkle hash tree response. Null if validation failed. */
    public final MerkleTree tree;

    public ValidationComplete(RepairJobDesc desc)
    {
        super(Type.VALIDATION_COMPLETE, desc);
        this.success = false;
        this.tree = null;
    }

    public ValidationComplete(RepairJobDesc desc, MerkleTree tree)
    {
        super(Type.VALIDATION_COMPLETE, desc);
        assert tree != null;
        this.success = true;
        this.tree = tree;
    }
}
