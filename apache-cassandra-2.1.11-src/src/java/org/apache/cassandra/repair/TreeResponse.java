

/**
 * Merkle tree response sent from given endpoint.
 */
public class TreeResponse
{
    public final InetAddress endpoint;
    public final MerkleTree tree;

    public TreeResponse(InetAddress endpoint, MerkleTree tree)
    {
        this.endpoint = endpoint;
        this.tree = tree;
    }
}
