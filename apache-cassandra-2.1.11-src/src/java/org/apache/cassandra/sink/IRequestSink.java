

public interface IRequestSink
{
    /**
     * Transform or drop a write request (represented by a Mutation).
     *
     * @param mutation the Mutation to be applied locally.
     * @return null if the mutation is to be dropped, or the transformed mutation to apply, which may be just
     * the original mutation.
     */
    IMutation handleWriteRequest(IMutation mutation);
}
