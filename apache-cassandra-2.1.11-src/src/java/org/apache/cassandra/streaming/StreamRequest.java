

public class StreamRequest
{
    public static final IVersionedSerializer<StreamRequest> serializer = new StreamRequestSerializer();

    public final String keyspace;
    public final Collection<Range<Token>> ranges;
    public final Collection<String> columnFamilies = new HashSet<>();
    public final long repairedAt;
    public StreamRequest(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies, long repairedAt)
    {
        this.keyspace = keyspace;
        this.ranges = ranges;
        this.columnFamilies.addAll(columnFamilies);
        this.repairedAt = repairedAt;
    }

    public static class StreamRequestSerializer implements IVersionedSerializer<StreamRequest>
    {
        public void serialize(StreamRequest request, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(request.keyspace);
            out.writeLong(request.repairedAt);
            out.writeInt(request.ranges.size());
            for (Range<Token> range : request.ranges)
            {
                Token.serializer.serialize(range.left, out);
                Token.serializer.serialize(range.right, out);
            }
            out.writeInt(request.columnFamilies.size());
            for (String cf : request.columnFamilies)
                out.writeUTF(cf);
        }

        public StreamRequest deserialize(DataInput in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            long repairedAt = in.readLong();
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
            {
                Token left = Token.serializer.deserialize(in);
                Token right = Token.serializer.deserialize(in);
                ranges.add(new Range<>(left, right));
            }
            int cfCount = in.readInt();
            List<String> columnFamilies = new ArrayList<>(cfCount);
            for (int i = 0; i < cfCount; i++)
                columnFamilies.add(in.readUTF());
            return new StreamRequest(keyspace, ranges, columnFamilies, repairedAt);
        }

        public long serializedSize(StreamRequest request, int version)
        {
            int size = TypeSizes.NATIVE.sizeof(request.keyspace);
            size += TypeSizes.NATIVE.sizeof(request.repairedAt);
            size += TypeSizes.NATIVE.sizeof(request.ranges.size());
            for (Range<Token> range : request.ranges)
            {
                size += Token.serializer.serializedSize(range.left, TypeSizes.NATIVE);
                size += Token.serializer.serializedSize(range.right, TypeSizes.NATIVE);
            }
            size += TypeSizes.NATIVE.sizeof(request.columnFamilies.size());
            for (String cf : request.columnFamilies)
                size += TypeSizes.NATIVE.sizeof(cf);
            return size;
        }
    }
}
