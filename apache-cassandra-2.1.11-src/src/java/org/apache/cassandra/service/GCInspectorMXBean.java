
public interface GCInspectorMXBean
{
    // returns { interval (ms), max(gc real time (ms)), sum(gc real time (ms)), sum((gc real time (ms))^2), sum(gc bytes), count(gc) }
    public double[] getAndResetStats();
}
