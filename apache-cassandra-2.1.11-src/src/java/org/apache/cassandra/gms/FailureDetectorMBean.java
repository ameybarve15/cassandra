
public interface FailureDetectorMBean
{
    public void dumpInterArrivalTimes();

    public void setPhiConvictThreshold(double phi);

    public double getPhiConvictThreshold();

    public String getAllEndpointStates();

    public String getEndpointState(String address) throws UnknownHostException;

    public Map<String, String> getSimpleStates();

    public int getDownEndpointCount();

    public int getUpEndpointCount();
}
