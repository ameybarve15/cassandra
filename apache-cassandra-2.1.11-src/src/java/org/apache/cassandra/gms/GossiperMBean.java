

public interface GossiperMBean
{
    public long getEndpointDowntime(String address) throws UnknownHostException;

    public int getCurrentGenerationNumber(String address) throws UnknownHostException;

    public void unsafeAssassinateEndpoint(String address) throws UnknownHostException;

}