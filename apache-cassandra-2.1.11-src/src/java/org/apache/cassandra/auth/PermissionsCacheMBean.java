

public interface PermissionsCacheMBean
{
    public void invalidate();

    public void setValidity(int validityPeriod);

    public int getValidity();

    public void setUpdateInterval(int updateInterval);

    public int getUpdateInterval();
}
