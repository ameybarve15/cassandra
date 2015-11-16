

public interface JMXConfigurableThreadPoolExecutorMBean extends JMXEnabledThreadPoolExecutorMBean
{
    void setCorePoolSize(int n);

    int getCorePoolSize();
}