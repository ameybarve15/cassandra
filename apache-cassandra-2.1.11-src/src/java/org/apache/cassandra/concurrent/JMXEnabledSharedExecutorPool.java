

public class JMXEnabledSharedExecutorPool extends SharedExecutorPool
{

    public static final JMXEnabledSharedExecutorPool SHARED = new JMXEnabledSharedExecutorPool("SharedPool");

    public JMXEnabledSharedExecutorPool(String poolName)
    {
        super(poolName);
    }

    public interface JMXEnabledSEPExecutorMBean extends JMXEnabledThreadPoolExecutorMBean
    {
    }

    public class JMXEnabledSEPExecutor extends SEPExecutor implements JMXEnabledSEPExecutorMBean
    {

        private final SEPMetrics metrics;
        private final String mbeanName;

        public JMXEnabledSEPExecutor(int poolSize, int maxQueuedLength, String name, String jmxPath)
        {
            super(JMXEnabledSharedExecutorPool.this, poolSize, maxQueuedLength);
            metrics = new SEPMetrics(this, jmxPath, name);

            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbeanName = "org.apache.cassandra." + jmxPath + ":type=" + name;

            mbs.registerMBean(this, new ObjectName(mbeanName));
        }

        private void unregisterMBean()
        {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(mbeanName));

            // release metrics
            metrics.release();
        }

        @Override
        public synchronized void shutdown()
        {
            // synchronized, because there is no way to access super.mainLock, which would be
            // the preferred way to make this threadsafe
            if (!isShutdown())
            {
                unregisterMBean();
            }
            super.shutdown();
        }

        public int getCoreThreads()
        {
            return 0;
        }

        public void setCoreThreads(int number)
        {
            throw new UnsupportedOperationException();
        }

        public void setMaximumThreads(int number)
        {
            throw new UnsupportedOperationException();
        }
    }

    public TracingAwareExecutorService newExecutor(int maxConcurrency, int maxQueuedTasks, String name, String jmxPath)
    {
        JMXEnabledSEPExecutor executor = new JMXEnabledSEPExecutor(maxConcurrency, maxQueuedTasks, name, jmxPath);
        executors.add(executor);
        return executor;
    }
}
