
public class DatacenterAwareRequestCoordinator implements IRequestCoordinator<InetAddress>
{
    private Map<String, Queue<InetAddress>> requestsByDatacenter = new HashMap<>();
    private int remaining = 0;
    private final IRequestProcessor<InetAddress> processor;

    protected DatacenterAwareRequestCoordinator(IRequestProcessor<InetAddress> processor)
    {
        this.processor = processor;
    }

    public void add(InetAddress request)
    {
        String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(request);
        Queue<InetAddress> queue = requestsByDatacenter.get(dc);
        if (queue == null)
        {
            queue = new LinkedList<>();
            requestsByDatacenter.put(dc, queue);
        }
        queue.add(request);
        remaining++;
    }

    public void start()
    {
        for (Queue<InetAddress> requests : requestsByDatacenter.values())
        {
            if (!requests.isEmpty())
              processor.process(requests.peek());
        }
    }

    // Returns how many request remains
    public int completed(InetAddress request)
    {
        String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(request);
        Queue<InetAddress> requests = requestsByDatacenter.get(dc);
        assert requests != null;
        assert request.equals(requests.peek());
        requests.poll();
        if (!requests.isEmpty())
            processor.process(requests.peek());
        return --remaining;
    }
}
