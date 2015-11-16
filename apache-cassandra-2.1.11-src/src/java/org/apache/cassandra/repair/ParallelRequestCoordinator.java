

public class ParallelRequestCoordinator<R> implements IRequestCoordinator<R>
{
    private final Set<R> requests = new HashSet<>();
    private final IRequestProcessor<R> processor;

    public ParallelRequestCoordinator(IRequestProcessor<R> processor)
    {
        this.processor = processor;
    }

    @Override
    public void add(R request) { requests.add(request); }

    @Override
    public void start()
    {
        for (R request : requests)
            processor.process(request);
    }

    @Override
    public int completed(R request)
    {
        requests.remove(request);
        return requests.size();
    }
}
