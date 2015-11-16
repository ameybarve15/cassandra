

public class SequentialRequestCoordinator<R> implements IRequestCoordinator<R>
{
    private final Queue<R> requests = new LinkedList<>();
    private final IRequestProcessor<R> processor;

    public SequentialRequestCoordinator(IRequestProcessor<R> processor)
    {
        this.processor = processor;
    }

    @Override
    public void add(R request)
    {
        requests.add(request);
    }

    @Override
    public void start()
    {
        if (requests.isEmpty())
            return;

        processor.process(requests.peek());
    }

    @Override
    public int completed(R request)
    {
        assert request.equals(requests.peek());
        requests.poll();
        int remaining = requests.size();
        if (remaining != 0)
            processor.process(requests.peek());
        return remaining;
    }
}
