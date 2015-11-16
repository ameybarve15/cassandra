

public interface IRequestCoordinator<R>
{
    void add(R request);

    void start();

    // Returns how many request remains
    int completed(R request);
}
