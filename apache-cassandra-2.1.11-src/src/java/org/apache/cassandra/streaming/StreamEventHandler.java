

public interface StreamEventHandler extends FutureCallback<StreamState>
{
    /**
     * Callback for various streaming events.
     *
     * @see StreamEvent.Type
     * @param event Stream event.
     */
    void handleStreamEvent(StreamEvent event);
}
