package smartfab.model.events;

import java.util.ArrayList;
import java.util.List;

public class EventDispatcher<T> {
    private final List<EventListener<T>> listeners = new ArrayList<>();

    public synchronized void subscribe(EventListener<T> listener) {
        listeners.add(listener);
    }

    public synchronized void unsubscribe(EventListener<T> listener) {
        listeners.remove(listener);
    }

    public synchronized void notify(T event) {
        for (EventListener<T> listener : listeners) {
            listener.onEvent(event);
        }
    }
}