package smartfab.model.events;

import java.util.ArrayList;
import java.util.List;

public class EventDispatcher<T> {
    private final List<EventListener<T>> listeners = new ArrayList<>();

    public void subscribe(EventListener<T> listener) {
        listeners.add(listener);
    }

    public void unsubscribe(EventListener<T> listener) {
        listeners.remove(listener);
    }

    public void notify(T event) {
        for (EventListener<T> listener : listeners) {
            listener.onEvent(event);
        }
    }
}