package smartfab.model.events;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Gianluca bianchi
 * 
 *      THREAD-SAFE
 *      Event Dispatcher for implementing the event-listener pattern
 *      that drives the flow of the {@link smartfab.model.events.ProductionLineEvent}
 *      events.
 *      NOTE: the method for notify the listeners it's implemented with the pattern
 *      FIRE-AND-FORGET, so it starts another anonymous thread
 */
public class EventDispatcher<T> {
    private final List<EventListener<T>> listeners = new ArrayList<>();

    public synchronized void subscribe(EventListener<T> listener) {
        listeners.add(listener);
    }

    public synchronized void unsubscribe(EventListener<T> listener) {
        listeners.remove(listener);
    }

    /**
     * Starts new anonymous thread for sending each message
     * @param event
     */
    public synchronized void notify(T event) {
        for (EventListener<T> listener : listeners) {
            new Thread(()->{
                listener.onEvent(event);
            }).start();
        }
    }
}