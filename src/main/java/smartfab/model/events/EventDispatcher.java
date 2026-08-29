package smartfab.model.events;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Gianluca bianchi
 *
 *      THREAD-SAFE event dispatcher implementing the event-listener pattern.
 *
 *      Delivery is ASYNCHRONOUS but ORDERED: events are queued and drained by a
 *      single dispatch thread, so listeners observe them in the order they were
 *      published.
 *
 *      It used to fire "new Thread(...)" per listener per event, which is
 *      fire-and-forget but gives NO ordering guarantee: three transitions
 *      published as IDLE, WAITING, CALIBRATING could reach a listener in any
 *      order, and a status display would show the wrong state. One pump thread
 *      also costs a lot less than one thread per event.
 *
 *      Asynchronous it must stay: a listener may block for seconds (a
 *      calibration is simulated by sleeping inside onEvent), and the publisher
 *      is often a gRPC server thread that must not be held up.
 */
public class EventDispatcher<T> {

    private final List<EventListener<T>> listeners = new ArrayList<>();
    private final LinkedList<T>          queue     = new LinkedList<>();

    /** Started on the first publish, so a dispatcher nobody uses costs nothing. */
    private Thread pump;

    public synchronized void subscribe(EventListener<T> listener) {
        this.listeners.add(listener);
    }

    public synchronized void unsubscribe(EventListener<T> listener) {
        this.listeners.remove(listener);
    }

    /**
     * Queues the event for delivery and returns immediately.
     *
     * Named publish, not notify, because this class is meant to be composed
     * into objects that also use the intrinsic monitor: having notify(T) next
     * to Object.notify() in the same class is a trap.
     */
    public synchronized void publish(T event) {
        this.queue.addLast(event);
        startPumpIfNeeded();
        notifyAll();
    }

    /** Must be called with the monitor held. */
    private void startPumpIfNeeded() {
        if (this.pump != null) {
            return;
        }
        this.pump = new Thread(this::pumpLoop, "event-dispatcher");
        this.pump.setDaemon(true);   // must never keep the JVM alive on exit
        this.pump.start();
    }

    private void pumpLoop() {
        while (true) {
            final T                    event;
            final List<EventListener<T>> targets;

            synchronized (this) {
                while (this.queue.isEmpty()) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                event   = this.queue.removeFirst();
                targets = List.copyOf(this.listeners);
            }

            /*
             * Outside the monitor: a listener is allowed to be slow, and
             * holding the lock here would block every publisher.
             */
            for (EventListener<T> listener : targets) {
                try {
                    listener.onEvent(event);
                } catch (RuntimeException e) {
                    System.err.println("Listener failed on " + event + ": " + e.getMessage());
                }
            }
        }
    }
}
