package smartfab.model.events;

public interface EventListener<T> {
    void onEvent(T event);
}