package de.reondo.hazelcast.client;

/**
 * Created by dwalter on 07.07.2016.
 */
public class QueueEntry<T> {

    private T value;
    private long timestamp;

    public QueueEntry(T value) {
        this.value = value;
        this.timestamp = System.nanoTime();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public T getValue() {
        return value;
    }

    public long ageMillis() {
        return (System.nanoTime() - timestamp) / 1000000;
    }

    @Override
    public String toString() {
        return "QueueEntry{" +
                "value=" + value +
                ", age=" + ageMillis() + "ms" +
                "}";
    }
}
