package de.reondo.hazelcast.client;

/**
 * Created by dwalter on 13.06.2016.
 */
public class StatEntry {

    public enum Type {
        READ, WRITE;
    }

    private final Type type;
    private final long threadId;
    private final boolean success;
    private final long startNanos;
    private final long durationNanos;

    public StatEntry(Type type, long threadId, boolean success, long startNanos, long durationNanos) {
        this.type = type;
        this.threadId = threadId;
        this.success = success;
        this.startNanos = startNanos;
        this.durationNanos = durationNanos;
    }

    public Type getType() {
        return type;
    }

    public boolean isSuccess() {
        return success;
    }

    public long getStartNanos() {
        return startNanos;
    }

    public long getDurationNanos() {
        return durationNanos;
    }

    public long getThreadId() {
        return threadId;
    }
}
