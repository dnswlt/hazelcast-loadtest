package de.reondo.hazelcast.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import static de.reondo.hazelcast.client.StatEntry.Type.READ;
import static de.reondo.hazelcast.client.StatEntry.Type.WRITE;

/**
 * Created by dwalter on 13.06.2016.
 */
public class StatSummary {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatSummary.class);

    private final List<StatEntry> entries;
    private boolean successOnly;

    public StatSummary(List<StatEntry> entries) {
        Collections.sort(entries, new Comparator<StatEntry>() {
            @Override
            public int compare(StatEntry o1, StatEntry o2) {
                long diff = o1.getStartNanos() - o2.getStartNanos();
                if (diff < 0) {
                    return -1;
                } else if (diff == 0) {
                    return 0;
                } else {
                    return 1;
                }
            }
        });
        this.entries = entries;
    }

    private File getStatFile() {
        return new File(String.format("timings_%s.csv",
                new SimpleDateFormat("yyyy-MM-dd'T'HHmmss").format(new Date())));
    }

    public long getReadMin() {
        return getMin(READ);
    }

    public long getReadMax() {
        return getMax(READ);
    }

    public double getReadMeanMillis() {
        return getMeanMillis(READ);
    }

    public long getReadMedian() {
        return getMedian(READ);
    }

    public long getReadPercentile(double perc) {
        return getPercentile(READ, perc);
    }

    public long getWriteMin() {
        return getMin(WRITE);
    }

    public long getWriteMax() {
        return getMax(WRITE);
    }

    public double getWriteMeanMillis() {
        return getMeanMillis(WRITE);
    }

    public long getWriteMedian() {
        return getMedian(WRITE);
    }

    public long getWritePercentile(double perc) {
        return getPercentile(WRITE, perc);
    }


    private long getMin(StatEntry.Type type) {
        long min = Long.MAX_VALUE;
        for (StatEntry e : entries) {
            if (e.getType() == type && (!successOnly || e.isSuccess())) {
                min = Math.min(min, e.getDurationNanos());
            }
        }
        return min;
    }

    private long getMax(StatEntry.Type type) {
        long readMax = Long.MIN_VALUE;
        for (StatEntry e : entries) {
            if (e.getType() == type && (!successOnly || e.isSuccess())) {
                readMax = Math.max(readMax, e.getDurationNanos());
            }
        }
        return readMax;
    }

    private double getMeanMillis(StatEntry.Type type) {
        long sum = 0;
        int total = 0;
        for (StatEntry e : entries) {
            if (e.getType() == type && (!successOnly || e.isSuccess())) {
                sum += e.getDurationNanos() / App.NANOS_IN_MILLIS;
                ++total;
            }
        }
        return sum / (double) Math.max(1, total);
    }

    private long getMedian(StatEntry.Type type) {
        return getPercentile(type, 0.5);
    }

    private long getPercentile(StatEntry.Type type, double p) {
        int count = 0;
        for (StatEntry e : entries) {
            if (e.getType() == type && (!successOnly || e.isSuccess())) {
                ++count;
            }
        }
        if (count == 0) {
            return 0;
        }
        long[] values = new long[count];
        count = 0;
        for (StatEntry e : entries) {
            if (e.getType() == type && (!successOnly || e.isSuccess())) {
                values[count++] = e.getDurationNanos();
            }
        }
        Arrays.sort(values);

        int index = (int) Math.ceil(p * values.length);
        return values[Math.max(index - 1, 0)];
    }


    public static Builder builder(List<StatEntry> entries) {
        return new Builder(entries);
    }

    /**
     * Returns read throughput as number of reads divided by total time from first to (last read + last read duration).
     *
     * @return Unit: reads/s
     */
    public double getReadThroughput() {
        return getThroughput(READ);
    }

    /**
     * See {@link #getReadThroughput()}.
     */
    public double getWriteThroughput() {
        return getThroughput(WRITE);
    }

    private double getThroughput(StatEntry.Type type) {
        long minStart = Long.MAX_VALUE;
        long maxDuration = Long.MIN_VALUE;
        int count = 0;
        for (StatEntry e : entries) {
            if (e.getType() == type && (!successOnly || e.isSuccess())) {
                ++count;
                minStart = Math.min(minStart, e.getStartNanos());
                maxDuration = Math.max(maxDuration, e.getStartNanos() + e.getDurationNanos());
            }
        }
        return count / ((double) (maxDuration - minStart) / (App.NANOS_IN_MILLIS * 1000));
    }

    /**
     * Write all entries sorted by start time to given file.
     */
    public void saveEntries() {
        File file = getStatFile();
        try (Writer writer = new BufferedWriter(new FileWriter(file))) {
            writer.write("ThreadId;Type;Success;Start;Duration\n");
            for (StatEntry e : entries) {
                writer.write(String.format("%d;%s;%s;%d;%d\n",
                        e.getThreadId(),
                        e.getType() == READ ? "R" : "W",
                        e.isSuccess() ? "1" : "0",
                        e.getStartNanos()/App.NANOS_IN_MILLIS,
                        e.getDurationNanos()/App.NANOS_IN_MILLIS));
            }
            LOGGER.info("Saved result to " + file);
        } catch (IOException e) {
            LOGGER.error("Could not save statistics to {}: {}", file, e.getMessage());
        }
    }

    public int getReadCount() { return getTotalCount(READ); }

    public int getWriteCount() { return getTotalCount(WRITE); }

    private int getTotalCount(StatEntry.Type type) {
        int count = 0;
        for (StatEntry e : entries) {
            if (e.getType() == type && (!successOnly || e.isSuccess())) {
                ++count;
            }
        }
        return count;
    }

    public double getReadSuccessRate() {
        return getSuccessRate(READ);
    }

    public double getWriteSuccessRate() {
        return getSuccessRate(WRITE);
    }

    private double getSuccessRate(StatEntry.Type type) {
        int totalCount = 0;
        int successCount = 0;
        for (StatEntry e : entries) {
            if (e.getType() == type) {
                ++totalCount;
                if (e.isSuccess()) {
                    ++successCount;
                }
            }
        }
        return ((double) successCount) / totalCount;
    }

    public static class Builder {

        private final StatSummary instance;

        public Builder(List<StatEntry> entries) {
            instance = new StatSummary(entries);
        }

        public Builder withSuccessOnly(boolean successOnly) {
            instance.successOnly = successOnly;
            return this;
        }

        public StatSummary build() {
            return instance;
        }

    }

}
