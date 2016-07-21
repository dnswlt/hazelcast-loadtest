package de.reondo.hazelcast.client;

import java.util.List;

/**
 * Created by dwalter on 30.06.2016.
 */
public class Config {

    private long durationMillis;
    private int numBytes;
    private int numOffers;
    private double probSale;
    private int numThreads;
    private long warmupMillis;
    private long pauseMillis;
    private boolean throttlingEnabled;
    private List<String> hazelcastHosts;
    private final boolean clearMap;

    /**
     * Prefer ConfigBuilder to construct a Config.
     */
    Config(long durationMillis, int numBytes, int numOffers, double probSale, int numThreads, long warmupMillis,
           long pauseMillis, boolean throttlingEnabled, List<String> hazelcastHosts, boolean clearMap) {
        this.durationMillis = durationMillis;
        this.numBytes = numBytes;
        this.numOffers = numOffers;
        this.probSale = probSale;
        this.numThreads = numThreads;
        this.warmupMillis = warmupMillis;
        this.pauseMillis = pauseMillis;
        this.throttlingEnabled = throttlingEnabled;
        this.hazelcastHosts = hazelcastHosts;
        this.clearMap = clearMap;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    public int getNumBytes() {
        return numBytes;
    }

    public int getNumOffers() {
        return numOffers;
    }

    public double getProbSale() {
        return probSale;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public long getWarmupMillis() {
        return warmupMillis;
    }

    public long getPauseMillis() {
        return pauseMillis;
    }

    public boolean isThrottlingEnabled() {
        return throttlingEnabled;
    }

    public List<String> getHazelcastHosts() {
        return hazelcastHosts;
    }

    public boolean isClearMap() {
        return clearMap;
    }

    @Override
    public String toString() {
        return "Config{" +
                "durationMillis=" + durationMillis +
                ", numBytes=" + numBytes +
                ", numOffers=" + numOffers +
                ", probSale=" + probSale +
                ", numThreads=" + numThreads +
                ", warmupMillis=" + warmupMillis +
                ", pauseMillis=" + pauseMillis +
                ", throttlingEnabled=" + throttlingEnabled +
                ", hazelcastHosts=" + hazelcastHosts +
                ", clearMap=" + clearMap +
                '}';
    }
}

