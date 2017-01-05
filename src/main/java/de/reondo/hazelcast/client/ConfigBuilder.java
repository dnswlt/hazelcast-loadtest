package de.reondo.hazelcast.client;

import java.util.ArrayList;
import java.util.List;

public class ConfigBuilder {
    private long durationMillis;
    private int numBytes = 4000;
    private int numOffers = 8;
    private double probSale = 0.2;
    private int numThreads = 1;
    private long warmupMillis = 0;
    private long throughput = 0;
    private List<String> hazelcastHosts = new ArrayList<>();
    private boolean clearMap;

    public ConfigBuilder setDurationMillis(long durationMillis) {
        this.durationMillis = durationMillis;
        return this;
    }

    public ConfigBuilder setNumBytes(int numBytes) {
        this.numBytes = numBytes;
        return this;
    }

    public ConfigBuilder setNumOffers(int numOffers) {
        this.numOffers = numOffers;
        return this;
    }

    public ConfigBuilder setProbSale(double probSale) {
        this.probSale = probSale;
        return this;
    }

    public ConfigBuilder setNumThreads(int numThreads) {
        this.numThreads = numThreads;
        return this;
    }

    public ConfigBuilder setWarmupMillis(long warmupMillis) {
        this.warmupMillis = warmupMillis;
        return this;
    }

    public ConfigBuilder setHazelcastHosts(List<String> hazelcastHosts) {
        this.hazelcastHosts = hazelcastHosts;
        return this;
    }

    public ConfigBuilder setClearMap(boolean clearMap) {
        this.clearMap = clearMap;
        return this;
    }

    public void setThroughput(long throughput) {
        this.throughput = throughput;
    }

    public Config createConfig() {
        return new Config(durationMillis, numBytes, numOffers, probSale, numThreads, warmupMillis, throughput,
                hazelcastHosts, clearMap);
    }
}
