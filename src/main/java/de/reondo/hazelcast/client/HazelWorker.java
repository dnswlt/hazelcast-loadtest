package de.reondo.hazelcast.client;

import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static de.reondo.hazelcast.client.App.NANOS_IN_MILLIS;
import static de.reondo.hazelcast.client.StatEntry.Type.READ;
import static de.reondo.hazelcast.client.StatEntry.Type.WRITE;

/**
 * Worker thread Runnable for NOVA Hazelcast load test
 * <p>
 * Created by dwalter on 13.06.2016.
 */
public class HazelWorker implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HazelWorker.class);

    private static final Deque<QueueEntry<String>> offerQueue = new LinkedList<>();
    private static final Object offerQueueLock = new Object();
    private static final AtomicInteger totalOfferCounter = new AtomicInteger();

    private static final List<StatEntry> statEntries = new ArrayList<>();
    private static final Object statEntriesLock = new Object();
    public static final int STAT_ENTRIES_LIMIT = 1000000;

    private final long durationMillis;
    private final long warmupMillis;
    private static long pauseMillis;
    private final int numBytes;
    private final int numOffers;
    private final double probSale;
    private final HazelcastInstance client;
    private final Random rnd;
    private final boolean throttlingEnabled;
    private int queueClock;
    private int totalOfferCounterOnLastQueueReset;

    public HazelWorker(Config config, HazelcastInstance client) {
        this.durationMillis = config.getDurationMillis();
        this.warmupMillis = config.getWarmupMillis();
        this.pauseMillis = config.getPauseMillis();
        this.numBytes = config.getNumBytes();
        this.numOffers = config.getNumOffers();
        this.probSale = config.getProbSale();
        this.throttlingEnabled = config.isThrottlingEnabled();
        rnd = new Random();
        this.client = client;
    }

    @Override
    public void run() {

        long delayMillis = rnd.nextInt((int)warmupMillis);
        LOGGER.debug("Delaying start of timing for {}ms", delayMillis);
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            LOGGER.error("Ignoring interrupt that occurred during delay.");
        }
        Map<String, byte[]> map = client.getMap(App.ANGEBOTE);
        doTiming(map);
    }

    private void doTiming(Map<String, byte[]> map) {
        LOGGER.info("Starting timing");
        double lambda = (double) numOffers; // lambda of Poisson distribution (= expected value and = variance)
        final double L = Math.exp(-lambda);
        byte[] data = new byte[numBytes];
        long threadId = Thread.currentThread().getId();
        long started = System.nanoTime();
        final long end = started + durationMillis * NANOS_IN_MILLIS;
        long now = started;
        int totalIterations = 0;
        int totalOffers = 0;
        int totalSales = 0;
        while (System.nanoTime() < end) {
            ++totalIterations;
            int nOffers = getPoisson(L);
            long before = System.nanoTime();
            for (int i = 0; i < nOffers; ++i) {
                String offerId = generateOffer(map, data);
                ++totalOffers;
                if (i == 0) {
                    synchronized (offerQueueLock) {
                        int len = offerQueue.size();
                        if (len == 0 || rnd.nextDouble() < probSale) {
                            // Add offer to queue
                            offerQueue.addLast(new QueueEntry<>(offerId));
                            if (len > 100000) {
                                while (len-- > 50000) {
                                    offerQueue.removeFirst(); // avoid oversized queue
                                }
                                LOGGER.debug("Dumped 50% oldest offerQueue entries");
                            }
                        }
                    }
                }
            }
            long after = System.nanoTime();
            addStat(WRITE, threadId, before - started, after - before, true);
            if (rnd.nextDouble() < probSale) {
                // buy an offer according to look-book ratio
                before = System.nanoTime();
                boolean success = buyOffer(map);
                if (success) {
                    ++totalSales;
                }
                after = System.nanoTime();
                addStat(READ, threadId, before - started, after - before, success);
            }
            if (pauseMillis > 0) {
                try {
                    Thread.sleep(pauseMillis);
                } catch (InterruptedException e) {
                    LOGGER.warn("Ignoring interrupt while pausing.");
                }
            }
        }
        long durationMillis = (System.nanoTime() - started) / App.NANOS_IN_MILLIS;
        LOGGER.info("Timing done, {} offers created and {} offers bought in {} iterations and {}ms.", totalOffers, totalSales, totalIterations, durationMillis);
    }

    private void addStat(StatEntry.Type type, long threadId, long startNanos, long durationNanos, boolean success) {
        List<StatEntry> saveBlock = null;
        synchronized (statEntriesLock) {
            statEntries.add(new StatEntry(type, threadId, success, startNanos, durationNanos));
            if (statEntries.size() > STAT_ENTRIES_LIMIT) {
                // save stats every 1'000'000 entries to avoid OOM
                saveBlock = getStatEntries();
                statEntries.clear();
            }
        }
        if (saveBlock != null) {
            LOGGER.info("Cleared {} stat entries to restrict memory consumption.", saveBlock.size());
            StatSummary summary = new StatSummary(saveBlock);
            summary.saveEntries();
        }
    }

    /**
     * Take an offer from the offerQueue, and "buy" it, i.e. read it from the map.
     *
     * @param map
     */
    private boolean buyOffer(Map<String, byte[]> map) {
        QueueEntry<String> key;
        int qClock = 0;
        synchronized (offerQueueLock) {
            key = offerQueue.pollFirst();
            qClock = queueClock;
        }
        if (key == null) {
//            LOGGER.error("Offer queue is empty, load test bug.");
            return false;
        }
        byte[] data = map.get(key.getValue());
        if (data == null) {
            if (throttlingEnabled) {
                throttle(qClock, map, key);
            }
            return false;
        }
        if (data.length != numBytes) {
            LOGGER.error("Offer does not have size. Expected={}, actual={}.", numBytes, data.length);
            return false;
        }
        return true;
    }

    private void throttle(int qClock, Map<String, byte[]> map, QueueEntry<String> key) {
        int delta = 0;
        boolean doThrottle = true;
        int queueSize = 0;
        int mapSize = 0;
        synchronized (offerQueueLock) {
            int cntNow = totalOfferCounter.get();
            if (qClock == queueClock) {
                queueSize = offerQueue.size();
                mapSize = map.size();
                offerQueue.clear();
                queueClock++;
                pauseMillis = Math.max(20, pauseMillis + pauseMillis/2);
                delta = cntNow - totalOfferCounterOnLastQueueReset;
                totalOfferCounterOnLastQueueReset = cntNow;
            } else {
                doThrottle = false;
            }
        }
        if (doThrottle) {
            LOGGER.debug("Offer for key {} not found in map. Queue size {}, Map size {}, " +
                            "map inserts since last throttling {}. Throttling threads.",
                    key, queueSize, mapSize, delta);
            LOGGER.info("Cleared queue, throttled threads to pauseMillis={}", pauseMillis);
        }
    }

    /**
     * Generate random bytes into data, and store with random UUID key in Hazelcast.
     *
     * @param map
     * @param data array to be stored, will be updated with random data
     * @return UUID key
     */
    private String generateOffer(Map<String, byte[]> map, byte[] data) {
        rnd.nextBytes(data);
        String key = UUID.randomUUID().toString();
        map.put(key, data);
        totalOfferCounter.incrementAndGet();
        return key;
    }

    public int getPoisson(double L) {
        double p = 1.0;
        int k = 0;
        do {
            k++;
            p *= rnd.nextDouble();
        } while (p > L);

        return k - 1;
    }


    public static List<StatEntry> getStatEntries() {
        synchronized (statEntriesLock) {
            return new ArrayList<>(statEntries);
        }
    }

}
