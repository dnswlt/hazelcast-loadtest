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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
    private final int numBytes;
    private final int numOffers;
    private final double probSale;
    private final HazelcastInstance client;
    private final Random rnd;
    private final BlockingQueue<Integer> throttleQueue;

    public HazelWorker(Config config, HazelcastInstance client, BlockingQueue<Integer> throttleQueue) {
        this.durationMillis = config.getDurationMillis();
        this.warmupMillis = config.getWarmupMillis();
        this.numBytes = config.getNumBytes();
        this.numOffers = config.getNumOffers();
        this.probSale = config.getProbSale();
        rnd = new Random();
        this.client = client;
        this.throttleQueue = throttleQueue;
    }

    @Override
    public void run() {

        if (warmupMillis > 0) {
            long delayMillis = rnd.nextInt((int) warmupMillis);
            LOGGER.debug("Delaying start of timing for {}ms", delayMillis);
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                LOGGER.error("Ignoring interrupt that occurred during delay.");
            }
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
        int totalIterations = 0;
        int totalOffers = 0;
        int totalSales = 0;
        while (System.nanoTime() < end) {
            if (throttleQueue != null) {
                try {
                    throttleQueue.take();
                } catch (InterruptedException e) {
                    LOGGER.warn("Interrupted while waiting for token from throttleQueue. Aborting.");
                    break;
                }
            }
            ++totalIterations;
            int nOffers = getPoisson(L);
            long before = System.nanoTime();
            String offerId = null;
            for (int i = 0; i < nOffers; ++i) {
                offerId = generateOffer(map, data);
                ++totalOffers;
            }
            long after = System.nanoTime();
            if (rnd.nextDouble() < probSale && offerId != null) {
                synchronized (offerQueueLock) {
                    // Add offer to queue
                    offerQueue.addLast(new QueueEntry<>(offerId));
                }
            }
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
        synchronized (offerQueueLock) {
            key = offerQueue.pollFirst();
        }
        if (key == null) {
            return false; // nothing in queue
        }
        byte[] data = map.get(key.getValue());
        if (data == null) {
            LOGGER.warn("Eviction has removed key {}. Clearing offer queue.", key.getValue());
            synchronized (offerQueueLock) {
                offerQueue.clear();
            }
            return false;
        }
        return true;
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
