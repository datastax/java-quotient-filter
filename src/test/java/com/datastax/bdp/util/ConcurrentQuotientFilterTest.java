package com.datastax.bdp.util;


import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

public class ConcurrentQuotientFilterTest {

    @Test
    public void testFuzz() throws Exception {
        testFuzzImpl(42);
    }

    /**
     * Insert a bunch of hashes from multiple threads, then have a thread iterate the hashes
     * while others threads removes 10% of the hashes. After 10% of the hashes are removed
     * check that all hashes that should be present are still present.
     * @param seed
     * @throws Exception
     */
    private void testFuzzImpl(long seed) throws Exception {
        //Use available parallelism and increase test size if appropriate
        int processors = Runtime.getRuntime().availableProcessors();
        long hashesPerThread = 1_000_000;
        long numHashes = hashesPerThread * processors;

        Random r = new Random(seed);
        System.out.println("Test fuzz seed " + seed);
        System.out.println("Max memory " + Runtime.getRuntime().maxMemory());

        ConcurrentQuotientFilter cqf = new ConcurrentQuotientFilter(numHashes, 1024, 512);

        AtomicLong containsChecks = new AtomicLong();
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("CQF size " + cqf.allocatedSize() + " contains checks " + containsChecks.getAndSet(0));
                }
            }
        }.start();

        long hashMask = (~0L << (64L - 10L)) | (~0L >>> cqf.fingerprintBits());
        System.out.println("hash mask, fingerprint bits " + cqf.fingerprintBits());
        System.out.println(Long.toBinaryString(hashMask));

        ListeningExecutorService es = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(processors));
        List<ListenableFuture<Object>> tasks = new ArrayList<>();
        List<Set<Long>> storedHashes = new ArrayList<>(processors);

        //Insert the hashes in parallel
        for (int zz = 0; zz < processors; zz++) {
            int index = zz;
            storedHashes.add(new HashSet<>());
            Random r2 = new Random(r.nextLong());
            tasks.add(es.submit(new Runnable() {
                @Override
                public void run() {
                    byte[] toHash = new byte[16];
                    for (int ii = 0; ii < hashesPerThread; ii++) {
                        r2.nextBytes(toHash);
                        long hash = cqf.insert(toHash);
                        storedHashes.get(index).add(hash & hashMask);
                    }

                }
            }, null));
        }

        //Wait for all inserts to complete
        Futures.allAsList(tasks).get();

        //All hashes should return a filter positive
        for (Long hash : Iterables.concat(storedHashes)) {
            assertTrue(cqf.maybeContains(hash));
            containsChecks.incrementAndGet();
        }

        //Used to track when iteration has started (and the iterator has acquired its locks)
        CountDownLatch startedIterating = new CountDownLatch(1);
        Set<Long> found = new HashSet<>();
        //Iterate all the hashes and log what is found
        Thread t = new Thread() {
            @Override
            public void run() {
                cqf.foreach(hash -> {
                    startedIterating.countDown();
                    if (cqf.maybeContains(hash)) {
                        found.add(hash);
                    }
                });
            }
        };

        //Wait for iteration to start and read locks to be acquired
        startedIterating.await();

        //Submit tasks to delete 10% of the hashes
        tasks.clear();
        List<Set<Long>> removedHashes = new ArrayList<>(processors);
        for (int zz = 0; zz < processors; zz++) {
            int index = zz;
            removedHashes.add(new HashSet<>());
            Random r2 = new Random(r.nextLong());
            tasks.add(es.submit(new Runnable() {
                @Override
                public void run() {
                    for (Long hash : storedHashes.get(index)) {
                        if (r2.nextDouble() <= 0.1) {
                            cqf.remove(hash);
                            removedHashes.get(index).add(hash);
                        }
                    }

                }
            }, null));
        }

        //Wait for the iteration to complete
        t.join();
        //Wait for the 10% deletes to complete
        Futures.allAsList(tasks).get();

        //The iterator should have had a read lock on all stripes and found everything
        assertTrue(storedHashes.size() == found.size());

        Set<Long> allRemovedHashes = new HashSet<>();
        Iterables.addAll(allRemovedHashes, Iterables.concat(removedHashes));
        for (Long hash : Iterables.concat(storedHashes)) {
            if (!allRemovedHashes.contains(hash)) {
                assertTrue(cqf.maybeContains(hash));
            }
        }
    }
}
