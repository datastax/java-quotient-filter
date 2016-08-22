/*
 *      Copyright (C) 2016 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
//Parts of this file are copyrighted by Vedant Kumar <vsk@berkeley.edu>
//https://github.com/aweisberg/quotient-filter/commit/54539e6e287c7f68139733c65ecc4873e2872d54
/*
 * qf.c
 *
 * Copyright (c) 2014 Vedant Kumar <vsk@berkeley.edu>
 */
/*
        Copyright (c) 2014 Vedant Kumar <vsk@berkeley.edu>

        Permission is hereby granted, free of charge, to any person obtaining a
        copy of this software and associated documentation files (the
        "Software"), to deal in the Software without restriction, including
        without limitation the rights to use, copy, modify, merge, publish,
        distribute, sublicense, and/or sell copies of the Software, and to
        permit persons to whom the Software is furnished to do so, subject to
        the following conditions:

        The above copyright notice and this permission notice shall be included
        in all copies or substantial portions of the Software.  THE SOFTWARE IS
        PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
        INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
        FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
        LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
        FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
        DEALINGS IN THE SOFTWARE.
*/
package com.datastax.bdp.util;

import com.google.common.math.IntMath;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class QuotientFilterTest {
    static final int Q_MAX = 12;
    static final int R_MAX = 6;
    static final int ROUNDS_MAX = 1000;
    static final ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    static final ThreadLocal<Random> random = new ThreadLocal<Random>()
    {
        @Override
        public Random initialValue()
        {
            return new Random();
        }
    };


    /* Check QF structural invariants. */
    static void assertQFInvariants(QuotientFilter qf)
    {
        assertTrue(qf.QUOTIENT_BITS > 0);
        assertTrue(qf.REMAINDER_BITS > 0);
        assertTrue(qf.QUOTIENT_BITS + qf.REMAINDER_BITS <= 63);
        assertTrue(qf.ELEMENT_BITS == (qf.REMAINDER_BITS + 3));
        assertNotNull(qf.table);

        long idx;
        long start;
        long size = qf.MAX_SIZE;
        assertTrue(qf.entries <= size);
        long last_run_elt = Long.MAX_VALUE;
        long visited = 0;

        if (qf.entries == 0)
        {
            for (start = 0; start < size; ++start)
            {
                assert(qf.getElement(start) == 0);
            }
            return;
        }

        for (start = 0; start < size; ++start)
        {
            if (qf.isElementClusterStart(qf.getElement(start)))
            {
                break;
            }
        }

        assertTrue(start < size);

        idx = start;
        do
        {
            long elt = qf.getElement(idx);

             /* Make sure there are no dirty entries. */
            if (qf.isElementEmpty(elt))
            {
                assertTrue(qf.getElementRemainder(elt) == 0);
            }

            /* Check for invalid metadata bits. */
            if (qf.isElementContinuation(elt))
            {
                assertTrue(qf.isElementShifted(elt));

                /* Check that this is actually a continuation. */
                long prev = qf.getElement(qf.decrementIndex(idx));
                assertFalse(qf.isElementEmpty(prev));
            }

            /* Check that remainders within runs are sorted. */
            if (!qf.isElementEmpty(elt))
            {
                long rem = qf.getElementRemainder(elt);
                if (qf.isElementContinuation(elt))
                {
                    assertTrue(rem >= last_run_elt);
                }
                last_run_elt = rem;
                ++visited;
            }

            idx = qf.incrementIndex(idx);
        }
        while (idx != start);

        assertTrue(qf.entries == visited);
    }

    /* Generate a random 64-bit hash. If @clrhigh, clear the high (64-p) bits. */
    long genhash(QuotientFilter qf, boolean clrhigh, Set<Long> keys)
    {
        long hash;
        long mask = clrhigh ? QuotientFilter.LOW_MASK(qf.QUOTIENT_BITS + qf.REMAINDER_BITS) : ~0;
        long size = qf.MAX_SIZE;

        Random random = QuotientFilterTest.random.get();
        /* If the QF is overloaded, use a linear scan to find an unused hash. */
        if (keys.size() > (3 * (size / 4)))
        {
            long probe;
            long start = random.nextLong() & qf.INDEX_MASK;
            for (probe = qf.incrementIndex(start); probe != start; probe = qf.incrementIndex(probe))
            {
                if (qf.isElementEmpty(qf.getElement(probe)))
                {
                    long hi = clrhigh ? 0 : (random.nextLong() & ~mask);
                    hash = hi | (probe << qf.REMAINDER_BITS) | (random.nextLong() & qf.REMAINDER_MASK);
                    if (!keys.contains(hash))
                    {
                        return hash;
                    }
                }
            }
        }

        /* Find a random unused hash. */
        do
        {
            hash = random.nextLong() & mask;
        }
        while (keys.contains(hash));
        return hash;
    }

    /* Insert a random p-bit hash into the QF. */
    void ht_put(QuotientFilter qf, Set<Long> keys)
    {
        long hash = genhash(qf, true, keys);
        qf.insert(hash);
        keys.add(hash);
    }

    /* Remove a hash from the filter. */
    void ht_del(QuotientFilter qf, Set<Long> keys)
    {
        Iterator<Long> i = keys.iterator();
        long idx = random.get().nextLong() % keys.size();
        Long hash = i.next();
        for (int ii = 0; ii < idx; ii++)
        {
            hash = i.next();
        }
        qf.remove(hash);
        assertTrue(!qf.maybeContain(hash));
        keys.remove(hash);
    }

    /* Check that a set of keys are in the QF. */
    void ht_check(QuotientFilter qf, Set<Long> keys)
    {
        assertQFInvariants(qf);
        Iterator<Long> i = keys.iterator();
        while (i.hasNext())
        {
            assertTrue(qf.maybeContain(i.next()));
        }
    }

    void qf_test(QuotientFilter qf)
    {
        Random random = QuotientFilterTest.random.get();
        /* Basic get/set tests. */
        int idx;
        int size = Ints.checkedCast(qf.MAX_INSERTIONS);
        for (idx = 0; idx < size; ++idx)
        {
            assertTrue(qf.getElement(idx) == 0);
            qf.setElement(idx, idx & qf.ELEMENT_MASK);
        }
        for (idx = 0; idx < size; ++idx)
        {
            assertTrue(qf.getElement(idx) == (idx & qf.ELEMENT_MASK));
        }
        qf.clear();

        /* Random get/set tests. */
        long[] elements = new long[size];
        for (idx = 0; idx < size; ++idx)
        {
            int slot = random.nextInt(size);
            long hash = random.nextLong();
            qf.setElement(slot, hash & qf.ELEMENT_MASK);
            elements[slot] = hash & qf.ELEMENT_MASK;
        }
        for (idx = 0; idx < elements.length; ++idx)
        {
            assertTrue(qf.getElement(idx) == elements[idx]);
        }
        qf.clear();

        /* Check: forall x, insert(x) => may-contain(x). */
        Set<Long> keys = new HashSet<>();
        for (idx = 0; idx < size; ++idx)
        {
            long elt = genhash(qf, false, keys);
            qf.insert(elt);
            keys.add(elt);
        }
        ht_check(qf, keys);
        keys.clear();
        qf.clear();

        /* Check that the QF works like a hash set when all keys are p-bit values. */
        for (idx = 0; idx < ROUNDS_MAX; ++idx)
        {
            while (qf.entries < size)
            {
                ht_put(qf, keys);
            }

            while (qf.entries > (size / 2))
            {
                ht_del(qf, keys);
            }
            ht_check(qf, keys);

            QuotientFilter.QFIterator QFIterator = qf.new QFIterator();
            while (QFIterator.hasNext())
            {
                long hash = QFIterator.next();
                assertTrue(keys.contains(hash));
            }
        }
    }

    @Test
    public void qf_bench() throws Exception
    {
        int q_large = 28;
        int q_small = 17;
        int nlookups = 1000000;

        Random tlr = QuotientFilterTest.random.get();

        /* Test random inserts + lookups. */
        int ninserts = (3 * (1 << q_large) / 4);
        System.out.printf("Testing %d random inserts and %d lookups", ninserts, nlookups);
        System.out.flush();
        QuotientFilter qf = new QuotientFilter(q_large, 1);
        long tv1 = System.nanoTime();
        while (qf.entries < ninserts)
        {
            qf.insert(tlr.nextLong());
            if (qf.entries % 10000000 == 0)
            {
                System.out.print(".");
                System.out.flush();
            }
        }

        for (int i = 0; i < nlookups; ++i)
        {
            qf.maybeContain(tlr.nextLong());
        }
        long tv2 = System.nanoTime();
        long sec = TimeUnit.NANOSECONDS.toSeconds(tv2 - tv1);
        System.out.printf(" done (%d seconds).%n", sec);

        /* Create a large cluster. Test random lookups. */
        qf = new QuotientFilter(q_small, 1);
        System.out.printf("Testing %d contiguous inserts and %d lookups", 1 << q_small,
                nlookups);
        System.out.flush();
        tv1 = System.nanoTime();
        for (long quot = 0; quot < (1 << (q_small - 2)); ++quot)
        {
            long hash = quot << 1;
            qf.insert(hash);
            qf.insert(hash | 1);
            if (quot % 2000 == 0)
            {
                System.out.print(".");
                System.out.flush();
            }
        }
        for (int i = 0; i < nlookups; ++i)
        {
            qf.maybeContain(tlr.nextLong());
            if (i % 50000 == 0) {
                System.out.print(".");
                System.out.flush();
            }
        }
        tv2 = System.nanoTime();
        sec = TimeUnit.NANOSECONDS.toSeconds(tv2 - tv1);
        System.out.printf(" done (%d seconds).%n", sec);
    }


    /* Fill up the QF (at least partially). */
    void random_fill(QuotientFilter qf)
    {
        Set<Long> keys = new HashSet<Long>();
        long elts = random.get().nextLong() % qf.MAX_INSERTIONS;
        while (elts > 0) {
            ht_put(qf, keys);
            --elts;
        }
        assertQFInvariants(qf);
    }

    /* Check if @lhs is a subset of @rhs. */
    void subsetof(QuotientFilter lhs, QuotientFilter rhs)
    {
        QuotientFilter.QFIterator QFIterator = lhs.new QFIterator();
        while (QFIterator.hasNext())
        {
            assertTrue(rhs.maybeContain(QFIterator.next()));
        }
    }

    /* Check if @qf contains both @qf1 and @qf2. */
    void supersetof(QuotientFilter qf, QuotientFilter qf1,
                           QuotientFilter qf2)
    {
        QuotientFilter.QFIterator QFIterator = qf.new QFIterator();
        while (QFIterator.hasNext())
        {
            long hash = QFIterator.next();
            assertTrue(qf1.maybeContain(hash) | qf2.maybeContain(hash));
        }
    }

    private Callable<Object> getQuotientFilterTestFor(int quotient, int remainder)
    {
        long seed = ThreadLocalRandom.current().nextLong();
        return () -> {
            random.get().setSeed(seed);
            System.out.println("Thread " + Thread.currentThread().getName() + " testing q=" + quotient + " r=" + remainder + " seed " + seed);
            QuotientFilter qf = new QuotientFilter(quotient, remainder);
            qf_test(qf);
            return null;
        };
    }

    @Test
    public void testQuotientFilter() throws Exception
    {
        List<Future<Object>> results = new ArrayList<Future<Object>>();
        for (int q = 1; q <= Q_MAX; ++q)
        {
            for (int r = 1; r <= R_MAX; ++r)
            {
                Callable<Object> testTask = getQuotientFilterTestFor(q, r);
                results.add(es.submit(testTask));
            }
        }

        for (Future<Object> f : results)
        {
            f.get();
        }
    }

    private Callable<Object> getQuotientFilterMergeTestFor(int quotient1, int remainder1, int quotient2, int remainder2)
    {
        long seed = ThreadLocalRandom.current().nextLong();
        //long seed = -8123614265494799222L;
        return () -> {
            random.get().setSeed(seed);
            System.out.println("Thread " + Thread.currentThread().getName() + " testing q1=" + quotient1 + " r1=" + remainder1 +  " q2=" + quotient2 + " r2=" + remainder2 +" seed " + seed);
            QuotientFilter qf;
            QuotientFilter qf1 = new QuotientFilter(quotient1, remainder1);
            QuotientFilter qf2 = new QuotientFilter(quotient2, remainder2);

            random_fill(qf1);
            random_fill(qf2);

            //Check if overflow will occur on merge
            long entries = qf1.entries + qf2.entries;
            int maxQuotientBits = quotient1 + remainder1 - 1;
            boolean expectNotEnoughFingerprintBits = entries > (long)(LongMath.pow(2, maxQuotientBits) * 0.75);

            try
            {
                qf = qf1.merge(qf2);
            }
            catch (IllegalArgumentException e)
            {
                if (quotient1 + remainder1 != quotient2 + remainder2)
                {
                    return null;
                }
                if (expectNotEnoughFingerprintBits & e.getMessage().equals("Impossible to merge not enough fingerprint bits"))
                {
                    return null;
                }
                throw e;
            }
            assertQFInvariants(qf);
            subsetof(qf1, qf);
            subsetof(qf2, qf);
            supersetof(qf, qf1, qf2);
            return null;
        };
    }

    @Test
    public void testQuotientFilterMerge() throws Exception
    {
        List<Future<Object>> results = new ArrayList<Future<Object>>();
        getQuotientFilterMergeTestFor(1, 1, 1, 1).call();
        for (int q1 = 1; q1 <= Q_MAX; ++q1) {
            for (int r1 = 1; r1 <= R_MAX; ++r1) {
                for (int q2 = 1; q2 <= Q_MAX; ++q2) {
                    for (int r2 = 1; r2 <= R_MAX; ++r2) {
                        Callable<Object> testTask = getQuotientFilterMergeTestFor(q1, r1, q2, r2);
                        results.add(es.submit(testTask));
                        //testTask.call();
                    }
                }
            }
        }

        for (Future<Object> f : results)
        {
            f.get();
        }
    }
}