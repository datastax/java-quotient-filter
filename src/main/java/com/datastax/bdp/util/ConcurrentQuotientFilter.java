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
package com.datastax.bdp.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import net.jpountz.xxhash.XXHashFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A thread safe auto-resizing concurrent quotient filter that generates errors on overflow.
 */
public class ConcurrentQuotientFilter
{
    static final XXHashFactory hashFactory = XXHashFactory.fastestInstance();
    private final List<Stripe> stripes;

    public ConcurrentQuotientFilter(long largestNumberOfElements, long startingElements, int numStripes)
    {
        Preconditions.checkArgument(startingElements > 0);
        Preconditions.checkArgument(largestNumberOfElements > startingElements);
        Preconditions.checkArgument(numStripes > 0);
        Preconditions.checkArgument(largestNumberOfElements / numStripes > 0);
        Preconditions.checkArgument(startingElements / numStripes > 0);
        Preconditions.checkArgument(numStripes <= 1024);

        //Make them all powers of two, the rounding will happen in quotient filter anyways.
        largestNumberOfElements = roundUpPowerOf2(largestNumberOfElements);
        startingElements = roundUpPowerOf2(startingElements);
        numStripes = Ints.checkedCast(roundUpPowerOf2(startingElements));
        System.out.printf("Largest elements %d, starting elements %d, numStripes %d%n", largestNumberOfElements, startingElements, numStripes);


        QuotientFilter qf = QuotientFilter.create(largestNumberOfElements, 1);
        if ((64 - (qf.REMAINDER_BITS + qf.QUOTIENT_BITS)) < 10)
        {
            throw new IllegalArgumentException("Need at least 10 bits of hash to use to pick a stripe.");
        }

        stripes = new ArrayList<Stripe>(numStripes);
        for (int ii = 0; ii < numStripes; ii++)
        {
            //All stripes must have the same largest number of elements in order to end up with the same
            //size fingerprint.
            stripes.add(new Stripe(largestNumberOfElements, Math.max(1, startingElements / numStripes), ii));
        }
    }

    static long roundUpPowerOf2(long value) {
        return Long.bitCount(value) == 1 ?
                value :
                Long.highestOneBit(value) << 1L;
    }

    public long insert(byte[] data)
    {
        return insert(data, 0, data.length);
    }

    public long insert(byte[] data, int offset, int length)
    {
        long hash = hashFactory.hash64().hash(data, offset, length, 0);
        insert(hash);
        return hash;
    }

    public void insert(long hash)
    {
        Stripe stripe = stripe(hash);
        stripe.lock.writeLock().lock();
        try
        {
            stripe.qf.insert(hash);
        }
        finally
        {
            stripe.lock.writeLock().unlock();
        }
    }

    public void remove(long hash)
    {
        Stripe stripe = stripe(hash);
        stripe.lock.writeLock().lock();
        try
        {
            stripe.qf.remove(hash);
        }
        finally
        {
            stripe.lock.writeLock().unlock();
        }
    }

    public boolean maybeContains(long hash)
    {
        Stripe stripe = stripe(hash);
        stripe.lock.readLock().lock();
        try
        {
            return stripe.qf.maybeContains(hash);
        }
        finally
        {
            stripe.lock.readLock().unlock();
        }
    }

    public long allocatedSize() {
        long sum = 0;
        for (Stripe s : stripes) {
            s.lock.readLock().lock();
            try {
                sum += s.qf.MAX_SIZE;
            } finally {
                s.lock.readLock().unlock();
            }
        }
        return sum;
    }

    private Stripe stripe(long hash)
    {

        return stripes.get((((int)hash >>> (64 - 10)) % stripes.size()));
    }

    public void insertAll(Iterator<Long> hashes)
    {
        try (AutoCloseable lock = lockAll(true))
        {
            while (hashes.hasNext())
            {
                long hash = hashes.next();
                stripe(hash).qf.insert(hash);
            }
        }
        catch (Exception e)
        {
            Throwables.propagate(e);
        }
    }

    /**
     * Thread safe. Holds a read lock while iterating. Inverts control to prevent taking an iterator
     * and then not closing it.
     */
    public void foreach(Consumer<Long> consumer)
    {
        try (CQFIterator iterator = new CQFIterator())
        {
            while (iterator.hasNext())
            {
                consumer.accept(iterator.next());
            }
        }
        catch (Exception e)
        {
            Throwables.propagate(e);
        }
    }

    public AutoCloseable lockAll(boolean write)
    {
        if (write)
        {
            stripes.stream().forEach(stripe -> stripe.lock.writeLock().lock());
            return () -> stripes.stream().forEach(stripe -> stripe.lock.writeLock().unlock());
        }
        else
        {
            stripes.stream().forEach(stripe -> stripe.lock.readLock().lock());
            return () -> stripes.stream().forEach(stripe -> stripe.lock.readLock().unlock());
        }
    }

    private class CQFIterator extends UnmodifiableIterator<Long> implements AutoCloseable
    {
        Iterator<Long> delegate;
        AutoCloseable unlock;

        private CQFIterator()
        {
            unlock = lockAll(false);
            Iterable<Iterator<Long>> filters = (Iterable)stripes.stream().map(stripe -> new AbstractIterator<Long>() {
                private final Iterator<Long> delegate = stripe.qf.iterator();
                @Override
                public Long computeNext() {
                    if (delegate.hasNext()) {
                        return delegate.next() | (stripe.index << (64 - 10));
                    }
                    endOfData();
                    return null;
                }}).collect(Collectors.toList());
            delegate = Iterators.mergeSorted(filters, Ordering.natural());
        }


        @Override
        public void close() throws Exception {
            unlock.close();
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Long next() {
            return delegate.next();
        }
    }

    int fingerprintBits() {
        QuotientFilter qf = stripes.get(0).qf;
        return qf.QUOTIENT_BITS + qf.REMAINDER_BITS;
    }

    private class Stripe
    {
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        final int index;
        QuotientFilter qf;

        public Stripe(long largestNumberOfElements, long startingElements, int index)
        {
            qf = QuotientFilter.create(largestNumberOfElements, startingElements);
            this.index = index;
        }
    }
}
