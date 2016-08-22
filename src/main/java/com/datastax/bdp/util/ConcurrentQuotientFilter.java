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
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.UnmodifiableIterator;
import net.jpountz.xxhash.XXHashFactory;

import java.util.ArrayList;
import java.util.Collection;
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

    public ConcurrentQuotientFilter(long largestNumberOfElements, int startingElements, int numStripes)
    {
        Preconditions.checkArgument(startingElements > 0);
        Preconditions.checkArgument(largestNumberOfElements > startingElements);
        Preconditions.checkArgument(numStripes > 0);
        Preconditions.checkArgument(largestNumberOfElements / numStripes > 0);
        Preconditions.checkArgument(startingElements / numStripes > 0);
        Preconditions.checkArgument(numStripes <= 1024);
        //Life is just easier if these are powers of 2, so enforce it. They will be rounded to them anyways.
        Preconditions.checkArgument(Integer.bitCount(numStripes) == 1);
        Preconditions.checkArgument(Long.bitCount(largestNumberOfElements) == 1);
        Preconditions.checkArgument(Long.bitCount(startingElements) == 1);

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
            stripes.add(new Stripe(largestNumberOfElements, Math.max(1, startingElements / numStripes)));
        }
    }

    public void insert(byte[] data)
    {
        insert(data);
    }

    public void insert(byte[] data, int offset, int length)
    {
        insert(hashFactory.hash64().hash(data, offset, length, 0));
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

    public boolean maybeContain(long hash)
    {
        Stripe stripe = stripe(hash);
        stripe.lock.readLock().lock();
        try
        {
            return stripe.qf.maybeContain(hash);
        }
        finally
        {
            stripe.lock.readLock().unlock();
        }
    }

    private Stripe stripe(long hash)
    {

        return stripes.get((((int)hash >>> (64 - 10)) % stripes.size()));
    }

    public void insertAll(Collection<QuotientFilter> filtersToAdd)
    {
        if (filtersToAdd.stream().map(filter -> filter.REMAINDER_BITS + filter.QUOTIENT_BITS).distinct().count() != 1)
        {
            throw new IllegalArgumentException("All filters must have the same size fingerprint");
        }

        int remainderBits = filtersToAdd.iterator().next().REMAINDER_BITS;
        int quotientBits = filtersToAdd.iterator().next().QUOTIENT_BITS;
        int fingerprintBits = remainderBits + quotientBits;

        try (AutoCloseable lock = lockAll(true))
        {
            boolean notEmpty = false;
            for (Stripe s : stripes)
            {
                notEmpty |= s.qf.entries > 0;
            }

            long totalEntries = filtersToAdd.stream().collect(Collectors.summingLong(filter -> filter.entries));


            //TODO presize the output tables to reduce resizing
            Iterable<Iterator<Long>> filterIterators = (Iterable) filtersToAdd.stream().map(stripe -> stripe.iterator()).collect(Collectors.toList());
            Iterator<Long> hashes = Iterators.mergeSorted(filterIterators, Ordering.natural());
            while (hashes.hasNext())
            {
                insert(hashes.next());
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
            Iterable<Iterator<Long>> filters = (Iterable)stripes.stream().map(stripe -> stripe.qf.iterator()).collect(Collectors.toList());
            delegate = Iterators.mergeSorted(filters, Ordering.natural());
            unlock = lockAll(false);
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

    private class Stripe
    {
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        QuotientFilter qf;

        public Stripe(long largestNumberOfElements, int startingElements)
        {
            qf = QuotientFilter.create(largestNumberOfElements, startingElements);
        }

        private void resize(long minimumElements)
        {
            qf = qf.resize(minimumElements);
        }
    }
}
