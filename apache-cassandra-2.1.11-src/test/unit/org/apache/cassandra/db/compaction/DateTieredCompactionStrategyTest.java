/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.compaction.DateTieredCompactionStrategy.getBuckets;
import static org.apache.cassandra.db.compaction.DateTieredCompactionStrategy.newestBucket;
import static org.apache.cassandra.db.compaction.DateTieredCompactionStrategy.trimToThreshold;
import static org.apache.cassandra.db.compaction.DateTieredCompactionStrategy.filterOldSSTables;
import static org.apache.cassandra.db.compaction.DateTieredCompactionStrategy.validateOptions;

import static org.junit.Assert.*;

public class DateTieredCompactionStrategyTest extends SchemaLoader
{
    public static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_STANDARD1 = "Standard1";

    @Test
    public void testOptionsValidation() throws ConfigurationException
    {
        Map<String, String> options = new HashMap<>();
        options.put(DateTieredCompactionStrategyOptions.BASE_TIME_KEY, "30");
        options.put(DateTieredCompactionStrategyOptions.MAX_SSTABLE_AGE_KEY, "1825");
        Map<String, String> unvalidated = validateOptions(options);
        assertTrue(unvalidated.isEmpty());

        try
        {
            options.put(DateTieredCompactionStrategyOptions.BASE_TIME_KEY, "0");
            validateOptions(options);
            fail(String.format("%s == 0 should be rejected", DateTieredCompactionStrategyOptions.BASE_TIME_KEY));
        }
        catch (ConfigurationException e) {}

        try
        {
            options.put(DateTieredCompactionStrategyOptions.BASE_TIME_KEY, "-1337");
            validateOptions(options);
            fail(String.format("Negative %s should be rejected", DateTieredCompactionStrategyOptions.BASE_TIME_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(DateTieredCompactionStrategyOptions.BASE_TIME_KEY, "1");
        }

        try
        {
            options.put(DateTieredCompactionStrategyOptions.MAX_SSTABLE_AGE_KEY, "-1337");
            validateOptions(options);
            fail(String.format("Negative %s should be rejected", DateTieredCompactionStrategyOptions.MAX_SSTABLE_AGE_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(DateTieredCompactionStrategyOptions.MAX_SSTABLE_AGE_KEY, "0");
        }

        options.put("bad_option", "1.0");
        unvalidated = validateOptions(options);
        assertTrue(unvalidated.containsKey("bad_option"));
    }

    @Test
    public void testTimeConversions()
    {
        Map<String, String> options = new HashMap<>();
        options.put(DateTieredCompactionStrategyOptions.BASE_TIME_KEY, "30");
        options.put(DateTieredCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, "SECONDS");

        DateTieredCompactionStrategyOptions opts = new DateTieredCompactionStrategyOptions(options);
        assertEquals(opts.maxSSTableAge, TimeUnit.SECONDS.convert(365, TimeUnit.DAYS));

        options.put(DateTieredCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, "MILLISECONDS");
        opts = new DateTieredCompactionStrategyOptions(options);
        assertEquals(opts.maxSSTableAge, TimeUnit.MILLISECONDS.convert(365, TimeUnit.DAYS));

        options.put(DateTieredCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, "MICROSECONDS");
        options.put(DateTieredCompactionStrategyOptions.MAX_SSTABLE_AGE_KEY, "10");
        opts = new DateTieredCompactionStrategyOptions(options);
        assertEquals(opts.maxSSTableAge, TimeUnit.MICROSECONDS.convert(10, TimeUnit.DAYS));

        options.put(DateTieredCompactionStrategyOptions.MAX_SSTABLE_AGE_KEY, "0.5");
        opts = new DateTieredCompactionStrategyOptions(options);
        assertEquals(opts.maxSSTableAge, TimeUnit.MICROSECONDS.convert(1, TimeUnit.DAYS) / 2);

        options.put(DateTieredCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, "HOURS");
        options.put(DateTieredCompactionStrategyOptions.MAX_SSTABLE_AGE_KEY, "0.5");
        opts = new DateTieredCompactionStrategyOptions(options);
        assertEquals(opts.maxSSTableAge, 12);

    }

    @Test
    public void testGetBuckets()
    {
        List<Pair<String, Long>> pairs = Lists.newArrayList(
                Pair.create("a", 199L),
                Pair.create("b", 299L),
                Pair.create("a", 1L),
                Pair.create("b", 201L)
        );
        List<List<String>> buckets = getBuckets(pairs, 100L, 2, 200L);
        assertEquals(2, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(2, bucket.size());
            assertEquals(bucket.get(0), bucket.get(1));
        }


        pairs = Lists.newArrayList(
                Pair.create("a", 2000L),
                Pair.create("b", 3600L),
                Pair.create("a", 200L),
                Pair.create("c", 3950L),
                Pair.create("too new", 4125L),
                Pair.create("b", 3899L),
                Pair.create("c", 3900L)
        );
        buckets = getBuckets(pairs, 100L, 3, 4050L);
        // targets (divPosition, size): (40, 100), (39, 100), (12, 300), (3, 900), (0, 2700)
        // in other words: 0 - 2699, 2700 - 3599, 3600 - 3899, 3900 - 3999, 4000 - 4099
        assertEquals(3, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(2, bucket.size());
            assertEquals(bucket.get(0), bucket.get(1));
        }


        // Test base 1.
        pairs = Lists.newArrayList(
                Pair.create("a", 200L),
                Pair.create("a", 299L),
                Pair.create("b", 2000L),
                Pair.create("b", 2014L),
                Pair.create("c", 3610L),
                Pair.create("c", 3690L),
                Pair.create("d", 3898L),
                Pair.create("d", 3899L),
                Pair.create("e", 3900L),
                Pair.create("e", 3950L),
                Pair.create("too new", 4125L)
        );
        buckets = getBuckets(pairs, 100L, 1, 4050L);

        assertEquals(5, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(2, bucket.size());
            assertEquals(bucket.get(0), bucket.get(1));
        }
    }

    @Test
    public void testPrepBucket()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // create 3 sstables
        int numSSTables = 3;
        for (int r = 0; r < numSSTables; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            rm.add(CF_STANDARD1, Util.cellname("column"), value, r);
            rm.apply();
            cfs.forceBlockingFlush();
        }
        cfs.forceBlockingFlush();

        List<SSTableReader> sstrs = new ArrayList<>(cfs.getSSTables());

        List<SSTableReader> newBucket = newestBucket(Collections.singletonList(sstrs.subList(0, 2)), 4, 32, 9, 10);
        assertTrue("incoming bucket should not be accepted when it has below the min threshold SSTables", newBucket.isEmpty());

        newBucket = newestBucket(Collections.singletonList(sstrs.subList(0, 2)), 4, 32, 10, 10);
        assertFalse("non-incoming bucket should be accepted when it has at least 2 SSTables", newBucket.isEmpty());

        assertEquals("an sstable with a single value should have equal min/max timestamps", sstrs.get(0).getMinTimestamp(), sstrs.get(0).getMaxTimestamp());
        assertEquals("an sstable with a single value should have equal min/max timestamps", sstrs.get(1).getMinTimestamp(), sstrs.get(1).getMaxTimestamp());
        assertEquals("an sstable with a single value should have equal min/max timestamps", sstrs.get(2).getMinTimestamp(), sstrs.get(2).getMaxTimestamp());

        // if we have more than the max threshold, the oldest should be dropped
        Collections.sort(sstrs, Collections.reverseOrder(new Comparator<SSTableReader>() {
            public int compare(SSTableReader o1, SSTableReader o2) {
                return Long.compare(o1.getMinTimestamp(), o2.getMinTimestamp()) ;
            }
        }));

        List<SSTableReader> bucket = trimToThreshold(sstrs, 2);
        assertEquals("one bucket should have been dropped", 2, bucket.size());
        for (SSTableReader sstr : bucket)
            assertFalse("the oldest sstable should be dropped", sstr.getMinTimestamp() == 0);
    }

    @Test
    public void testFilterOldSSTables()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // create 3 sstables
        int numSSTables = 3;
        for (int r = 0; r < numSSTables; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            rm.add(CF_STANDARD1, Util.cellname("column"), value, r);
            rm.apply();
            cfs.forceBlockingFlush();
        }
        cfs.forceBlockingFlush();

        Iterable<SSTableReader> filtered;
        List<SSTableReader> sstrs = new ArrayList<>(cfs.getSSTables());

        filtered = filterOldSSTables(sstrs, 0, 2);
        assertEquals("when maxSSTableAge is zero, no sstables should be filtered", sstrs.size(), Iterables.size(filtered));

        filtered = filterOldSSTables(sstrs, 1, 2);
        assertEquals("only the newest 2 sstables should remain", 2, Iterables.size(filtered));

        filtered = filterOldSSTables(sstrs, 1, 3);
        assertEquals("only the newest sstable should remain", 1, Iterables.size(filtered));

        filtered = filterOldSSTables(sstrs, 1, 4);
        assertEquals("no sstables should remain when all are too old", 0, Iterables.size(filtered));
    }


    @Test
    public void testDropExpiredSSTables() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // create 2 sstables
        DecoratedKey key = Util.dk(String.valueOf("expired"));
        Mutation rm = new Mutation(KEYSPACE1, key.getKey());
        rm.add(CF_STANDARD1, Util.cellname("column"), value, System.currentTimeMillis(), 1);
        rm.apply();
        cfs.forceBlockingFlush();
        SSTableReader expiredSSTable = cfs.getSSTables().iterator().next();
        Thread.sleep(10);
        key = Util.dk(String.valueOf("nonexpired"));
        rm = new Mutation(KEYSPACE1, key.getKey());
        rm.add(CF_STANDARD1, Util.cellname("column"), value, System.currentTimeMillis());
        rm.apply();
        cfs.forceBlockingFlush();
        assertEquals(cfs.getSSTables().size(), 2);

        Map<String, String> options = new HashMap<>();

        options.put(DateTieredCompactionStrategyOptions.BASE_TIME_KEY, "30");
        options.put(DateTieredCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, "MILLISECONDS");
        options.put(DateTieredCompactionStrategyOptions.MAX_SSTABLE_AGE_KEY, Double.toString((1d / (24 * 60 * 60))));
        options.put(DateTieredCompactionStrategyOptions.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, "0");
        DateTieredCompactionStrategy dtcs = new DateTieredCompactionStrategy(cfs, options);
        for (SSTableReader sstable : cfs.getSSTables())
            dtcs.addSSTable(sstable);
        dtcs.startup();
        assertNull(dtcs.getNextBackgroundTask((int) (System.currentTimeMillis() / 1000)));
        Thread.sleep(2000);
        AbstractCompactionTask t = dtcs.getNextBackgroundTask((int) (System.currentTimeMillis()/1000));
        assertNotNull(t);
        assertEquals(1, Iterables.size(t.sstables));
        SSTableReader sstable = t.sstables.iterator().next();
        assertEquals(sstable, expiredSSTable);
        cfs.getDataTracker().unmarkCompacting(cfs.getSSTables());
    }

}
