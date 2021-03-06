/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.server.scheduler;

import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import voldemort.store.StorageEngine;
import voldemort.utils.ClosableIterator;
import voldemort.utils.IoThrottler;
import voldemort.utils.Pair;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * Expire old data
 * 
 * @author jay
 * 
 */
public class DataCleanupJob<K, V> implements Runnable {

    private static final Logger logger = Logger.getLogger(DataCleanupJob.class);

    private final StorageEngine<K, V> store;
    private final Semaphore cleanupPermits;
    private final long maxAgeMs;
    private final Time time;
    private final IoThrottler throttler;

    public DataCleanupJob(StorageEngine<K, V> store,
                          Semaphore cleanupPermits,
                          long maxAgeMs,
                          Time time,
                          IoThrottler throttler) {
        this.store = Utils.notNull(store);
        this.cleanupPermits = Utils.notNull(cleanupPermits);
        this.maxAgeMs = maxAgeMs;
        this.time = time;
        this.throttler = throttler;
    }

    public void run() {
        acquireCleanupPermit();
        ClosableIterator<Pair<K, Versioned<V>>> iterator = null;
        try {
            logger.info("Starting data cleanup on store \"" + store.getName() + "\"...");
            int deleted = 0;
            long now = time.getMilliseconds();
            iterator = store.entries();
            try {
                while(iterator.hasNext()) {
                    // check if we have been interrupted
                    if(Thread.currentThread().isInterrupted()) {
                        logger.info("Datacleanup job halted.");
                        return;
                    }
                    Pair<K, Versioned<V>> keyAndVal = iterator.next();
                    VectorClock clock = (VectorClock) keyAndVal.getSecond().getVersion();
                    if(now - clock.getTimestamp() > maxAgeMs) {
                        store.delete(keyAndVal.getFirst(), clock);
                        deleted++;
                        if(deleted % 10000 == 0)
                            logger.debug("Deleted item " + deleted);
                    }
                    throttler.maybeThrottle(clock.sizeInBytes());
                }
            } catch(RuntimeException e) {
                iterator.close();
                logger.error("Error during data cleanup", e);
                throw e;
            } finally {
                if(iterator != null)
                    iterator.close();
            }
            logger.info("Data cleanup on store \"" + store.getName() + "\" is complete; " + deleted
                        + " items deleted.");
        } catch(Exception e) {
            logger.error("Error in data cleanup job for store " + store.getName() + ": ", e);
        } finally {
            if(iterator != null)
                iterator.close();
            this.cleanupPermits.release();
        }
    }

    private void acquireCleanupPermit() {
        logger.debug("Acquiring lock to perform data cleanup on \"" + store.getName() + "\".");
        try {
            this.cleanupPermits.acquire();
        } catch(InterruptedException e) {
            throw new IllegalStateException("Datacleanup interrupted while waiting for cleanup permit.",
                                            e);
        }
    }

}
