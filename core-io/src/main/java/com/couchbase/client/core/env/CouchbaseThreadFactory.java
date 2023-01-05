/*
 * Copyright (c) 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.env;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;

public class CouchbaseThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
  private final Logger logger = LoggerFactory.getLogger(CouchbaseThreadFactory.class);
  static class CouchbaseThread extends ForkJoinWorkerThread {
    public CouchbaseThread(ForkJoinPool pool) {
      super(pool);
    }
  }

  private final String namePrefix;
  private final AtomicInteger threadNumber = new AtomicInteger();

  public CouchbaseThreadFactory(String namePrefix) {
    this.namePrefix = namePrefix;
  }

  @Override
  public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
    CouchbaseThread t = new CouchbaseThread(pool);
    t.setName(namePrefix + threadNumber.getAndIncrement());
    t.setDaemon(true);
    logger.info("Created thread {}, currently {} threads in pool, {} running", t.getName(), pool.getActiveThreadCount(), pool.getRunningThreadCount());
    return t;
  }
}
