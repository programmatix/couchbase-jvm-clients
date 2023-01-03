package com.couchbase.client.core.env;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;

public class CouchbaseForkPoolThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
  private final Logger logger = LoggerFactory.getLogger(CouchbaseForkPoolThreadFactory.class);
  static class CouchbaseThread extends ForkJoinWorkerThread {
    public CouchbaseThread(ForkJoinPool pool) {
      super(pool);
    }
  }

  private final String namePrefix;
  private final AtomicInteger threadNumber = new AtomicInteger();

  public CouchbaseForkPoolThreadFactory(String namePrefix) {
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
