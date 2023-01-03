package com.couchbase.client.core.env;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CouchbaseThreadFactory implements ThreadFactory {
  private final Logger logger = LoggerFactory.getLogger(CouchbaseThreadFactory.class);

  private final String namePrefix;
  private final AtomicInteger threadNumber = new AtomicInteger();

  public CouchbaseThreadFactory(String namePrefix) {
    this.namePrefix = namePrefix;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(r);
    t.setName(namePrefix + threadNumber.getAndIncrement());
    t.setDaemon(true);
    logger.info("Created thread {}", t.getName());
    return t;
  }
}
