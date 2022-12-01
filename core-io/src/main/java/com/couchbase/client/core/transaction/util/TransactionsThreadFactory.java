package com.couchbase.client.core.transaction.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionsThreadFactory implements ThreadFactory {
  private final String namePrefix;
  private final AtomicInteger threadNumber = new AtomicInteger();

  public TransactionsThreadFactory(String namePrefix) {
    this.namePrefix = namePrefix;
  }

  public Thread newThread(Runnable r) {
    Thread t = new Thread(namePrefix + threadNumber.getAndIncrement());
    System.out.println("Creating thread " + t.getName());
    t.setDaemon(true);
    return t;
  }
}
