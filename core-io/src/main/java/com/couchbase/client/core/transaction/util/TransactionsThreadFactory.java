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
