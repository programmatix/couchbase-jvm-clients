/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty.kv;

import io.netty.channel.Channel;
import com.couchbase.client.core.util.NanoTimestamp;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This class gets populated with timings and success/failure of different steps in the
 * channel bootstrap process and later allows to extract useful information for debugging.
 *
 * @since 2.0.0
 */
public class ConnectTimings {

  private final List<Timing> timings = Collections.synchronizedList(new ArrayList<>());

  private ConnectTimings() {
  }

  /**
   * Start the connect timings for a given class and channel.
   *
   * @param channel the channel to start from.
   * @param clazz the clazz to use as an identifier key.
   */
  public static void start(final Channel channel, final Class<?> clazz) {
    ConnectTimings timings = channel.attr(ChannelAttributes.CONNECT_TIMINGS_KEY).get();
    if (timings == null) {
      timings = new ConnectTimings();
      channel.attr(ChannelAttributes.CONNECT_TIMINGS_KEY).set(timings);
    }
    timings.timings.add(new Timing(clazz));
  }

  /**
   * Stops the timing.
   *
   * @param channel the channel to start from.
   * @param clazz the clazz to use as an identifier key.
   * @param timeout if stopped because of a timeout or not.
   * @return the duration.
   */
  public static Optional<Duration> stop(final Channel channel, final Class<?> clazz,
                                        boolean timeout) {
    ConnectTimings timings = channel.attr(ChannelAttributes.CONNECT_TIMINGS_KEY).get();
    for (Timing timing : timings.timings) {
      if (timing.clazz().equals(clazz)) {
        return Optional.of(timing.complete(timeout));
      }
    }
    return Optional.empty();
  }

  /**
   * Convenience method to record a single timing right away.
   *
   * @param channel the channel to start from.
   * @param clazz the clazz to use as an identifier key.
   */
  public static void record(final Channel channel, final Class<?> clazz) {
    start(channel, clazz);
    stop(channel, clazz, false);
  }

  /**
   * Exports the timings into a string.
   *
   * @param channel which channel to export.
   * @return the exported string.
   */
  public static String toString(final Channel channel) {
    String channelId = channel.id().asShortText();
    StringBuilder sb = new StringBuilder();

    sb
      .append("[")
      .append(channel.localAddress())
      .append(" -> ")
      .append(channel.remoteAddress());

    sb.append(" (id: ").append(channelId).append(")");
    sb.append("]\n");

    for (ConnectTimings.Timing timing : timings(channel)) {
      sb
        .append(" -> ")
        .append(timing.clazz.getSimpleName())
        .append(": ~")
        .append(timing.isComplete() ? timing.latency().toMillis() : 0)
        .append("ms (complete=")
        .append(timing.isComplete())
        .append(", timeout=")
        .append(timing.timeout)
        .append(")\n");
    }

    return sb.toString();
  }

  public static SortedMap<String, Duration> toMap(final Channel channel) {
    SortedMap<String, Duration> timings = new TreeMap<>();
    if (channel == null) {
      return timings;
    }

    for (ConnectTimings.Timing timing : timings(channel)) {
      timings.put(timing.clazz.getSimpleName(), timing.latency());
    }
    return timings;
  }

  private static List<Timing> timings(final Channel channel) {
    ConnectTimings ct = channel.attr(ChannelAttributes.CONNECT_TIMINGS_KEY).get();
    return ct == null ? Collections.emptyList() : ct.timings;
  }

  /**
   * Holds an individual timing, the sum of which makes up the full
   * timings sequence implemented by the parent.
   */
  static class Timing {

    /**
     * Stores the time when this timing is created.
     */
    private final NanoTimestamp start = NanoTimestamp.now();

    /**
     * Holds the class which is responsible for the timing.
     */
    private final Class<?> clazz;

    /**
     * Once complete, stores the time at end.
     */
    private volatile NanoTimestamp end = NanoTimestamp.never();

    /**
     * Once completed, holds info if this timing timed out
     * or not. Supplied by the caller.
     */
    private volatile boolean timeout;

    Timing(Class<?> clazz) {
      this.clazz = clazz;
    }

    Class<?> clazz() {
      return clazz;
    }

    /**
     * The latency of this operation if complete.
     *
     * @return the {@link Duration} of this timing.
     */
    Duration latency() {
      if (!isComplete()) {
        throw new IllegalStateException("Incomplete Timing.");
      }
      return end.minus(start);
    }

    /**
     * True if {@link #complete(boolean)} was called already.
     *
     * @return true if complete.
     */
    boolean isComplete() {
      return !end.isNever();
    }

    /**
     * Completes this {@link Timing}.
     *
     * @param timeout if the timing timed out or not.
     * @return returns the duration of the completed event.
     */
    Duration complete(boolean timeout) {
      this.end = NanoTimestamp.now();
      this.timeout = timeout;
      return latency();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Timing timing = (Timing) o;

      if (start != timing.start) {
        return false;
      }
      if (end != timing.end) {
        return false;
      }
      if (timeout != timing.timeout) {
        return false;
      }
      return clazz != null ? clazz.equals(timing.clazz) : timing.clazz == null;
    }

    @Override
    public int hashCode() {
      int result = start.hashCode();
      result = 31 * result + (clazz != null ? clazz.hashCode() : 0);
      result = 31 * result + end.hashCode();
      result = 31 * result + (timeout ? 1 : 0);
      return result;
    }
  }
}
