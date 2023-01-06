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

package com.couchbase.client.core.retry;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.request.RequestNotRetriedEvent;
import com.couchbase.client.core.cnc.events.request.RequestRetryScheduledEvent;
import com.couchbase.client.core.msg.UnmonitoredRequest;
import com.couchbase.client.core.protostellar.ProtostellarFailureBehaviour;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.protostellar.ProtostellarBaseRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.retry.RetryOrchestrator.controlledBackoff;

@Stability.Internal
public class RetryOrchestratorProtostellar {
  private final static Logger logger = LoggerFactory.getLogger(RetryOrchestratorProtostellar.class);

  /**
   * Returns non-null iff it should retry.  The retry itself must be performed by the calling code.
   */
  public static @Nullable Duration shouldRetry(Core core, ProtostellarRequest<?> request, ProtostellarFailureBehaviour behaviour) {
    CoreContext ctx = core.context();

    if (behaviour.retry() == null) {
      return null;
    }

    // todo sn handle timeouts here

    RetryReason reason = behaviour.retry();

    if (reason.alwaysRetry()) {
      return retryWithDuration(ctx, request, controlledBackoff(request.context().retryAttempts()), reason);
    }

    try {
      RetryAction retryAction;

      // The 99% case is that the retry strategy is the default BestEffortRetryStrategy.INSTANCE.  We can avoid turning the ProtostellarRequest into a Request in this case (Request is required by
      // the RetryStrategy public API).
      if (request.retryStrategy() == BestEffortRetryStrategy.INSTANCE) {
        // todo sn emulate BestEffortRetryStrategy behaviour, with exponential backoff.  For now just using a fixed retry.
        retryAction = RetryAction.withDuration(Duration.ofMillis(50));
      }
      else {
        ProtostellarBaseRequest wrapper = new ProtostellarBaseRequest(request);

        retryAction = request.retryStrategy().shouldRetry(wrapper, behaviour.retry()).get();
      }

      Optional<Duration> duration = retryAction.duration();
      if (duration.isPresent()) {
        Duration cappedDuration = capDuration(duration.get(), request);
        return retryWithDuration(ctx, request, cappedDuration, reason);
      } else {
        // todo sn do we need to emulate this? "unmonitored request's severity is downgraded to debug to not spam the info-level logs"
        ctx.environment().eventBus().publish(new RequestNotRetriedEvent(Event.Severity.DEBUG, request.getClass(), request.context(), reason, null));
        // todo sn how to cancel the request now?
        // request.cancel(CancellationReason.noMoreRetries(reason), retryAction.exceptionTranslator());
      }
    }
    catch (Throwable throwable) {
      ctx.environment().eventBus().publish(
        new RequestNotRetriedEvent(Event.Severity.INFO, request.getClass(), request.context(), reason, throwable)
      );
    }

    return null;
  }

  private static Duration retryWithDuration(final CoreContext ctx, final ProtostellarRequest<?> request,
                                        final Duration duration, final RetryReason reason) {
    Duration cappedDuration = capDuration(duration, request);
    ctx.environment().eventBus().publish(
      new RequestRetryScheduledEvent(cappedDuration, request.context(), request.getClass(), reason)
    );
    request.context().incrementRetryAttempts(cappedDuration, reason);
    logger.info("Retrying op with duration {} reason {} attempts {}", duration, reason, request.context().retryAttempts());
    return cappedDuration;
  }

  @Stability.Internal
  public static Duration capDuration(final Duration uncappedDuration, final ProtostellarRequest<?> request) {
    long theoreticalTimeout = System.nanoTime() + uncappedDuration.toNanos();
    long absoluteTimeout = request.absoluteTimeout();
    long timeoutDelta = theoreticalTimeout - absoluteTimeout;
    if (timeoutDelta > 0) {
      Duration cappedDuration = uncappedDuration.minus(Duration.ofNanos(timeoutDelta));
      if (cappedDuration.isNegative()) {
        return uncappedDuration; // something went wrong, return the uncapped one as a safety net
      }
      return cappedDuration;

    }
    return uncappedDuration;
  }
}
