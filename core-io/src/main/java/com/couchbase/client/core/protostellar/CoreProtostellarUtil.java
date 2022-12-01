package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.grpc.Deadline;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.ProtostellarErrorContext;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.protostellar.kv.v1.DocumentContentType;
import com.couchbase.client.protostellar.kv.v1.DurabilityLevel;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

// todo sn split this grab-bag util class up
public class CoreProtostellarUtil {
  private CoreProtostellarUtil() {}

  public static Duration kvTimeout(Optional<Duration> customTimeout, Core core) {
    return customTimeout.orElse(core.context().environment().timeoutConfig().kvTimeout());
  }

  public static Duration kvDurableTimeout(Optional<Duration> customTimeout,
                                          Optional<com.couchbase.client.core.msg.kv.DurabilityLevel> dl,
                                          Core core) {
    if (customTimeout.isPresent()) {
      return customTimeout.get();
    } else if (dl.isPresent()) {
      return core.context().environment().timeoutConfig().kvDurableTimeout();
    } else {
      return core.context().environment().timeoutConfig().kvTimeout();
    }
  }
  public static Deadline convertTimeout(Optional<Duration> customTimeout, Duration defaultTimeout) {
    if (customTimeout.isPresent()) {
      return Deadline.after(customTimeout.get().toMillis(), TimeUnit.MILLISECONDS);
    } else {
      return Deadline.after(defaultTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  public static Deadline convertKvDurableTimeout(Optional<Duration> customTimeout, Optional<com.couchbase.client.core.msg.kv.DurabilityLevel> dl, Core core) {
    if (customTimeout.isPresent()) {
      return Deadline.after(customTimeout.get().toMillis(), TimeUnit.MILLISECONDS);
    } else if (dl.isPresent()) {
      return Deadline.after(core.context().environment().timeoutConfig().kvDurableTimeout().toMillis(), TimeUnit.MILLISECONDS);
    } else {
      return Deadline.after(core.context().environment().timeoutConfig().kvTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  public static Deadline convertKvTimeout(Optional<Duration> customTimeout, Core core) {
    if (customTimeout.isPresent()) {
      return Deadline.after(customTimeout.get().toMillis(), TimeUnit.MILLISECONDS);
    } else {
      return Deadline.after(core.context().environment().timeoutConfig().kvTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  public static Deadline convertTimeout(Duration timeout) {
    return Deadline.after(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  public static int convertToFlags(DocumentContentType contentType) {
    int flags = 0;
    switch (contentType) {
      case JSON:
        flags = CodecFlags.JSON_COMPAT_FLAGS;
        break;
      case BINARY:
        flags = CodecFlags.BINARY_COMPAT_FLAGS;
        break;
    }
    return flags;
  }

  public static DocumentContentType convertFromFlags(int flags) {
    if ((flags & CodecFlags.JSON_COMPAT_FLAGS) != 0) {
      return DocumentContentType.JSON;
    }
    if ((flags & CodecFlags.BINARY_COMPAT_FLAGS) != 0) {
      return DocumentContentType.BINARY;
    }
    return DocumentContentType.UNKNOWN;
  }

  public static void handleShutdownBlocking(Core core, AbstractContext context) {
    if (core.protostellar().endpoint().isShutdown()) {
      throw RequestCanceledException.shuttingDown(new ProtostellarErrorContext(context));
    }
  }

  public static <T> boolean handleShutdownAsync(Core core, CompletableFuture<T> ret, AbstractContext context) {
    if (core.protostellar().endpoint().isShutdown()) {
      ret.completeExceptionally(RequestCanceledException.shuttingDown(new ProtostellarErrorContext(context)));
      return true;
    }
    return false;
  }

  public static <TSdkResult> @Nullable boolean handleShutdownReactive(Sinks.One<TSdkResult> ret, Core core, AbstractContext context) {
    if (core.protostellar().endpoint().isShutdown()) {
      ret.tryEmitError(RequestCanceledException.shuttingDown(new ProtostellarErrorContext(context))).orThrow();
      return true;
    }
    return false;
  }

  public static DurabilityLevel convert(com.couchbase.client.core.msg.kv.DurabilityLevel dl) {
    switch (dl) {
      case MAJORITY:
        return DurabilityLevel.MAJORITY;
      case MAJORITY_AND_PERSIST_TO_ACTIVE:
        return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
      case PERSIST_TO_MAJORITY:
        return DurabilityLevel.PERSIST_TO_MAJORITY;
    }

    // NONE should be handled earlier, by not sending anything.
    throw new IllegalArgumentException("Unknown durability level " + dl);
  }

  public static <TResponse> ProtostellarFailureBehaviour convertKeyValueException(Throwable t,
                                                                                  ProtostellarRequest<TResponse> request) {
    return CoreProtostellarErrorHandlingUtil.convertKeyValueException(t, request);
  }

  public static RequestSpan createSpan(Core core,
                                       String spanName,
                                       Optional<com.couchbase.client.core.msg.kv.DurabilityLevel> durability,
                                       @Nullable RequestSpan parent) {
    RequestSpan span = CbTracing.newSpan(core.context().environment().requestTracer(), spanName, parent);

    if (durability.isPresent()) {
      switch (durability.get()) {
        case MAJORITY:
          span.attribute(TracingIdentifiers.ATTR_DURABILITY, "majority");
          break;
        case MAJORITY_AND_PERSIST_TO_ACTIVE:
          span.attribute(TracingIdentifiers.ATTR_DURABILITY, "majority_and_persist_active");
          break;
        case PERSIST_TO_MAJORITY:
          span.attribute(TracingIdentifiers.ATTR_DURABILITY, "persist_majority");
          break;
      }
    }

    return span;
  }
}
