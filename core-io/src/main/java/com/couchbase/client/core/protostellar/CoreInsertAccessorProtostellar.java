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

package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.grpc.Deadline;
import com.couchbase.client.core.error.context.ProtostellarErrorContext;
import com.couchbase.client.core.retry.ProtostellarRequestBehaviour;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertTimeout;

@Stability.Internal
public class CoreInsertAccessorProtostellar {
  private CoreInsertAccessorProtostellar() {}

  public static <TSdkResult> TSdkResult blocking(Core core,
                                                 Duration timeout,
                                                 ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.InsertRequest> req,
                                                 Function<com.couchbase.client.protostellar.kv.v1.InsertResponse, TSdkResult> convertResponse) {
    com.couchbase.client.protostellar.kv.v1.InsertRequest request = req.request();
    return AccessorKeyValueProtostellar.blocking(core,
      req,
      // todo sn withDeadline creates a new stub and Google performance docs advise reusing stubs as much as possible
      // Measure the impact to decide if it's worth tracking if it's a non-default timeout
      () ->         core.protostellar().endpoint().kvBlockingStub().withDeadline(convertTimeout(timeout)).insert(request),
      convertResponse,
      (err) ->      convertException(core, req, err));
  }

  public static <TSdkResult> CompletableFuture<TSdkResult> async(Core core,
                                                                 Duration timeout,
                                                                 ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.InsertRequest> req,
                                                                 Function<com.couchbase.client.protostellar.kv.v1.InsertResponse, TSdkResult> convertResponse) {
    com.couchbase.client.protostellar.kv.v1.InsertRequest request = req.request();
    return AccessorKeyValueProtostellar.async(core,
      req,
      () ->         core.protostellar().endpoint().kvStub().withDeadline(convertTimeout(timeout)).insert(request),
      convertResponse,
      (err) ->      convertException(core, req, err));
  }

  public static <TSdkResult> Mono<TSdkResult> reactive(Core core,
                                                       Duration timeout,
                                                       ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.InsertRequest> req,
                                                       Function<com.couchbase.client.protostellar.kv.v1.InsertResponse, TSdkResult> convertResponse) {
    com.couchbase.client.protostellar.kv.v1.InsertRequest request = req.request();
    return AccessorKeyValueProtostellar.reactive(core,
      req,
      () ->         core.protostellar().endpoint().kvStub().withDeadline(convertTimeout(timeout)).insert(request),
      convertResponse,
      (err) ->      convertException(core, req, err));
  }

  private static ProtostellarRequestBehaviour convertException(Core core, ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.InsertRequest> req, Throwable t) {
    return CoreProtostellarUtil.convertKeyValueException(core, req, t);
  }
}
