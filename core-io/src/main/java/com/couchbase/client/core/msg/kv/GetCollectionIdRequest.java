/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.extras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.request;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Fetches the collection ID from the cluster based on a {@link CollectionIdentifier}.
 */
public class GetCollectionIdRequest extends BaseKeyValueRequest<GetCollectionIdResponse> {

  public GetCollectionIdRequest(final Duration timeout, final CoreContext ctx,
                                final RetryStrategy retryStrategy,
                                final CollectionIdentifier collectionIdentifier) {
    super(timeout, ctx, retryStrategy, null, collectionIdentifier);
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf body = null;
    try {
      CollectionIdentifier ci = collectionIdentifier();
      if (!ci.collection().isPresent()) {
        throw InvalidArgumentException.fromMessage("A collection name needs to be present");
      }

      // Note that the scope can be empty, according to spec it is the same as _default scope
      body = Unpooled.copiedBuffer(ci.scope().orElse("") + "." + ci.collection().get(), UTF_8);
      return request(alloc, MemcacheProtocol.Opcode.COLLECTIONS_GET_CID, noDatatype(),
        noPartition(), opaque, noCas(), noExtras(), noKey(), body);
    } finally {
      ReferenceCountUtil.release(body);
    }
  }

  @Override
  public GetCollectionIdResponse decode(ByteBuf response, KeyValueChannelContext ctx) {
    ResponseStatus status = MemcacheProtocol.decodeStatus(response);
    Optional<Long> cid = Optional.empty();
    if (status.success()) {
      cid = Optional.of(extras(response).get().getUnsignedInt(8));
    }
    return new GetCollectionIdResponse(status, cid);
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public String name() {
    return "get_collection_id";
  }

}
