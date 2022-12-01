package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.Map;

@Stability.Internal
public class ProtostellarKeyValueRequestContext extends ProtostellarRequestContext {
  private final String id;
  private final CollectionIdentifier collectionIdentifier;


  public ProtostellarKeyValueRequestContext(Core core,
                                            ServiceType serviceType,
                                            String requestName,
                                            Duration timeout,
                                            String id,
                                            CollectionIdentifier collectionIdentifier) {
    super(core, serviceType, requestName, timeout);
    this.id = id;
    this.collectionIdentifier = collectionIdentifier;
  }

  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    input.put("id", id);
    input.put("bucket", collectionIdentifier.bucket());
    input.put("scope", collectionIdentifier.scope().orElse(CollectionIdentifier.DEFAULT_SCOPE));
    input.put("collection", collectionIdentifier.collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION));
  }
}
