package com.couchbase.client.core.api.admin;

import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.config.CollectionsManifest;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import reactor.core.publisher.Mono;

import java.time.Duration;

public interface CoreAdminOps {

  void createCollectionBlocking(String scopeName, String collectionName, Duration maxTTL,
                                CoreCommonOptions options);

  CoreAsyncResponse<Void> createCollectionAsync(String scopeName, String collectionName, Duration maxTTL,
                                                  CoreCommonOptions options);

  Mono<Void> createCollectionReactive(String scopeName, String collectionName, Duration maxTTL,
                                                          CoreCommonOptions options);

  CoreAsyncResponse<Void> createScopeAsync(String scopeName, CoreCommonOptions options);

  CoreAsyncResponse<Void> dropCollectionAsync(String scopeName, String collectionName, CoreCommonOptions options);

  CoreAsyncResponse<Void> dropScopeAsync(String scopeName, CoreCommonOptions options);

  CoreAsyncResponse<CollectionsManifest> getAllScopesAsync(CoreCommonOptions options);
}
