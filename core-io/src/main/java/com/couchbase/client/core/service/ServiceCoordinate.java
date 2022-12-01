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

package com.couchbase.client.core.service;

import com.couchbase.client.core.util.CbCollections;

import java.util.Objects;
import java.util.Set;

/**
 * Describes the types of services available in a couchbase cluster.
 *
 * @since 1.0.0
 */
public class ServiceCoordinate {
  private final ServiceType serviceType;
  private final Protocol protocol;

  public static ServiceCoordinate KV = new ServiceCoordinate(ServiceType.KV, Protocol.MEMCACHED);
  public static ServiceCoordinate QUERY = new ServiceCoordinate(ServiceType.QUERY, Protocol.HTTP);
  public static ServiceCoordinate ANALYTICS = new ServiceCoordinate(ServiceType.ANALYTICS, Protocol.HTTP);
  public static ServiceCoordinate EVENTING = new ServiceCoordinate(ServiceType.EVENTING, Protocol.HTTP);
  public static ServiceCoordinate VIEWS = new ServiceCoordinate(ServiceType.VIEWS, Protocol.HTTP);
  public static ServiceCoordinate MANAGER = new ServiceCoordinate(ServiceType.MANAGER, Protocol.HTTP);
  public static ServiceCoordinate SEARCH = new ServiceCoordinate(ServiceType.SEARCH, Protocol.HTTP);
  public static ServiceCoordinate BACKUP = new ServiceCoordinate(ServiceType.BACKUP, Protocol.HTTP);

  public static ServiceCoordinate KV_PROTOSTELLAR = new ServiceCoordinate(ServiceType.KV, Protocol.PROTOSTELLAR);
  public static ServiceCoordinate QUERY_PROTOSTELLAR = new ServiceCoordinate(ServiceType.QUERY, Protocol.PROTOSTELLAR);
  public static ServiceCoordinate ANALYTICS_PROTOSTELLAR = new ServiceCoordinate(ServiceType.ANALYTICS, Protocol.PROTOSTELLAR);
  public static ServiceCoordinate EVENTING_PROTOSTELLAR = new ServiceCoordinate(ServiceType.EVENTING, Protocol.PROTOSTELLAR);
  public static ServiceCoordinate VIEWS_PROTOSTELLAR = new ServiceCoordinate(ServiceType.VIEWS, Protocol.PROTOSTELLAR);
  public static ServiceCoordinate MANAGER_PROTOSTELLAR = new ServiceCoordinate(ServiceType.MANAGER, Protocol.PROTOSTELLAR);
  public static ServiceCoordinate SEARCH_PROTOSTELLAR = new ServiceCoordinate(ServiceType.SEARCH, Protocol.PROTOSTELLAR);

  public static Set<ServiceCoordinate> ALL = CbCollections.setOf(KV, QUERY, ANALYTICS, EVENTING, VIEWS, MANAGER, SEARCH, BACKUP,
    KV_PROTOSTELLAR, QUERY_PROTOSTELLAR, ANALYTICS_PROTOSTELLAR, EVENTING_PROTOSTELLAR, VIEWS_PROTOSTELLAR, MANAGER_PROTOSTELLAR, SEARCH_PROTOSTELLAR);

  public ServiceCoordinate(ServiceType serviceType, Protocol protocol) {
    this.serviceType = serviceType;
    this.protocol = protocol;
  }

  public ServiceType serviceType() {
    return serviceType;
  }

  public Protocol protocol() {
    return protocol;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServiceCoordinate that = (ServiceCoordinate) o;
    return serviceType == that.serviceType && protocol == that.protocol;
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceType, protocol);
  }

  @Override
  public String toString() {
    return "ServiceTypeAndProtocol{" +
      "serviceType=" + serviceType +
      ", protocol=" + protocol +
      '}';
  }

  public String ident() {
    return serviceType.ident();
  }
}

