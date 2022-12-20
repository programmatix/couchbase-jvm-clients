/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.core.config;

import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.service.ServiceCoordinate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PortInfo {

    private final Map<ServiceCoordinate, Integer> ports;
    private final Map<ServiceCoordinate, Integer> sslPorts;
    private final Map<String, AlternateAddress> alternateAddresses;
    private final String hostname;

    /**
     * Creates a new {@link PortInfo}.
     *
     * <p>Note that if the hostname is null (not provided by the server), it is explicitly set to
     * null because otherwise the loaded InetAddress would point to localhost.</p>
     *
     * @param services the list of services mapping to ports.
     */
    @JsonCreator
    public PortInfo(
        @JsonProperty("services") Map<String, Integer> services,
        @JsonProperty("hostname") String hostname,
        @JsonProperty("alternateAddresses") Map<String, AlternateAddress> aa
    ) {
        ports = new HashMap<>();
        sslPorts = new HashMap<>();
        alternateAddresses = aa == null ? Collections.emptyMap() : aa;
        this.hostname = hostname; // might be null when decoded from JSON, covered at a higher level

        extractPorts(services, ports, sslPorts);
    }

    /**
     * Alternate constructor, used from the global config usually.
     *
     * @param ports the parsed ports.
     * @param sslPorts the parsed ssl ports.
     * @param alternateAddresses the parsed alternate addresses.
     * @param hostname the hostname of the port info (node).
     */
    PortInfo(final Map<ServiceCoordinate, Integer> ports, final Map<ServiceCoordinate, Integer> sslPorts,
             final Map<String, AlternateAddress> alternateAddresses, final String hostname) {
      this.ports = requireNonNull(ports);
      this.sslPorts = requireNonNull(sslPorts);
      this.alternateAddresses = requireNonNull(alternateAddresses);
      this.hostname = requireNonNull(hostname);
    }

    public NodeIdentifier identifier() {
      return new NodeIdentifier(hostname, ports.get(ServiceCoordinate.MANAGER));
    }

    /**
     * Helper method to extract ports from the raw services port mapping.
     *
     * @param input the raw input ports
     * @param ports the output direct ports
     * @param sslPorts the output ssl ports
     */
    static void extractPorts(final Map<String, Integer> input,
                             final Map<ServiceCoordinate, Integer> ports,
                             final Map<ServiceCoordinate, Integer> sslPorts) {
        for (Map.Entry<String, Integer> entry : input.entrySet()) {
            String service = entry.getKey();
            int port = entry.getValue();
          switch (service) {
            case "mgmt":
              ports.put(ServiceCoordinate.MANAGER, port);
              break;
            case "capi":
              ports.put(ServiceCoordinate.VIEWS, port);
              break;
            case "kv":
              ports.put(ServiceCoordinate.KV, port);
              break;
            case "kvSSL":
              sslPorts.put(ServiceCoordinate.KV, port);
              break;
            case "capiSSL":
              sslPorts.put(ServiceCoordinate.VIEWS, port);
              break;
            case "mgmtSSL":
              sslPorts.put(ServiceCoordinate.MANAGER, port);
              break;
            case "n1ql":
              ports.put(ServiceCoordinate.QUERY, port);
              break;
            case "n1qlSSL":
              sslPorts.put(ServiceCoordinate.QUERY, port);
              break;
            case "fts":
              ports.put(ServiceCoordinate.SEARCH, port);
              break;
            case "ftsSSL":
              sslPorts.put(ServiceCoordinate.SEARCH, port);
              break;
            case "cbas":
              ports.put(ServiceCoordinate.ANALYTICS, port);
              break;
            case "cbasSSL":
              sslPorts.put(ServiceCoordinate.ANALYTICS, port);
              break;
            case "eventingAdminPort":
              ports.put(ServiceCoordinate.EVENTING, port);
              break;
            case "eventingSSL":
              sslPorts.put(ServiceCoordinate.EVENTING, port);
              break;
            case "backupAPI":
              ports.put(ServiceCoordinate.BACKUP, port);
              break;
            case "backupAPIHTTPS":
              sslPorts.put(ServiceCoordinate.BACKUP, port);
              break;
          }
        }
    }

    public Map<ServiceCoordinate, Integer> ports() {
        return ports;
    }

    public Map<ServiceCoordinate, Integer> sslPorts() {
        return sslPorts;
    }

    public String hostname() {
        return hostname;
    }

    public Map<String, AlternateAddress> alternateAddresses() {
        return alternateAddresses;
    }

    @Override
    public String toString() {
        return "PortInfo{"
            + "ports=" + ports
            + ", sslPorts=" + sslPorts
            + ", hostname='" + hostname
            + ", alternateAddresses=" + alternateAddresses
            + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PortInfo portInfo = (PortInfo) o;

        if (ports != null ? !ports.equals(portInfo.ports) : portInfo.ports != null) return false;
        if (sslPorts != null ? !sslPorts.equals(portInfo.sslPorts) : portInfo.sslPorts != null)
            return false;
        if (alternateAddresses != null ? !alternateAddresses.equals(portInfo.alternateAddresses) : portInfo.alternateAddresses != null)
            return false;
        return hostname != null ? hostname.equals(portInfo.hostname) : portInfo.hostname == null;
    }

    @Override
    public int hashCode() {
        int result = ports != null ? ports.hashCode() : 0;
        result = 31 * result + (sslPorts != null ? sslPorts.hashCode() : 0);
        result = 31 * result + (alternateAddresses != null ? alternateAddresses.hashCode() : 0);
        result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
        return result;
    }
}
