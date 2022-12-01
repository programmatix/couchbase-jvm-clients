This file will be removed before release, it's just internal development notes.

# Services
Protostellar/Stellar-nebula is not a service, so it doesn't go in ServiceType.  It's just a proxy/gateway/forwarder.
It _does_ do transactions service work, but that may be an external service later.  So we should have ServiceType.TRANSACTIONS, but should see it as something like query or KV - e.g. SN is just forwarding to that service.
