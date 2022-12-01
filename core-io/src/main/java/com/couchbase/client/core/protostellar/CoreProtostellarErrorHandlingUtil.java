package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.deps.com.google.protobuf.Any;
import com.couchbase.client.core.deps.com.google.protobuf.InvalidProtocolBufferException;
import com.couchbase.client.core.deps.com.google.rpc.PreconditionFailure;
import com.couchbase.client.core.deps.com.google.rpc.ResourceInfo;
import com.couchbase.client.core.deps.com.google.rpc.Status;
import com.couchbase.client.core.deps.io.grpc.StatusRuntimeException;
import com.couchbase.client.core.deps.io.grpc.protobuf.StatusProto;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentLockedException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.context.ProtostellarErrorContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryReason;

import java.util.concurrent.ExecutionException;

public class CoreProtostellarErrorHandlingUtil {
  private CoreProtostellarErrorHandlingUtil() {}

  private static final String PRECONDITION_CAS = "CAS";
  private static final String PRECONDITION_LOCKED = "LOCKED";
  private static final String TYPE_URL_PRECONDITION_FAILURE = "type.googleapis.com/google.rpc.PreconditionFailure";
  private static final String TYPE_URL_RESOURCE_INFO = "type.googleapis.com/google.rpc.ResourceInfo";

  public static <TResponse> ProtostellarFailureBehaviour convertKeyValueException(Throwable t,
                                                                                  ProtostellarRequest<TResponse> request) {
    if (t instanceof ExecutionException) {
      return convertKeyValueException(t.getCause(), request);
    }

    ProtostellarErrorContext context = new ProtostellarErrorContext(request.context());

    if (t instanceof StatusRuntimeException) {
      // https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto
      StatusRuntimeException sre = (StatusRuntimeException) t;

      Status status = StatusProto.fromThrowable(sre);
      context.put("server", status.getMessage());

      if (status.getDetailsCount() > 0) {
        Any details = status.getDetails(0);
        String typeUrl = details.getTypeUrl();

        // https://github.com/grpc/grpc-web/issues/399#issuecomment-443248907
        try {
          if (typeUrl.equals(TYPE_URL_PRECONDITION_FAILURE)) {
            PreconditionFailure info = PreconditionFailure.parseFrom(details.getValue());

            if (info.getViolationsCount() > 0) {
              PreconditionFailure.Violation violation = info.getViolations(0);
              String type = violation.getType();

              if (type.equals(PRECONDITION_CAS)) {
                return new ProtostellarFailureBehaviour(null, new CasMismatchException(context), context);
              }
              else if (type.equals(PRECONDITION_LOCKED)) {
                return new ProtostellarFailureBehaviour(RetryReason.KV_LOCKED, new DocumentLockedException(context), context);
              }
            }
          } else if (typeUrl.equals(TYPE_URL_RESOURCE_INFO)) {
            ResourceInfo info = ResourceInfo.parseFrom(details.getValue());

            context.put("resourceName", info.getResourceName());
            context.put("resourceType", info.getResourceType());
          }
        } catch (InvalidProtocolBufferException e) {
          return new ProtostellarFailureBehaviour(null, new DecodingFailureException("Failed to decode GRPC response", e), context);
        }
      }

      com.couchbase.client.core.deps.io.grpc.Status.Code code = sre.getStatus().getCode();

      switch (code) {
        case ALREADY_EXISTS:
          return new ProtostellarFailureBehaviour(null, new DocumentExistsException(context), context);
        case NOT_FOUND:
          return new ProtostellarFailureBehaviour(null, new DocumentNotFoundException(context), context);
        default:
          // TODO snbrett handle all codes
          return new ProtostellarFailureBehaviour(null, new UnsupportedOperationException("Unhandled error code " + code), context);
      }
    } else if (t instanceof RuntimeException) {
      return new ProtostellarFailureBehaviour(null, (RuntimeException) t, context);
    } else {
      return new ProtostellarFailureBehaviour(null, new RuntimeException(t), context);
    }
  }
}
