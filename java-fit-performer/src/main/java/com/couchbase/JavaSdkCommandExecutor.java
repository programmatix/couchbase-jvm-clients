/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.codec.DefaultJsonSerializer;
import com.couchbase.client.java.codec.JsonTranscoder;
import com.couchbase.client.java.codec.LegacyTranscoder;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.codec.RawStringTranscoder;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.CommonDurabilityOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.ScanOptions;
import com.couchbase.client.java.kv.ScanResult;
import com.couchbase.client.java.kv.ScanSort;
import com.couchbase.client.java.kv.ScanTerm;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.performer.core.commands.SdkCommandExecutor;
import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.performer.core.stream.StreamStreamer;
import com.couchbase.client.performer.core.util.ErrorUtil;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.kv.rangescan.Scan;
import com.couchbase.client.protocol.shared.*;
import com.couchbase.client.protocol.shared.Exception;
import com.couchbase.utils.ClusterConnection;
import com.google.protobuf.ByteString;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.couchbase.client.performer.core.util.TimeUtil.getTimeNow;
import static com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN;


/**
 * SdkOperation performs each requested SDK operation
 */
public class JavaSdkCommandExecutor extends SdkCommandExecutor {
    private static final JsonTranscoder JSON_TRANSCODER = JsonTranscoder.create(DefaultJsonSerializer.create());
    private static final LegacyTranscoder LEGACY_TRANSCODER = LegacyTranscoder.create(DefaultJsonSerializer.create());

    private final ClusterConnection connection;
    private final ConcurrentHashMap<String, RequestSpan> spans;

    public JavaSdkCommandExecutor(ClusterConnection connection, Counters counters, ConcurrentHashMap<String, RequestSpan> spans) {
        super(counters);
        this.connection = connection;
        this.spans = spans;
    }

    @Override
    protected com.couchbase.client.protocol.run.Result performOperation(com.couchbase.client.protocol.sdk.Command op, PerRun perRun) {
        var result = com.couchbase.client.protocol.run.Result.newBuilder();

        if (op.hasInsert()){
            var request = op.getInsert();
            var collection = connection.collection(request.getLocation());
            var content = content(request.getContent());
            var docId = getDocId(request.getLocation());
            var options = createOptions(request, spans);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            MutationResult mr;
            if (options == null) mr = collection.insert(docId, content);
            else mr = collection.insert(docId, content, options);
            result.setElapsedNanos(System.nanoTime() - start);
            if (op.getReturnResult()) populateResult(result, mr);
            else setSuccess(result);
        } else if (op.hasGet()) {
            var request = op.getGet();
            var collection = connection.collection(request.getLocation());
            var docId = getDocId(request.getLocation());
            var options = createOptions(request, spans);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            GetResult gr;
            if (options == null) gr = collection.get(docId);
            else gr = collection.get(docId, options);
            result.setElapsedNanos(System.nanoTime() - start);
            if (op.getReturnResult()) populateResult(result, gr);
            else setSuccess(result);
        } else if (op.hasRemove()){
            var request = op.getRemove();
            var collection = connection.collection(request.getLocation());
            var docId = getDocId(request.getLocation());
            var options = createOptions(request, spans);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            MutationResult mr;
            if (options == null) mr = collection.remove(docId);
            else mr = collection.remove(docId, options);
            result.setElapsedNanos(System.nanoTime() - start);
            if (op.getReturnResult()) populateResult(result, mr);
            else setSuccess(result);
        } else if (op.hasReplace()){
            var request = op.getReplace();
            var collection = connection.collection(request.getLocation());
            var docId = getDocId(request.getLocation());
            var options = createOptions(request, spans);
            var content = content(request.getContent());
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            MutationResult mr;
            if (options == null) mr = collection.replace(docId, content);
            else mr = collection.replace(docId, content, options);
            result.setElapsedNanos(System.nanoTime() - start);
            if (op.getReturnResult()) populateResult(result, mr);
            else setSuccess(result);
        } else if (op.hasUpsert()){
            var request = op.getUpsert();
            var collection = connection.collection(request.getLocation());
            var docId = getDocId(request.getLocation());
            var options = createOptions(request, spans);
            var content = content(request.getContent());
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            MutationResult mr;
            if (options == null) mr = collection.upsert(docId, content);
            else mr = collection.upsert(docId, content, options);
            result.setElapsedNanos(System.nanoTime() - start);
            if (op.getReturnResult()) populateResult(result, mr);
            else setSuccess(result);
        // [start:3.4.1]
        } else if (op.hasRangeScan()){
            var request = op.getRangeScan();
            var collection = connection.collection(request.getCollection());
            var options = createOptions(request, spans);
            var scanType = convertScanType(request);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            Stream<ScanResult> results;
            if (options != null) results = collection.scan(scanType, options);
            else results = collection.scan(scanType);
            result.setElapsedNanos(System.nanoTime() - start);
            var streamer = new StreamStreamer<ScanResult>(results, perRun, request.getStreamConfig().getStreamId(), request.getStreamConfig(),
                    (ScanResult r) -> processScanResult(request, r),
                    (Throwable err) -> convertException(err));
            perRun.streamerOwner().addAndStart(streamer);
            result.setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                    .setCreated(com.couchbase.client.protocol.streams.Created.newBuilder()
                            .setType(STREAM_KV_RANGE_SCAN)
                            .setStreamId(streamer.streamId())));
        // [end:3.4.1]
        } else {
            throw new UnsupportedOperationException(new IllegalArgumentException("Unknown operation"));
        }

        return result.build();
    }

    // [start:3.4.1]
    public static Result processScanResult(Scan request, ScanResult r) {
        try {
            byte[] bytes;

            var builder = com.couchbase.client.protocol.sdk.kv.rangescan.ScanResult.newBuilder()
                    .setId(r.id())
                    .setIdOnly(r.idOnly())
                    .setStreamId(request.getStreamConfig().getStreamId());

            if (!r.idOnly()) {
                builder.setCas(r.cas());
                if (r.expiryTime().isPresent()) {
                    builder.setExpiryTime(r.expiryTime().get().getEpochSecond());
                }
            }

            if (request.hasContentAs()) {
                if (request.getContentAs().hasAsString()) {
                    bytes = r.contentAs(String.class).getBytes(StandardCharsets.UTF_8);
                } else if (request.getContentAs().hasAsByteArray()) {
                    bytes = r.contentAsBytes();
                } else if (request.getContentAs().hasAsJson()) {
                    bytes = r.contentAs(JsonObject.class).toBytes();
                } else throw new UnsupportedOperationException("Unknown contentAs");

                builder.setContent(ByteString.copyFrom(bytes));
            }

            return Result.newBuilder()
                    .setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                            .setRangeScanResult(builder.build()))
                    .build();
        } catch (RuntimeException err) {
            return Result.newBuilder()
                    .setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                            .setError(com.couchbase.client.protocol.streams.Error.newBuilder()
                                    .setException(convertExceptionShared(err))
                                    .setStreamId(request.getStreamConfig().getStreamId())))
                    .build();
        }
    }
    // [end:3.4.1]

    public static Optional<com.couchbase.client.java.kv.ScanTerm> convertScanTerm(com.couchbase.client.protocol.sdk.kv.rangescan.ScanTermChoice st) {
        if (st.hasDefault()) {
            return Optional.empty();
        }
        else if (st.hasMaximum()) {
            return Optional.of(com.couchbase.client.java.kv.ScanTerm.maximum());
        }
        else if (st.hasMinimum()) {
            return Optional.of(com.couchbase.client.java.kv.ScanTerm.minimum());
        }
        else if (st.hasTerm()) {
            var stt = st.getTerm();
            if (stt.hasExclusive() && stt.getExclusive()) {
                if (stt.hasAsString()) {
                    return Optional.of(com.couchbase.client.java.kv.ScanTerm.exclusive(stt.getAsString()));
                }
                else if (stt.hasAsBytes()) {
                    return Optional.of(com.couchbase.client.java.kv.ScanTerm.exclusive(stt.getAsBytes().toByteArray()));
                }
                else throw new UnsupportedOperationException();
            }
            if (stt.hasAsString()) {
                return Optional.of(com.couchbase.client.java.kv.ScanTerm.inclusive(stt.getAsString()));
            }
            else if (stt.hasAsBytes()) {
                return Optional.of(com.couchbase.client.java.kv.ScanTerm.inclusive(stt.getAsBytes().toByteArray()));
            }
            else throw new UnsupportedOperationException();
        }
        else throw new UnsupportedOperationException();
    }

    public static com.couchbase.client.java.kv.ScanType convertScanType(com.couchbase.client.protocol.sdk.kv.rangescan.Scan request) {
        if (request.getScanType().hasRange()) {
            var rs = request.getScanType().getRange();
            if (rs.hasFromTo()) {
                var from = convertScanTerm(rs.getFromTo().getFrom());
                var to = convertScanTerm(rs.getFromTo().getTo());
                if (from.isPresent() && to.isPresent()) {
                    return com.couchbase.client.java.kv.ScanType.rangeScan(from.get(), to.get());
                }
                else if (from.isPresent()) {
                    return com.couchbase.client.java.kv.ScanType.rangeScan(from.get(), ScanTerm.maximum());
                }
                else if (to.isPresent()) {
                    return com.couchbase.client.java.kv.ScanType.rangeScan(ScanTerm.minimum(), to.get());
                }
                else {
                    return com.couchbase.client.java.kv.ScanType.rangeScan();
                }
            }
            else if (rs.hasDocIdPrefix()) {
                return com.couchbase.client.java.kv.ScanType.prefixScan(rs.getDocIdPrefix());
            }
            else throw new UnsupportedOperationException();
        }
        else if (request.getScanType().hasSampling()) {
            var ss = request.getScanType().getSampling();
            if (ss.hasSeed()) {
                return com.couchbase.client.java.kv.ScanType.samplingScan(ss.getLimit(), ss.getSeed());
            }
            return com.couchbase.client.java.kv.ScanType.samplingScan(ss.getLimit());
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    protected Exception convertException(Throwable raw) {
        return convertExceptionShared(raw);
    }

    public static Exception convertExceptionShared(Throwable raw) {
        var ret = com.couchbase.client.protocol.shared.Exception.newBuilder();

        if (raw instanceof CouchbaseException || raw instanceof UnsupportedOperationException) {
            CouchbaseExceptionType type;
            if (raw instanceof UnsupportedOperationException) {
                type = CouchbaseExceptionType.SDK_UNSUPPORTED_OPERATION_EXCEPTION;
            }
            else {
                var err = (CouchbaseException) raw;
                type = ErrorUtil.convertException(err);
            }

            if (type != null) {
                var out = CouchbaseExceptionEx.newBuilder()
                        .setName(raw.getClass().getSimpleName())
                        .setType(type)
                        .setSerialized(raw.toString());
                if (raw.getCause() != null) {
                    out.setCause(convertExceptionShared(raw.getCause()));
                }

                ret.setCouchbase(out);
            }
        }
        else {
            ret.setOther(ExceptionOther.newBuilder()
                    .setName(raw.getClass().getSimpleName())
                    .setSerialized(raw.toString()));
        }

        return ret.build();
    }

    public static void setSuccess(com.couchbase.client.protocol.run.Result.Builder result) {
        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setSuccess(true));
    }

    public static void populateResult(com.couchbase.client.protocol.run.Result.Builder result, MutationResult value) {
        var builder = com.couchbase.client.protocol.sdk.kv.MutationResult.newBuilder()
                .setCas(value.cas());
        value.mutationToken().ifPresent(mt ->
            builder.setMutationToken(com.couchbase.client.protocol.shared.MutationToken.newBuilder()
                            .setPartitionId(mt.partitionID())
                            .setPartitionUuid(mt.partitionUUID())
                            .setSequenceNumber(mt.sequenceNumber())
                            .setBucketName(mt.bucketName())));
        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setMutationResult(builder));
    }

    public static void populateResult(com.couchbase.client.protocol.run.Result.Builder result, GetResult value) {
        var builder = com.couchbase.client.protocol.sdk.kv.GetResult.newBuilder()
                .setCas(value.cas())
                // contentAsBytes was added later
                .setContent(ByteString.copyFrom(value.contentAs(JsonObject.class).toString().getBytes()));

        // [start:3.0.7]
        value.expiryTime().ifPresent(et -> builder.setExpiryTime(et.getEpochSecond()));
        // [end:3.0.7]

        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setGetResult(builder));
    }

    public static Object content(Content content) {
        if (content.hasPassthroughString()) {
            return content.getPassthroughString();
        }
        else if (content.hasConvertToJson()) {
            return JsonObject.fromJson(content.getConvertToJson().toByteArray());
        }
        throw new UnsupportedOperationException("Unknown content type");
    }

    public static @Nullable InsertOptions createOptions(com.couchbase.client.protocol.sdk.kv.Insert request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = InsertOptions.insertOptions();   
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    // [start:3.0.7]
                    out.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                    // [end:3.0.7]
                    // [start:<3.0.7]
                    throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end:<3.0.7]
                }
                else if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else throw new UnsupportedOperationException("Unknown expiry");
            }
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    // [start:3.4.1]
    public static @Nullable ScanOptions createOptions(com.couchbase.client.protocol.sdk.kv.rangescan.Scan request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = ScanOptions.scanOptions();
            if (opts.hasIdsOnly()) out.idsOnly(opts.getIdsOnly());
            if (opts.hasConsistentWith()) out.consistentWith(convertMutationState(opts.getConsistentWith()));
            if (opts.hasSort()) {
                out.sort(switch (opts.getSort()) {
                    case KV_RANGE_SCAN_SORT_NONE -> ScanSort.NONE;
                    case KV_RANGE_SCAN_SORT_ASCENDING -> ScanSort.ASCENDING;
                    default -> throw new UnsupportedOperationException();
                });
            }
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            if (opts.hasBatchByteLimit()) out.batchByteLimit(opts.getBatchByteLimit());
            if (opts.hasBatchItemLimit()) out.batchItemLimit(opts.getBatchItemLimit());
            // Presumably will be added soon, but not currently in Java SDK
            if (opts.hasBatchTimeLimit()) throw new UnsupportedOperationException();
            return out;
        }
        else return null;
    }
    // [end:3.4.1]

    public static @Nullable RemoveOptions createOptions(com.couchbase.client.protocol.sdk.kv.Remove request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = RemoveOptions.removeOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);
            if (opts.hasCas()) out.cas(opts.getCas());
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static @Nullable GetOptions createOptions(com.couchbase.client.protocol.sdk.kv.Get request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = GetOptions.getOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasWithExpiry()) out.withExpiry(opts.getWithExpiry());
            if (opts.getProjectionCount() > 0) out.project(opts.getProjectionList().stream().toList());
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static @Nullable ReplaceOptions createOptions(com.couchbase.client.protocol.sdk.kv.Replace request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = ReplaceOptions.replaceOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    // [start:3.0.7]
                    out.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                    // [end:3.0.7]
                    // [start:<3.0.7]
                    throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end:<3.0.7]
                }
                else if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else throw new UnsupportedOperationException("Unknown expiry");
            }
            if (opts.hasPreserveExpiry()) {
                // [start:3.1.5]
                out.preserveExpiry(opts.getPreserveExpiry());
                // [end:3.1.5]
                // [start:<3.1.5]
                throw new UnsupportedOperationException();
                // [end:<3.1.5]
            }
            if (opts.hasCas()) out.cas(opts.getCas());
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static @Nullable UpsertOptions createOptions(com.couchbase.client.protocol.sdk.kv.Upsert request, ConcurrentHashMap<String, RequestSpan> spans) {
        if (request.hasOptions()) {
            var opts = request.getOptions();
            var out = UpsertOptions.upsertOptions();
            if (opts.hasTimeoutMsecs()) out.timeout(Duration.ofMillis(opts.getTimeoutMsecs()));
            if (opts.hasDurability()) convertDurability(opts.getDurability(), out);
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    // [start:3.0.7]
                    out.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                    // [end:3.0.7]
                    // [start:<3.0.7]
                    throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
                    // [end:<3.0.7]
                }
                else if (opts.getExpiry().hasRelativeSecs()) out.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                else throw new UnsupportedOperationException("Unknown expiry");
            }
            if (opts.hasPreserveExpiry()) {
                // [start:3.1.5]
                out.preserveExpiry(opts.getPreserveExpiry());
                // [end:3.1.5]
                // [start:<3.1.5]
                throw new UnsupportedOperationException();
                // [end:<3.1.5]
            }
            if (opts.hasTranscoder()) out.transcoder(convertTranscoder(opts.getTranscoder()));
            if (opts.hasParentSpanId()) out.parentSpan(spans.get(opts.getParentSpanId()));
            return out;
        }
        else return null;
    }

    public static void convertDurability(com.couchbase.client.protocol.shared.DurabilityType durability, CommonDurabilityOptions options) {
        if (durability.hasDurabilityLevel()) {
            options.durability(switch (durability.getDurabilityLevel()) {
                case NONE -> DurabilityLevel.NONE;
                case MAJORITY -> DurabilityLevel.MAJORITY;
                case MAJORITY_AND_PERSIST_TO_ACTIVE -> DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
                case PERSIST_TO_MAJORITY -> DurabilityLevel.PERSIST_TO_MAJORITY;
                default -> throw new UnsupportedOperationException("Unexpected value: " + durability.getDurabilityLevel());
            });
        }
        else if (durability.hasObserve()) {
            options.durability(switch (durability.getObserve().getPersistTo()) {
                        case PERSIST_TO_NONE -> PersistTo.NONE;
                        case PERSIST_TO_ACTIVE -> PersistTo.ACTIVE;
                        case PERSIST_TO_ONE -> PersistTo.ONE;
                        case PERSIST_TO_TWO -> PersistTo.TWO;
                        case PERSIST_TO_THREE -> PersistTo.THREE;
                        case PERSIST_TO_FOUR -> PersistTo.FOUR;
                        default -> throw new UnsupportedOperationException("Unexpected value: " + durability.getDurabilityLevel());
                    }, switch (durability.getObserve().getReplicateTo()) {
                        case REPLICATE_TO_NONE -> ReplicateTo.NONE;
                        case REPLICATE_TO_ONE -> ReplicateTo.ONE;
                        case REPLICATE_TO_TWO -> ReplicateTo.TWO;
                        case REPLICATE_TO_THREE -> ReplicateTo.THREE;
                        default -> throw new UnsupportedOperationException("Unexpected value: " + durability.getDurabilityLevel());
                    });
        }
        else {
            throw new UnsupportedOperationException("Unknown durability");
        }
    }

    public static Transcoder convertTranscoder(com.couchbase.client.protocol.shared.Transcoder transcoder) {
        if (transcoder.hasRawJson()) return RawJsonTranscoder.INSTANCE;
        if (transcoder.hasJson()) return JSON_TRANSCODER;
        if (transcoder.hasLegacy()) return LEGACY_TRANSCODER;
        if (transcoder.hasRawString()) return RawStringTranscoder.INSTANCE;
        if (transcoder.hasRawBinary()) return RawBinaryTranscoder.INSTANCE;
        throw new UnsupportedOperationException("Unknown transcoder");
    }

    public static MutationState convertMutationState(com.couchbase.client.protocol.shared.MutationState consistentWith) {
        var mutationTokens = consistentWith.getTokensList().stream()
                .map(mt -> new com.couchbase.client.core.msg.kv.MutationToken((short) mt.getPartitionId(),
                        mt.getPartitionUuid(),
                        mt.getSequenceNumber(),
                        mt.getBucketName()))
                .toList();
        return MutationState.from(mutationTokens.toArray(new com.couchbase.client.core.msg.kv.MutationToken[0]));
    }
}
