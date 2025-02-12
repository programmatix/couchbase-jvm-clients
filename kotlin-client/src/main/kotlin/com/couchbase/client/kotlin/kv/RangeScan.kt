/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.kv

import com.couchbase.client.core.kv.CoreRangeScanSort
import com.couchbase.client.core.kv.RangeScanOrchestrator
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.internal.hexContentToString
import com.couchbase.client.kotlin.internal.isEmpty
import com.couchbase.client.kotlin.kv.KvScanConsistency.Companion.consistentWith
import com.couchbase.client.kotlin.kv.KvScanConsistency.Companion.notBounded
import com.couchbase.client.kotlin.util.StorageSize
import com.couchbase.client.kotlin.util.StorageSize.Companion.bytes

@VolatileCouchbaseApi
public val DEFAULT_SCAN_BATCH_SIZE_LIMIT: StorageSize = RangeScanOrchestrator.RANGE_SCAN_DEFAULT_BATCH_BYTE_LIMIT.bytes

@VolatileCouchbaseApi
public const val DEFAULT_SCAN_BATCH_ITEM_LIMIT: Int = RangeScanOrchestrator.RANGE_SCAN_DEFAULT_BATCH_ITEM_LIMIT

/**
 * Specifies the type of scan to do (range scan or sample scan)
 * and associated parameters.
 *
 * Create an instance using the [ScanType.prefix], [ScanType.range],
 * or [ScanType.sample] companion factory methods.
 */
@VolatileCouchbaseApi
public sealed class ScanType {
    public class Range internal constructor(
        public val from: ScanTerm,
        public val to: ScanTerm,
    ) : ScanType()

    public class Sample internal constructor(
        public val limit: Long,
        public val seed: Long?,
    ) : ScanType() {
        init {
            require(limit > 0) { "Sample size limit must be > 0 but got $limit" }
        }
    }

    public companion object {
        /**
         * Selects all document IDs in a range.
         *
         * @param from Start of the range (lower bound).
         * @param to End of the range (upper bound).
         */
        public fun range(
            from: ScanTerm = ScanTerm.Minimum,
            to: ScanTerm = ScanTerm.Maximum,
        ): Range = Range(
            from = from,
            to = to,
        )

        /**
         * Selects all document IDs with the given [prefix].
         */
        public fun prefix(
            prefix: String,
        ): Range = Range(
            from = ScanTerm(prefix),
            to = ScanTerm(prefix.toByteArray() + 0xff.toByte(), exclusive = true)
        )

        /**
         * Selects random documents.
         *
         * @param limit Upper bound (inclusive) on the number of items to select.
         * @param seed Seed for the random number generator that selects the items, or null to use a random seed.
         * **CAVEAT:** Specifying the same seed does not guarantee the same documents are selected.
         */
        public fun sample(
            limit: Long,
            seed: Long? = null,
        ): Sample = Sample(
            limit = limit,
            seed = seed,
        )
    }
}

/**
 * A lower or upper bound of a KV range scan.
 */
@VolatileCouchbaseApi
public class ScanTerm(
    term: ByteArray,
    public val exclusive: Boolean = false,
) {
    // Defensive copies to make this class fully immutable
    private val privateTerm = term.clone();
    public val term: ByteArray
        get() = privateTerm.clone()

    public constructor(
        term: String,
        exclusive: Boolean = false,
    ) : this(term.toByteArray(), exclusive)

    public companion object {
        /**
         * Before the "lowest" possible document ID.
         */
        public val Minimum: ScanTerm = ScanTerm(byteArrayOf(0x00))

        /**
         * After the "highest" possible document ID.
         */
        public val Maximum: ScanTerm = ScanTerm(byteArrayOf(0xff.toByte()))
    }

    override fun toString(): String {
        return "ScanTerm(exclusive=$exclusive" +
                ", termAsString=\"${String(privateTerm)}\"" +
                ", termBytes=${privateTerm.hexContentToString()}" +
                ")"
    }
}

/**
 * Specifies the order of scan results.
 */
@VolatileCouchbaseApi
public enum class ScanSort {
    /**
     * Do not sort the results.
     *
     * Fast and efficient. Results are emitted in the order they arrive from each partition.
     */
    NONE,

    /**
     * Sort the results by document ID in lexicographic order.
     *
     * Reasonably efficient. Suitable for large range scans, since it *does not*
     * require buffering the entire result set in memory.
     *
     * **CAVEAT**: When used with a [ScanType.sample] scan, the behavior is unspecified
     * and may change in a future version. Likely outcomes include:
     *
     * - Skewing the results towards "low" document IDs.
     * - Not actually sorting.
     *
     * If you require sorted results from a sample scan, please use [ScanSort.NONE],
     * then collect the results into a list and sort the list.
     */
    ASCENDING,
    ;

    internal fun toCore(): CoreRangeScanSort =
        when (this) {
            NONE -> CoreRangeScanSort.NONE
            ASCENDING -> CoreRangeScanSort.ASCENDING
        }
}

/**
 * Specifies whether to wait for certain KV mutations to be indexed
 * before starting the scan.
 *
 * Create instances using the [consistentWith] or [notBounded]
 * factory methods.
 */
@VolatileCouchbaseApi
public sealed class KvScanConsistency(
    internal val mutationState: MutationState?,
) {
    public companion object {
        /**
         * For when speed matters more than consistency. Executes the scan
         * immediately, without waiting for prior K/V mutations to be indexed.
         */
        public fun notBounded(): KvScanConsistency =
            NotBounded

        /**
         * Targeted consistency. Waits for specific K/V mutations to be indexed
         * before executing the scan.
         *
         * Sometimes referred to as "At Plus".
         *
         * @param tokens the mutations to await before executing the scan
         */
        public fun consistentWith(tokens: MutationState): KvScanConsistency =
            if (tokens.isEmpty()) NotBounded else ConsistentWith(tokens)
    }

    private object NotBounded : KvScanConsistency(null)

    private class ConsistentWith internal constructor(
        tokens: MutationState,
    ) : KvScanConsistency(tokens)
}
