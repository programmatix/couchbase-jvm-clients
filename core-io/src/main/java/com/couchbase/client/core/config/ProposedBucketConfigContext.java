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

package com.couchbase.client.core.config;

import io.netty.util.internal.ObjectUtil;

import java.util.Objects;

/**
 * This context keeps together a bunch of related information needed to turn a raw
 * config into a parsed one.
 *
 * @author Michael Nitschinger
 * @since 1.5.8
 */
public class ProposedBucketConfigContext {

    private final String bucketName;
    private final String config;
    private final String origin;

    private final boolean forcesOverride;

    /**
     * Creates a new proposed bucket config context.
     *
     * @param bucketName the name of the bucket, must not be null.
     * @param config the raw config, must not be null.
     * @param origin the origin of the config, can be null.
     */
    public ProposedBucketConfigContext(final String bucketName, final String config, final String origin) {
        this(bucketName, config, origin, false);
    }

    public ProposedBucketConfigContext(final String bucketName, final String config, final String origin,
                                       final boolean forcesOverride) {
        ObjectUtil.checkNotNull(bucketName, "bucket name cannot be null!");
        ObjectUtil.checkNotNull(config, "the raw config cannot be null!");
        this.bucketName = bucketName;
        this.config = config.replace("$HOST", origin);
        this.origin = origin;
        this.forcesOverride = forcesOverride;
    }

    public String bucketName() {
        return bucketName;
    }

    public String config() {
        return config;
    }

    /**
     * Returns the origin, might be null.
     *
     * @return the origin if set, null otherwise.
     */
    public String origin() {
        return origin;
    }

    public boolean forcesOverride() {
        return forcesOverride;
    }

    public ProposedBucketConfigContext forceOverride() {
        return new ProposedBucketConfigContext(bucketName, config, origin, true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProposedBucketConfigContext that = (ProposedBucketConfigContext) o;
        return forcesOverride == that.forcesOverride && Objects.equals(bucketName, that.bucketName)
          && Objects.equals(config, that.config) && Objects.equals(origin, that.origin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketName, config, origin, forcesOverride);
    }

    @Override
    public String toString() {
        return "ProposedBucketConfigContext{" +
          "bucketName='" + bucketName + '\'' +
          ", config='" + config + '\'' +
          ", origin='" + origin + '\'' +
          ", forcesOverride=" + forcesOverride +
          '}';
    }
}
