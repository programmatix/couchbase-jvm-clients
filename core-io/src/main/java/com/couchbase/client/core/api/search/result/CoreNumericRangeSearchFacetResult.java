/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.api.search.result;

import com.couchbase.client.core.annotation.Stability;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@Stability.Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public class CoreNumericRangeSearchFacetResult extends CoreAbstractSearchFacetResult {

  private final List<CoreSearchNumericRange> numericRanges;

  public CoreNumericRangeSearchFacetResult(
      @JsonProperty("$name") String name,
      @JsonProperty("field") String field,
      @JsonProperty("total") long total,
      @JsonProperty("missing") long missing,
      @JsonProperty("other") long other,
      @JsonProperty("numeric_ranges") List<CoreSearchNumericRange> numericRanges) {
    super(name, field, total, missing, other);
    this.numericRanges = numericRanges;
  }

  public List<CoreSearchNumericRange> numericRanges() {
    return this.numericRanges;
  }

  @Override
  public String toString() {
    return "NumericRangeSearchFacetResult{" +
        "name='" + name + '\'' +
        ", field='" + field + '\'' +
        ", total=" + total +
        ", missing=" + missing +
        ", other=" + other +
        ", ranges=" + numericRanges +
        '}';
  }
}
