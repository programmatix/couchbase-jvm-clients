/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.core.error;

import com.couchbase.client.core.error.context.ErrorContext;

/**
 * Generic exception that captures all meter-related errors.
 */
public class MeterException extends CouchbaseException {

  public MeterException(String message) {
    super(message);
  }

  public MeterException(String message, ErrorContext ctx) {
    super(message, ctx);
  }

  public MeterException(String message, Throwable cause) {
    super(message, cause);
  }

  public MeterException(String message, Throwable cause, ErrorContext ctx) {
    super(message, cause, ctx);
  }

}
