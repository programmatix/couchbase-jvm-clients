/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.kotlin.codec

import com.fasterxml.jackson.core.type.TypeReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

internal class TypeRefTest {

    @Test
    fun `captures same type as Jackson TypeReference`() {
        assertEquals(
            object : TypeReference<List<String>>() {}.type,
            typeRef<List<String>>().type
        )
    }

    @Test
    fun `captures nullability`() {
        assertFalse(typeRef<String>().nullable)
        assertTrue(typeRef<String?>().nullable)
    }
}
