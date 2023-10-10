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
package com.couchbase.client.scala.transformers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.couchbase.client.scala.json.JsonArray;
import com.couchbase.client.scala.json.JsonObject;
import scala.collection.Iterator;

import java.io.IOException;

/** Tells Jackson how to produce [[JsonObject]] and [[JsonArray]]. */
public class JacksonTransformers {

    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static final SimpleModule JSON_VALUE_MODULE = new SimpleModule("JsonValueModule",
        new Version(1, 0, 0, null, null, null));

    private JacksonTransformers() {}

    static {
        JSON_VALUE_MODULE.addSerializer(JsonObject.class, new JacksonTransformers.JsonObjectSerializer());
        JSON_VALUE_MODULE.addSerializer(JsonArray.class, new JacksonTransformers.JsonArraySerializer());
        JSON_VALUE_MODULE.addDeserializer(JsonObject.class, new JacksonTransformers.JsonObjectDeserializer());
        JSON_VALUE_MODULE.addDeserializer(JsonArray.class, new JacksonTransformers.JsonArrayDeserializer());
        MAPPER.registerModule(JacksonTransformers.JSON_VALUE_MODULE);
    }

    static class JsonObjectSerializer extends JsonSerializer<JsonObject> {
        @Override
        public void serialize(JsonObject value, JsonGenerator jgen,
                              SerializerProvider provider) throws IOException {
            Iterator<String> it = value.names().iterator();

            jgen.writeStartObject();
            while (it.hasNext()) {
                String name = it.next();
                Object o = value.get(name);
                jgen.writeObjectField(name, o);
            }
            jgen.writeEndObject();
        }
    }

    static class JsonArraySerializer extends JsonSerializer<JsonArray> {
        @Override
        public void serialize(JsonArray value, JsonGenerator jgen,
                              SerializerProvider provider) throws IOException {
            Iterator<Object> it = value.iterator();

            jgen.writeStartArray();
            while (it.hasNext()) {
                Object o = it.next();
                jgen.writeObject(o);
            }
            jgen.writeEndArray();
        }
    }

    static abstract class AbstractJsonValueDeserializer<T> extends JsonDeserializer<T> {

        private final boolean decimalForFloat;

        public AbstractJsonValueDeserializer() {
            decimalForFloat = Boolean.parseBoolean(
                System.getProperty("com.couchbase.json.decimalForFloat", "false")
            );
        }

        protected JsonObject decodeObject(final JsonParser parser, final JsonObject target) throws IOException {
            JsonToken current = parser.nextToken();
            String field = null;
            while(current != null && current != JsonToken.END_OBJECT) {
                if (current == JsonToken.START_OBJECT) {
                    target.put(field, decodeObject(parser, JsonObject.create()));
                } else if (current == JsonToken.START_ARRAY) {
                    target.put(field, decodeArray(parser, JsonArray.create()));
                } else if (current == JsonToken.FIELD_NAME) {
                    field = parser.getCurrentName();
                } else {
                    switch(current) {
                        case VALUE_TRUE:
                        case VALUE_FALSE:
                            target.put(field, parser.getBooleanValue());
                            break;
                        case VALUE_STRING:
                            target.put(field, parser.getValueAsString());
                            break;
                        case VALUE_NUMBER_INT:
                        case VALUE_NUMBER_FLOAT:
                            Number numberValue = parser.getNumberValue();
                            if (numberValue instanceof Double && decimalForFloat) {
                                numberValue = parser.getDecimalValue();
                            }
                            target.put(field, numberValue);
                            break;
                        case VALUE_NULL:
                            target.put(field, (JsonObject) null);
                            break;
                        default:
                            throw new IllegalStateException("Could not decode JSON token: " + current);
                    }
                }

                current = parser.nextToken();
            }
            return target;
        }

        protected JsonArray decodeArray(final JsonParser parser, final JsonArray target) throws IOException {
            JsonToken current = parser.nextToken();
            while (current != null && current != JsonToken.END_ARRAY) {
                if (current == JsonToken.START_OBJECT) {
                    target.add(decodeObject(parser, JsonObject.create()));
                } else if (current == JsonToken.START_ARRAY) {
                    target.add(decodeArray(parser, JsonArray.create()));
                } else {
                    switch(current) {
                        case VALUE_TRUE:
                        case VALUE_FALSE:
                            target.add(parser.getBooleanValue());
                            break;
                        case VALUE_STRING:
                            target.add(parser.getValueAsString());
                            break;
                        case VALUE_NUMBER_INT:
                        case VALUE_NUMBER_FLOAT:
                            Number numberValue = parser.getNumberValue();
                            if (numberValue instanceof Double && decimalForFloat) {
                                numberValue = parser.getDecimalValue();
                            }
                            target.add(numberValue);
                            break;
                        case VALUE_NULL:
                            target.add((JsonObject) null);
                            break;
                        default:
                            throw new IllegalStateException("Could not decode JSON token.");
                    }
                }

                current = parser.nextToken();
            }
            return target;
        }
    }

    static class JsonArrayDeserializer extends AbstractJsonValueDeserializer<JsonArray> {
        @Override
        public JsonArray deserialize(JsonParser jp, DeserializationContext ctx)
            throws IOException {
            if (jp.getCurrentToken() == JsonToken.START_ARRAY) {
                return decodeArray(jp, JsonArray.create());
            } else {
                throw new IllegalStateException("Expecting Array as root level object, " +
                    "was: " + jp.getCurrentToken());
            }
        }
    }

    static class JsonObjectDeserializer extends AbstractJsonValueDeserializer<JsonObject> {
        @Override
        public JsonObject deserialize(JsonParser jp, DeserializationContext ctx)
            throws IOException {
            if (jp.getCurrentToken() == JsonToken.START_OBJECT) {
                return decodeObject(jp, JsonObject.create());
            } else {
                throw new IllegalStateException("Expecting Object as root level object, " +
                    "was: " + jp.getCurrentToken());
            }
        }
    }

    public static JsonObject stringToJsonObject(String input) throws Exception {
        return MAPPER.readValue(input, JsonObject.class);
    }

    public static JsonArray stringToJsonArray(String input) throws Exception {
        return MAPPER.readValue(input, JsonArray.class);
    }

    public static JsonArray bytesToJsonArray(byte[] input) throws Exception {
        return MAPPER.readValue(input, JsonArray.class);
    }

    public static JsonObject bytesToJsonObject(byte[] input) throws Exception {
        return MAPPER.readValue(input, JsonObject.class);
    }



}
