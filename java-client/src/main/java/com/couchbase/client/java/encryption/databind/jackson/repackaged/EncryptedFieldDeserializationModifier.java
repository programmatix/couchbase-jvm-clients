/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.java.encryption.databind.jackson.repackaged;

import com.couchbase.client.core.annotation.Stability;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.deser.BeanDeserializerBuilder;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.SettableBeanProperty;
import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.java.encryption.annotation.Encrypted;

import java.util.ArrayList;
import java.util.List;

import static com.couchbase.client.java.encryption.databind.jackson.repackaged.RepackagedEncryptionModule.findAnnotation;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class EncryptedFieldDeserializationModifier extends BeanDeserializerModifier {
  private final CryptoManager cryptoManager;

  public EncryptedFieldDeserializationModifier(CryptoManager cryptoManager) {
    this.cryptoManager = requireNonNull(cryptoManager);
  }

  @Override
  public BeanDeserializerBuilder updateBuilder(DeserializationConfig config,
                                               BeanDescription beanDesc,
                                               BeanDeserializerBuilder builder) {

    final List<SettableBeanProperty> modified = new ArrayList<>();
    final List<PropertyName> unmangledNames = new ArrayList<>();

    builder.getProperties().forEachRemaining(prop -> {
      final Encrypted annotation = findAnnotation(prop, Encrypted.class);
      if (annotation != null) {
        final SettableBeanProperty newProp = prop
            .withName(new PropertyName(cryptoManager.mangle(prop.getName())))
            .withValueDeserializer(new EncryptedFieldDeserializer(cryptoManager, annotation));

        // Avoid ConcurrentModificationException by processing these in a separate pass
        modified.add(newProp);

        if (annotation.migration() != Encrypted.Migration.FROM_UNENCRYPTED) {
          // mark the unmangled name for removal
          unmangledNames.add(prop.getFullName());
        }
      }
    });

    // Remove references to unmangled field names; these won't appear in the JSON.
    // Do this before adding modified properties in case the name manging is a no-op.
    unmangledNames.forEach(builder::removeProperty);

    modified.forEach(builder::addProperty);

    return builder;
  }
}
