/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Committed offsets returned for a consumer group. */
@PublicEvolving
public final class ConsumerGroupOffsetsResult {
    private final String groupId;
    private final Map<TableBucket, Entry> entries;

    public ConsumerGroupOffsetsResult(String groupId, Map<TableBucket, Entry> entries) {
        this.groupId = Objects.requireNonNull(groupId, "groupId must not be null");
        this.entries =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(Objects.requireNonNull(entries, "entries")));
    }

    public String groupId() {
        return groupId;
    }

    public Map<TableBucket, Entry> entries() {
        return entries;
    }

    /** One committed consumer offset. */
    public static final class Entry {
        public final long committedOffset;
        public final String metadata;

        public Entry(long committedOffset, String metadata) {
            this.committedOffset = committedOffset;
            this.metadata = metadata == null ? "" : metadata;
        }

        public long committedOffset() {
            return committedOffset;
        }

        public String metadata() {
            return metadata;
        }
    }
}
