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

/** Per-bucket result of altering committed consumer offsets. */
@PublicEvolving
public final class AlterConsumerOffsetsResult {
    private final Map<TableBucket, BucketOutcome> outcomes;

    public AlterConsumerOffsetsResult(Map<TableBucket, BucketOutcome> outcomes) {
        this.outcomes =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(Objects.requireNonNull(outcomes, "outcomes")));
    }

    public Map<TableBucket, BucketOutcome> outcomes() {
        return outcomes;
    }

    /** Outcome for one table bucket. */
    public static final class BucketOutcome {
        public final long newOffset;
        public final String errorCode;
        public final String errorMessage;

        public BucketOutcome(long newOffset, String errorCode, String errorMessage) {
            this.newOffset = newOffset;
            this.errorCode = errorCode == null ? "NONE" : errorCode;
            this.errorMessage = errorMessage == null ? "" : errorMessage;
        }

        public long newOffset() {
            return newOffset;
        }

        public String errorCode() {
            return errorCode;
        }

        public String errorMessage() {
            return errorMessage;
        }

        public boolean isSuccess() {
            return "NONE".equals(errorCode);
        }
    }
}
