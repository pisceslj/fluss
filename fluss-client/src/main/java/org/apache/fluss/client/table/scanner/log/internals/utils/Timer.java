/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.scanner.log.internals.utils;

/** Monotonic timeout helper backed by a {@link Time} instance. */
public final class Timer {

    private final Time time;
    private long currentTimeMs;
    private long deadlineMs;

    Timer(Time time, long timeoutMs) {
        this.time = time;
        update();
        reset(timeoutMs);
    }

    public boolean isExpired() {
        return currentTimeMs >= deadlineMs;
    }

    public boolean notExpired() {
        return !isExpired();
    }

    public void reset(long timeoutMs) {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs must not be negative");
        }
        deadlineMs =
                currentTimeMs > Long.MAX_VALUE - timeoutMs
                        ? Long.MAX_VALUE
                        : currentTimeMs + timeoutMs;
    }

    public void update() {
        update(time.milliseconds());
    }

    public void update(long nowMs) {
        currentTimeMs = Math.max(currentTimeMs, nowMs);
    }

    public long remainingMs() {
        return Math.max(0L, deadlineMs - currentTimeMs);
    }

    public void sleep(long durationMs) {
        if (durationMs < 0) {
            throw new IllegalArgumentException("durationMs must not be negative");
        }
        time.sleep(Math.min(durationMs, remainingMs()));
        update();
    }
}
