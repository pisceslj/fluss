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

import java.time.Duration;

/** Time abstraction used by blocking client operations and tests. */
public interface Time {

    Time SYSTEM = new SystemTime();

    long milliseconds();

    void sleep(long ms);

    default Timer timer(long timeoutMs) {
        return new Timer(this, timeoutMs);
    }

    default Timer timer(Duration timeout) {
        return timer(timeout.toMillis());
    }
}
