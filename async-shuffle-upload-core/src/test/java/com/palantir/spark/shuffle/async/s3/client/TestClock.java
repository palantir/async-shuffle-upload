/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.spark.shuffle.async.s3.client;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public final class TestClock extends Clock {

    private final ZoneId initialZoneId;
    private Instant currentInstant;

    public TestClock() {
        Clock utcCLock = Clock.systemUTC();
        this.initialZoneId = utcCLock.getZone();
        this.currentInstant = utcCLock.instant();
    }

    @Override
    public ZoneId getZone() {
        return initialZoneId;
    }

    @Override
    public Clock withZone(ZoneId _zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return currentInstant;
    }

    public void addMillis(long millis) {
        currentInstant = currentInstant.plusMillis(millis);
    }

    public void addSeconds(long seconds) {
        currentInstant = currentInstant.plusSeconds(seconds);
    }

    public void addMinutes(long minutes) {
        currentInstant = currentInstant.plusSeconds(minutes);
    }
}
