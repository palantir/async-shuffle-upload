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

package com.palantir.spark.shuffle.async.s3.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public final class ShuffleIdTrackerTest {

    private ShuffleIdTracker idTrackerUnderTest;

    @Before
    public void before() {
        this.idTrackerUnderTest = new ShuffleIdTracker();
    }

    @Test
    public void testAttemptRegistration() {
        idTrackerUnderTest.registerShuffle(0);
        idTrackerUnderTest.registerUnmergedMapOutput(
                new MapOutputId(0, 0, 0L));
        assertThat(idTrackerUnderTest.isMapOutputRegistered(0, 0, 0L))
                .isTrue();
    }
}
