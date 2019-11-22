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

import com.google.common.collect.ImmutableSet;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum ShuffleStorageStrategy {
    BASIC("basic"),
    MERGING("merging");

    private final String value;

    ShuffleStorageStrategy(String value) {
        this.value = value;
    }

    public static ShuffleStorageStrategy fromString(String value) {
        return Arrays.stream(values())
                .filter(val -> val.value.equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new SafeIllegalArgumentException(
                        "Not a valid shuffle storage strategy.",
                        SafeArg.of("Valid strategies", Arrays.asList(ShuffleStorageStrategy.values())),
                        SafeArg.of("Invalid strategy", value)));
    }

    public String value() {
        return value;
    }

    public static Set<ShuffleStorageStrategy> valueSet() {
        return ImmutableSet.copyOf(values());
    }

    public static Set<String> strings() {
        return valueSet().stream().map(ShuffleStorageStrategy::value).collect(Collectors.toSet());
    }
}
