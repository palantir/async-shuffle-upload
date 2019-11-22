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

package com.palantir.spark.shuffle.async.data.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.spark.shuffle.async.immutables.ImmutablesStyle;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@ImmutablesStyle
@JsonDeserialize(as = ImmutableHadoopAsyncUploadConfig.class)
@JsonSerialize(as = ImmutableHadoopAsyncUploadConfig.class)
public interface HadoopAsyncUploadConfig {

    @JsonProperty("base-path")
    String basePath();

    @JsonProperty("access-key-id")
    Optional<String> accessKeyId();

    @JsonProperty("secret-key")
    Optional<String> secretKey();

    class Builder extends ImmutableHadoopAsyncUploadConfig.Builder {}

    static Builder builder() {
        return new Builder();
    }
}
