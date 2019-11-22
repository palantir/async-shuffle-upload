/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.spark.shuffle.async.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.Test;

public final class SparkShuffleAwsCredentialsTest {

    @Test
    public void testSerialize() {
        SparkShuffleAwsCredentials creds = SparkShuffleAwsCredentials.builder()
                .accessKeyId("access-key")
                .secretAccessKey("secret-key")
                .sessionToken("session-token")
                .build();
        byte[] bytes = creds.toBytes();
        assertThat(new String(bytes, StandardCharsets.UTF_8))
                .isEqualTo("{\"accessKeyId\":\"access-key\","
                        + "\"secretAccessKey\":\"secret-key\","
                        + "\"sessionToken\":\"session-token\"}");
    }

    @Test
    public void testDeserialize() {
        String serializedString = "{\"accessKeyId\":\"access-key\","
                + "\"secretAccessKey\":\"secret-key\","
                + "\"sessionToken\":\"session-token\"}";

        SparkShuffleAwsCredentials creds =
                SparkShuffleAwsCredentials.fromBytes(serializedString.getBytes(StandardCharsets.UTF_8));
    }
}
