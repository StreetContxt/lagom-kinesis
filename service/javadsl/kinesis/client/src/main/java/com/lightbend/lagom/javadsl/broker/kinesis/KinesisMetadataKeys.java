/*
 * Copyright (C) 2018 Quotable Value Limited. <https://github.com/Quotable-Value>
 */
package com.lightbend.lagom.javadsl.broker.kinesis;

import com.lightbend.lagom.javadsl.api.broker.MetadataKey;

import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Metadata keys specific to the Kinesis broker implementation.
 */
public final class KinesisMetadataKeys {

    private KinesisMetadataKeys() {
    }

    /** See [[com.amazonaws.services.kinesis.clientlibrary.types.UserRecord.getExplicitHashKey]] for more details. */
    public static final MetadataKey<Optional<String>> EXPLICIT_HASH_KEY = MetadataKey.named("kinesisExplicitHashKey");

    /** See [[com.amazonaws.services.kinesis.model.Record.getSequenceNumber]] for more details. */
    public static final MetadataKey<String> SEQUENCE_NUMBER = MetadataKey.named("kinesisSequenceNumber");

    /** See [[com.amazonaws.services.kinesis.clientlibrary.types.UserRecord.getSubSequenceNumber]] for more details. */
    public static final MetadataKey<OptionalLong> SUB_SEQUENCE_NUMBER = MetadataKey.named("kinesisSubSequenceNumber");

    /** See [[com.amazonaws.services.kinesis.model.Record.getApproximateArrivalTimestamp]] for more details. */
    public static final MetadataKey<Instant> APPROXIMATE_ARRIVAL_TIMESTAMP = MetadataKey.named("kinesisApproximateArrivalTimestamp");

    /** See [[com.amazonaws.services.kinesis.model.Record.getEncryptionType]] for more details. */
    public static final MetadataKey<String> ENCRYPTION_TYPE = MetadataKey.named("kinesisEncryptionType");

}
