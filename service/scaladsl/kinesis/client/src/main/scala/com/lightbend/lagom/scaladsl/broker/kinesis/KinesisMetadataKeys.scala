/*
 * Copyright (C) 2018 Quotable Value Limited. <https://github.com/Quotable-Value>
 */
package com.lightbend.lagom.scaladsl.broker.kinesis

import java.time.Instant

import com.lightbend.lagom.scaladsl.api.broker.MetadataKey

/**
  * Metadata keys specific to the Kinesis broker implementation.
  */
object KinesisMetadataKeys {

  /** See [[com.amazonaws.services.kinesis.clientlibrary.types.UserRecord]] for more details. */
  val ExplicitHashKey: MetadataKey[Option[String]] = MetadataKey("kinesisExplicitHashKey")

  /** See [[com.amazonaws.services.kinesis.model.Record]] for more details. */
  val SequenceNumber: MetadataKey[String] = MetadataKey("kinesisSequenceNumber")

  /** See [[com.amazonaws.services.kinesis.clientlibrary.types.UserRecord]] for more details. */
  val SubSequenceNumber: MetadataKey[Option[Long]] = MetadataKey("kinesisSubSequenceNumber")

  /** See [[com.amazonaws.services.kinesis.model.Record]] for more details. */
  val ApproximateArrivalTimestamp: MetadataKey[Instant] = MetadataKey("kinesisApproximateArrivalTimestamp")

  /** See [[com.amazonaws.services.kinesis.model.Record]] for more details. */
  val EncryptionType: MetadataKey[String] = MetadataKey("kinesisEncryptionType")

}
