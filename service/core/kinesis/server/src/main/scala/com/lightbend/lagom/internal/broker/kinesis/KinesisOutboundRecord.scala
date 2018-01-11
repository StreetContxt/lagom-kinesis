package com.lightbend.lagom.internal.broker.kinesis

import java.nio.ByteBuffer

case class KinesisOutboundRecord(
  data: ByteBuffer,
  partitionKey: String,
  explicitHashKey: Option[String]
)