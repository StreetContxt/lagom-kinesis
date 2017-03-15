/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.internal.javadsl.broker.kinesis

import akka.util.ByteString
import com.amazonaws.services.kinesis.model.Record
import com.gilt.gfc.aws.kinesis.client.{KinesisRecord, KinesisRecordReader, KinesisRecordWriter}
import com.lightbend.lagom.javadsl.api.deser.MessageSerializer.{NegotiatedDeserializer, NegotiatedSerializer}


/**
  * Adapts a Lagom NegotiatedDeserializer into a KinesisRecordReader so that messages
  * stored in Kinesis can be deserialized into the expected application's type.
  */
private[lagom] class JavadslKinesisRecordReader[T](deserializer: NegotiatedDeserializer[T, ByteString])
  extends KinesisRecordReader[T] {
  override def apply(r: Record): T = deserializer.deserialize(ByteString(r.getData))
}

/**
  * Adapts a Lagom NegotiatedSerializer into a KinesisRecordWriter so that application's
  * messages can be serialized into a Kinesis Record and published into Kinesis.
  */
private[lagom] class JavadslKinesisRecordWriter[T](serializer: NegotiatedSerializer[T, ByteString],
                                                   partitionKeyStrategy: T => String)
  extends KinesisRecordWriter[T] {
  override def toKinesisRecord(r: T): KinesisRecord = KinesisRecord(
    partitionKey = partitionKeyStrategy(r),
    data = serializer.serialize(r).toByteBuffer.array()
  )
}
