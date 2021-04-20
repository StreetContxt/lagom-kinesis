/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.internal.scaladsl.broker.kinesis

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.util.ByteString
import com.lightbend.lagom.internal.broker.kinesis.KinesisConfig
import com.lightbend.lagom.scaladsl.api.Descriptor.TopicCall
import com.lightbend.lagom.scaladsl.api.broker.Topic.TopicId
import com.lightbend.lagom.scaladsl.api.broker.{Subscriber, Topic}
import com.lightbend.lagom.scaladsl.api.deser.MessageSerializer.NegotiatedDeserializer
import com.lightbend.lagom.scaladsl.api.{ServiceInfo, ServiceLocator}

import scala.concurrent.ExecutionContext

private[lagom] class ScaladslKinesisTopic[Payload](kinesisConfig: KinesisConfig,
                                                   topicCall: TopicCall[Payload],
                                                   info: ServiceInfo,
                                                   system: ActorSystem,
                                                   serviceLocator: ServiceLocator)
                                                  (implicit mat: Materializer,
                                                   ec: ExecutionContext) extends Topic[Payload] {

  override def topicId: TopicId = topicCall.topicId

  private val deserializer: NegotiatedDeserializer[Payload, ByteString] = {
    val messageSerializer = topicCall.messageSerializer
    val protocol = messageSerializer.serializerForRequest.protocol
    messageSerializer.deserializer(protocol)
  }

  override def subscribe: Subscriber[Payload] = new ScaladslKinesisSubscriber[Payload, Payload](kinesisConfig, topicCall,
    ScaladslKinesisSubscriber.GroupId.default(info), info, system, serviceLocator, kr => deserializer.deserialize(kr.data))
}
