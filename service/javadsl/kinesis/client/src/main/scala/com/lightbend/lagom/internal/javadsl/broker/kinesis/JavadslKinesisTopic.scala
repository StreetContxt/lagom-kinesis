/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.internal.javadsl.broker.kinesis

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.lightbend.lagom.internal.broker.kinesis.KinesisConfig
import com.lightbend.lagom.javadsl.api.Descriptor.TopicCall
import com.lightbend.lagom.javadsl.api.broker.Topic.TopicId
import com.lightbend.lagom.javadsl.api.broker.{Subscriber, Topic}
import com.lightbend.lagom.javadsl.api.{ServiceInfo, ServiceLocator}

import scala.concurrent.ExecutionContext

/**
  * Represents a Kinesis topic and allows publishing/consuming messages to/from the topic.
  */
private[lagom] class JavadslKinesisTopic[Message](kinesisConfig: KinesisConfig,
                                                  topicCall: TopicCall[Message],
                                                  info: ServiceInfo,
                                                  system: ActorSystem,
                                                  serviceLocator: ServiceLocator)
                                                 (implicit mat: Materializer, ec: ExecutionContext)
  extends Topic[Message] {

  override def topicId: TopicId = topicCall.topicId

  override def subscribe(): Subscriber[Message] = new JavadslKinesisSubscriber(kinesisConfig, topicCall,
    JavadslKinesisSubscriber.GroupId.default(info), info, system, serviceLocator)
}
