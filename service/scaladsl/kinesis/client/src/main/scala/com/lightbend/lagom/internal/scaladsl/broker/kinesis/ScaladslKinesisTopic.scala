/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.internal.scaladsl.broker.kinesis

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.lightbend.lagom.internal.broker.kinesis.KinesisConfig
import com.lightbend.lagom.scaladsl.api.Descriptor.TopicCall
import com.lightbend.lagom.scaladsl.api.broker.Topic.TopicId
import com.lightbend.lagom.scaladsl.api.broker.{Subscriber, Topic}
import com.lightbend.lagom.scaladsl.api.{ServiceInfo, ServiceLocator}

import scala.concurrent.ExecutionContext

private[lagom] class ScaladslKinesisTopic[Message](kinesisConfig: KinesisConfig,
                                                   topicCall: TopicCall[Message],
                                                   info: ServiceInfo,
                                                   system: ActorSystem,
                                                   serviceLocator: ServiceLocator)
                                                  (implicit mat: ActorMaterializer,
                                                   ec: ExecutionContext) extends Topic[Message] {

  override def topicId: TopicId = topicCall.topicId

  override def subscribe: Subscriber[Message] = new ScaladslKinesisSubscriber(kinesisConfig, topicCall,
    ScaladslKinesisSubscriber.GroupId.default(info), info, system, serviceLocator)
}
