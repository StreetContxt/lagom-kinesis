/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.internal.scaladsl.broker.kinesis

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.lightbend.lagom.internal.broker.kinesis.KinesisConfig
import com.lightbend.lagom.internal.scaladsl.api.broker.TopicFactory
import com.lightbend.lagom.scaladsl.api.Descriptor.TopicCall
import com.lightbend.lagom.scaladsl.api.broker.Topic
import com.lightbend.lagom.scaladsl.api.{ServiceInfo, ServiceLocator}

import scala.concurrent.ExecutionContext

/**
 * Factory for creating topics instances.
 */
private[lagom] class KinesisTopicFactory(serviceInfo: ServiceInfo, system: ActorSystem,
  serviceLocator: ServiceLocator)(implicit
  materializer: Materializer,
  executionContext: ExecutionContext) extends TopicFactory {

  private val config = KinesisConfig(system.settings.config)

  def create[Message](topicCall: TopicCall[Message]): Topic[Message] = {
    new ScaladslKinesisTopic(config, topicCall, serviceInfo, system, serviceLocator)
  }
}
