/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.scaladsl.broker.kinesis

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.lightbend.lagom.internal.scaladsl.api.broker.{ TopicFactory, TopicFactoryProvider }
import com.lightbend.lagom.internal.scaladsl.broker.kinesis.KinesisTopicFactory
import com.lightbend.lagom.scaladsl.api.{ ServiceInfo, ServiceLocator }

import scala.concurrent.ExecutionContext

trait LagomKinesisClientComponents extends TopicFactoryProvider {
  def serviceInfo: ServiceInfo
  def actorSystem: ActorSystem
  def materializer: Materializer
  def executionContext: ExecutionContext
  def serviceLocator: ServiceLocator

  lazy val topicFactory: TopicFactory = new KinesisTopicFactory(serviceInfo, actorSystem, serviceLocator)(materializer, executionContext)
  override def optionalTopicFactory: Option[TopicFactory] = Some(topicFactory)
}
