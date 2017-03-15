/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.scaladsl.broker.kinesis

import com.lightbend.lagom.internal.scaladsl.broker.kinesis.ScaladslRegisterTopicProducers
import com.lightbend.lagom.scaladsl.api.ServiceLocator
import com.lightbend.lagom.scaladsl.server.LagomServer
import com.lightbend.lagom.spi.persistence.OffsetStore

/**
 * Components for including Kinesis into a Lagom application.
 *
 * Extending this trait will automatically start all topic producers.
 */
trait LagomKinesisComponents extends LagomKinesisClientComponents {
  def lagomServer: LagomServer
  def offsetStore: OffsetStore
  def serviceLocator: ServiceLocator

  override def topicPublisherName: Option[String] = super.topicPublisherName match {
    case Some(other) =>
      sys.error(s"Cannot provide the Kinesis topic factory as the default " +
        s"topic publisher since a default topic publisher " +
        s"has already been mixed into this cake: $other")
    case None => Some("kinesis")
  }

  // Eagerly start topic producers
  new ScaladslRegisterTopicProducers(lagomServer, topicFactory, serviceInfo, actorSystem,
    offsetStore, serviceLocator)(executionContext, materializer)
}
