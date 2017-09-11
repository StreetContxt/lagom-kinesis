/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.internal.broker.kinesis

import java.util.concurrent.TimeUnit

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.typesafe.config.{Config, ConfigException}

import scala.concurrent.duration.{FiniteDuration, _}

sealed trait KinesisConfig {
  /** The name of the Kinesis server to look up out of the service locator. */
  def kinesisServiceName: Option[String]

  /** The name of the DynamoDB server to look up out of the service locator. */
  def dynamodbServiceName: Option[String]

  /** A Url for the Kinesis endpoint. Will be ignored if serviceName is defined. */
  def kinesisEndpoint: Option[String]

  /** A Url for the Kinesis endpoint. Will be ignored if serviceName is defined. */
  def dynamodbEndpoint: Option[String]
}

object KinesisConfig {
  def apply(conf: Config): KinesisConfig =
    new KinesisConfigImpl(conf.getConfig("lagom.broker.kinesis"))

  private final class KinesisConfigImpl(conf: Config) extends KinesisConfig {
    override val kinesisEndpoint: Option[String] = Some(conf.getString("kinesis-endpoint")).filter(_.nonEmpty)
    override val kinesisServiceName: Option[String] = Some(conf.getString("kinesis-service-name")).filter(_.nonEmpty)
    override val dynamodbEndpoint: Option[String] = Some(conf.getString("dynamodb-endpoint")).filter(_.nonEmpty)
    override val dynamodbServiceName: Option[String] = Some(conf.getString("dynamodb-service-name")).filter(_.nonEmpty)
    if((kinesisEndpoint.isDefined || kinesisServiceName.isDefined) ^
      (dynamodbEndpoint.isDefined || dynamodbServiceName.isDefined )) {
      throw new ConfigException.Generic("kinesis and dynamo connection settings must either both be set or unset")
    }
  }

}

sealed trait ClientConfig {
  def minBackoff: FiniteDuration

  def maxBackoff: FiniteDuration

  def randomBackoffFactor: Double

  def awsAccessKey: Option[String]

  def awsSecretKey: Option[String]

  def regionName: Option[String]
}

object ClientConfig {

  private[kinesis] class ClientConfigImpl(conf: Config) extends ClientConfig {
    override val minBackoff: FiniteDuration = conf.getDuration("failure-exponential-backoff.min", TimeUnit.MILLISECONDS).millis
    override val maxBackoff: FiniteDuration = conf.getDuration("failure-exponential-backoff.max", TimeUnit.MILLISECONDS).millis
    override val randomBackoffFactor: Double = conf.getDouble("failure-exponential-backoff.random-factor")
    override val awsAccessKey: Option[String] = Some(conf.getString("aws-access-key")).filter(_.nonEmpty)
    override val awsSecretKey: Option[String] = Some(conf.getString("aws-secret-key")).filter(_.nonEmpty)
    override val regionName: Option[String] = Some(conf.getString("region-name")).filter(_.nonEmpty)
  }

}

sealed trait ProducerConfig extends ClientConfig {
  def role: Option[String]
}

object ProducerConfig {
  def apply(conf: Config): ProducerConfig =
    new ProducerConfigImpl(conf.getConfig("lagom.broker.kinesis.client.producer"))

  private class ProducerConfigImpl(conf: Config)
    extends ClientConfig.ClientConfigImpl(conf) with ProducerConfig {
    val role: Option[String] = conf.getString("role") match {
      case "" => None
      case other => Some(other)
    }
  }

}

sealed trait ConsumerConfig extends ClientConfig {
  def applicationName: String

  def initialPositionInStream: InitialPositionInStream
}

object ConsumerConfig {
  def apply(conf: Config): ConsumerConfig =
    new ConsumerConfigImpl(conf.getConfig("lagom.broker.kinesis.client.consumer"))

  private final class ConsumerConfigImpl(conf: Config)
    extends ClientConfig.ClientConfigImpl(conf)
      with ConsumerConfig {

    def applicationName: String = conf.getString("application-name")

    def initialPositionInStream: InitialPositionInStream =
      InitialPositionInStream.valueOf(conf.getString("initial-position-in-stream"))
  }

}

