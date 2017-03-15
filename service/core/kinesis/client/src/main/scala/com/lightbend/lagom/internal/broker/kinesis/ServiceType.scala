package com.lightbend.lagom.internal.broker.kinesis

private[kinesis] sealed trait ServiceType

private[kinesis] object ServiceType {
  def kinesisService(name: String): ServiceType = KinesisService(name)

  def dynamoService(name: String): ServiceType = DynamoService(name)

  case class KinesisService(name: String) extends ServiceType

  case class DynamoService(name: String) extends ServiceType
}
