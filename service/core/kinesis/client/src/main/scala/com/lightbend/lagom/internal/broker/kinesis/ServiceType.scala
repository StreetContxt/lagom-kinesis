package com.lightbend.lagom.internal.broker.kinesis

private[lagom] sealed trait ServiceType

private[lagom] object ServiceType {
  def kinesisService(name: String): ServiceType = KinesisService(name)

  def dynamoService(name: String): ServiceType = DynamoService(name)

  case class KinesisService(name: String) extends ServiceType

  case class DynamoService(name: String) extends ServiceType
}
