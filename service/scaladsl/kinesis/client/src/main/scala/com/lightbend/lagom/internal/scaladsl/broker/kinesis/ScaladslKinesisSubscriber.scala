/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.internal.scaladsl.broker.kinesis

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.{ActorSystem, SupervisorStrategy}
import akka.pattern.BackoffSupervisor
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import com.contxt.kinesis.KinesisRecord
import com.contxt.kinesis.KinesisSource
import com.lightbend.lagom.internal.broker.kinesis._
import com.lightbend.lagom.scaladsl.api.Descriptor.TopicCall
import com.lightbend.lagom.scaladsl.api.broker.Message
import com.lightbend.lagom.scaladsl.api.broker.MetadataKey
import com.lightbend.lagom.scaladsl.api.broker.Subscriber
import com.lightbend.lagom.scaladsl.api.{ServiceInfo, ServiceLocator}
import com.lightbend.lagom.scaladsl.broker.kinesis.KinesisMetadataKeys
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

/**
 * A Consumer for consuming messages from kinesis using the gfc-aws-kinesis API.
 */
class ScaladslKinesisSubscriber[Payload, SubscriberPayload](
  kinesisConfig: KinesisConfig,
  topicCall: TopicCall[Payload],
  groupId: Subscriber.GroupId,
  info: ServiceInfo,
  system: ActorSystem,
  serviceLocator: ServiceLocator,
  transform: KinesisRecord => SubscriberPayload
)(implicit mat: Materializer, ec: ExecutionContext)
  extends Subscriber[SubscriberPayload] {
  private val log = LoggerFactory.getLogger(classOf[ScaladslKinesisSubscriber[_, _]])

  import ScaladslKinesisSubscriber._

  private lazy val consumerId = KinesisClientIdSequenceNumber.getAndIncrement

  private def consumerConfig = ConsumerConfig(system.settings.config)

  @throws(classOf[IllegalArgumentException])
  override def withGroupId(groupId: String): Subscriber[SubscriberPayload] = {
    val newGroupId = {
      if (groupId == null) {
        val defaultGroupId = GroupId.default(info)
        log.debug {
          "Passed a null groupId, but Kinesis requires clients to set one. " +
            s"Defaulting $this consumer groupId to $defaultGroupId."
        }
        defaultGroupId
      } else GroupId(groupId)
    }

    if (newGroupId == this.groupId) this
    else new ScaladslKinesisSubscriber(kinesisConfig, topicCall, newGroupId, info, system, serviceLocator, transform)
  }

  override def withMetadata = new ScaladslKinesisSubscriber[Payload, Message[SubscriberPayload]](
    kinesisConfig, topicCall, groupId, info, system, serviceLocator, wrapPayload
  )

  private def wrapPayload(record: KinesisRecord): Message[SubscriberPayload] = {
    Message(transform(record)) +
      (MetadataKey.MessageKey[String] -> record.partitionKey) +
      (KinesisMetadataKeys.ApproximateArrivalTimestamp -> record.approximateArrivalTimestamp) +
      (KinesisMetadataKeys.EncryptionType -> record.encryptionType) +
      (KinesisMetadataKeys.ExplicitHashKey -> record.explicitHashKey) +
      (KinesisMetadataKeys.SequenceNumber -> record.sequenceNumber) +
      (KinesisMetadataKeys.SubSequenceNumber -> record.subSequenceNumber)
  }

  override def atMostOnceSource: Source[SubscriberPayload, _] = {

    val eventualDynamoEndpoint = Future.sequence {
      kinesisConfig.dynamodbServiceName.map { dynamoName =>
        serviceLocator.locate(dynamoName)
      }.toList
    }.map(_.find(_.isDefined).flatten)

    val eventualKinesisEndpoint = Future.sequence(
      kinesisConfig.kinesisServiceName.map { kinesisName =>
      serviceLocator.locate(kinesisName)
    }.toList
    ).map(_.find(_.isDefined).flatten)

    val eventualSource: Future[Source[SubscriberPayload, Future[Done]]] = for {
      kinesisEndpoint <- eventualKinesisEndpoint
      dynamoEndpoint <- eventualDynamoEndpoint
      configuration = KinesisSubscriberActor.buildKinesisStreamConsumerConfig(
        consumerConfig = consumerConfig,
        topicId = topicCall.topicId.name,
        applicationName = groupId.groupId,
        kinesisConfig = kinesisConfig,
        kinesisEndpoint = kinesisEndpoint.map(_.toString),
        dynamoDbEndpoint = dynamoEndpoint.map(_.toString)
      )
    } yield {
      KinesisSource(configuration).map { kr =>
        kr.markProcessed()
        transform(kr)
      }
    }

    Await.result(eventualSource, Duration.Inf)
  }

  override def atLeastOnce(flow: Flow[SubscriberPayload, Done, _]): Future[Done] = {
    val streamCompleted = Promise[Done]
    val consumerProps = KinesisSubscriberActor.props(
      kinesisConfig,
      consumerConfig,
      serviceLocator.locate,
      topicCall.topicId.name,
      groupId.groupId,
      flow,
      streamCompleted,
      transform
    )

    val backoffConsumerProps = BackoffSupervisor.propsWithSupervisorStrategy(
      consumerProps,
      s"KinesisConsumerActor$consumerId-${topicCall.topicId.name}",
      consumerConfig.minBackoff,
      consumerConfig.maxBackoff,
      consumerConfig.randomBackoffFactor,
      SupervisorStrategy.stoppingStrategy
    )

    system.actorOf(backoffConsumerProps, s"KinesisBackoffConsumer$consumerId-${topicCall.topicId.name}")

    streamCompleted.future
  }

}

private[lagom] object ScaladslKinesisSubscriber {
  private val KinesisClientIdSequenceNumber = new AtomicInteger(1)

  case class GroupId(groupId: String) extends Subscriber.GroupId {
    if (GroupId.isInvalidGroupId(groupId))
      throw new IllegalArgumentException(s"Failed to create group because [groupId=$groupId] " +
        s"contains invalid character(s). Check the Kinesis spec for creating a valid group id.")
  }

  case object GroupId {
    private val InvalidGroupIdChars =
      Set('/', '\\', ',', '\u0000', ':', '"', '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '=')

    private def isInvalidGroupId(groupId: String): Boolean = groupId.exists(InvalidGroupIdChars.apply)

    def default(info: ServiceInfo): GroupId = GroupId(info.serviceName)
  }

}
