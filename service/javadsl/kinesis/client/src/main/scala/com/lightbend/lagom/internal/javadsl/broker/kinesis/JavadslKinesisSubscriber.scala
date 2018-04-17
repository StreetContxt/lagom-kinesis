/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.internal.javadsl.broker.kinesis

import java.net.URI
import java.util.Optional
import java.util.OptionalLong
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.{ActorSystem, SupervisorStrategy}
import akka.pattern.BackoffSupervisor
import akka.stream.Materializer
import akka.stream.javadsl.{Flow, Source}
import com.contxt.kinesis.KinesisRecord
import com.lightbend.lagom.internal.broker.kinesis._
import com.lightbend.lagom.javadsl.api.Descriptor.TopicCall
import com.lightbend.lagom.javadsl.api.broker.Message
import com.lightbend.lagom.javadsl.api.broker.MetadataKey
import com.lightbend.lagom.javadsl.api.broker.Subscriber
import com.lightbend.lagom.javadsl.api.{ServiceInfo, ServiceLocator}
import com.lightbend.lagom.javadsl.broker.kinesis.KinesisMetadataKeys
import org.slf4j.LoggerFactory

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * A Consumer for consuming messages from kinesis using the gfc-aws-kinesis API.
  */
private[lagom] class JavadslKinesisSubscriber[Payload, SubscriberPayload](kinesisConfig: KinesisConfig,
                                                       topicCall: TopicCall[Payload],
                                                       groupId: Subscriber.GroupId,
                                                       info: ServiceInfo,
                                                       system: ActorSystem,
                                                       serviceLocator: ServiceLocator,
                                                       transform: KinesisRecord => SubscriberPayload
                                                       )
                                                      (implicit mat: Materializer, ec: ExecutionContext)
  extends Subscriber[SubscriberPayload] {
  private val log = LoggerFactory.getLogger(classOf[JavadslKinesisSubscriber[_, _]])

  import JavadslKinesisSubscriber._

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
    else new JavadslKinesisSubscriber(kinesisConfig, topicCall, newGroupId, info, system, serviceLocator, transform)
  }

  override def withMetadata() = new JavadslKinesisSubscriber(
    kinesisConfig, topicCall, groupId, info, system, serviceLocator, wrapPayload
  )

  private def wrapPayload(record: KinesisRecord): Message[SubscriberPayload] = {
    Message.create(transform(record))
      .add(MetadataKey.messageKey[String], record.partitionKey)
      .add(KinesisMetadataKeys.APPROXIMATE_ARRIVAL_TIMESTAMP, record.approximateArrivalTimestamp)
      .add(KinesisMetadataKeys.ENCRYPTION_TYPE, record.encryptionType)
      .add(KinesisMetadataKeys.EXPLICIT_HASH_KEY, Optional.ofNullable(record.explicitHashKey.orNull))
      .add(KinesisMetadataKeys.SEQUENCE_NUMBER, record.sequenceNumber)
      .add(KinesisMetadataKeys.SUB_SEQUENCE_NUMBER, record.subSequenceNumber match { case Some(n) => OptionalLong.of(n); case _ => OptionalLong.empty })
  }

  override def atMostOnceSource: Source[SubscriberPayload, _] = {
    ???
  }

  private def locateService(name: String): Future[Option[URI]] =
    serviceLocator
      .locate(name)
      .toScala
      .map(_.asScala)

  override def atLeastOnce(flow: Flow[SubscriberPayload, Done, _]): CompletionStage[Done] = {
    val streamCompleted = Promise[Done]
    val consumerProps = KinesisSubscriberActor.props(
      kinesisConfig,
      consumerConfig,
      locateService,
      topicCall.topicId().value(),
      groupId.groupId(),
      flow.asScala,
      streamCompleted,
      transform)


    val backoffConsumerProps = BackoffSupervisor.propsWithSupervisorStrategy(
      consumerProps,
      s"KinesisConsumerActor$consumerId-${topicCall.topicId().value}",
      consumerConfig.minBackoff,
      consumerConfig.maxBackoff,
      consumerConfig.randomBackoffFactor,
      SupervisorStrategy.stoppingStrategy)

    system.actorOf(backoffConsumerProps, s"KinesisBackoffConsumer$consumerId-${topicCall.topicId().value}")

    streamCompleted.future.toJava
  }

}

private[lagom] object JavadslKinesisSubscriber {
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

    def default(info: ServiceInfo): GroupId = GroupId(info.serviceName())
  }

}
