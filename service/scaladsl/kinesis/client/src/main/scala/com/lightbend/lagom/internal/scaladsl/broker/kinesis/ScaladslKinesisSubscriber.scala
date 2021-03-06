/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.internal.scaladsl.broker.kinesis

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.{ActorSystem, SupervisorStrategy}
import akka.pattern.BackoffSupervisor
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import com.contxt.kinesis.KinesisSource
import com.lightbend.lagom.internal.broker.kinesis._
import com.lightbend.lagom.scaladsl.api.Descriptor.TopicCall
import com.lightbend.lagom.scaladsl.api.broker.Subscriber
import com.lightbend.lagom.scaladsl.api.deser.MessageSerializer.NegotiatedDeserializer
import com.lightbend.lagom.scaladsl.api.{ServiceInfo, ServiceLocator}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

/**
 * A Consumer for consuming messages from kinesis using the gfc-aws-kinesis API.
 */
class ScaladslKinesisSubscriber[Message](
  kinesisConfig: KinesisConfig,
  topicCall: TopicCall[Message],
  groupId: Subscriber.GroupId,
  info: ServiceInfo,
  system: ActorSystem,
  serviceLocator: ServiceLocator
)(implicit mat: ActorMaterializer, ec: ExecutionContext)
  extends Subscriber[Message] {
  private val log = LoggerFactory.getLogger(classOf[ScaladslKinesisSubscriber[_]])

  import ScaladslKinesisSubscriber._

  private lazy val consumerId = KinesisClientIdSequenceNumber.getAndIncrement

  private def consumerConfig = ConsumerConfig(system.settings.config)

  @throws(classOf[IllegalArgumentException])
  override def withGroupId(groupId: String): Subscriber[Message] = {
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
    else new ScaladslKinesisSubscriber(kinesisConfig, topicCall, newGroupId, info, system, serviceLocator)
  }

  private val deserializer: NegotiatedDeserializer[Message, ByteString] = {
    val messageSerializer = topicCall.messageSerializer
    val protocol = messageSerializer.serializerForRequest.protocol
    messageSerializer.deserializer(protocol)
  }

  override def atMostOnceSource: Source[Message, _] = {

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

    val eventualSource: Future[Source[Message, Future[Done]]] = for {
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
      KinesisSource(configuration)(mat).map { kr =>
        kr.markProcessed()
        deserializer.deserialize(kr.data)
      }
    }

    Await.result(eventualSource, Duration.Inf)
  }

  def committableSource: Source[(Message, () => Unit), _] = {
    val eventualDynamoEndpoint = Future.sequence(
      kinesisConfig.dynamodbServiceName.map { dynamoName =>
      serviceLocator.locate(dynamoName)
    }.toList
    ).map(_.find(_.isDefined).flatten)

    val eventualKinesisEndpoint = Future.sequence(
      kinesisConfig.kinesisServiceName.map { kinesisName =>
      serviceLocator.locate(kinesisName)
    }.toList
    ).map(_.find(_.isDefined).flatten)

    val eventualSource: Future[Source[(Message, () => Unit), Future[Done]]] = for {
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
      KinesisSource(configuration)(mat).map { kr =>
        (deserializer.deserialize(kr.data), kr.markProcessed _)
      }
    }

    Await.result(eventualSource, Duration.Inf)
  }

  override def atLeastOnce(flow: Flow[Message, Done, _]): Future[Done] = {
    val streamCompleted = Promise[Done]
    val consumerProps = KinesisSubscriberActor.props(
      kinesisConfig,
      consumerConfig,
      serviceLocator.locate,
      topicCall.topicId.name,
      groupId.groupId,
      flow,
      deserializer.deserialize,
      streamCompleted
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
