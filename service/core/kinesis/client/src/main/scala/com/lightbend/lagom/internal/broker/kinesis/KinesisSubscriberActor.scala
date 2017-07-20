/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.internal.broker.kinesis

import java.net.URI

import akka.Done
import akka.actor.{Actor, ActorLogging, Props, Status}
import akka.pattern.pipe
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{KillSwitch, KillSwitches, Materializer}
import com.amazonaws.auth._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.gilt.gfc.aws.kinesis.akka.{KinesisNonBlockingStreamSource, KinesisStreamConsumerConfig}
import com.gilt.gfc.aws.kinesis.client.{KCLConfiguration, KinesisClientEndpoints, KinesisRecordReader}
import com.lightbend.lagom.internal.broker.kinesis.KinesisSubscriberActor._
import com.lightbend.lagom.internal.broker.kinesis.ServiceType._

import scala.concurrent.{ExecutionContext, Future, Promise}

private[lagom] class KinesisSubscriberActor[Message](kinesisConfig: KinesisConfig,
                                                     consumerConfig: ConsumerConfig,
                                                     locateService: String => Future[Option[URI]],
                                                     topicId: String,
                                                     groupId: String,
                                                     flow: Flow[Message, Done, _],
                                                     recordReader: KinesisRecordReader[Message],
                                                     streamCompleted: Promise[Done]
                                                    )(implicit mat: Materializer,
                                                      ec: ExecutionContext) extends Actor
  with ActorLogging {

  implicit val krr: KinesisRecordReader[Message] = recordReader

  /** Switch used to terminate the on-going Kinesis publishing stream when this actor fails. */
  private var shutdown: Option[KillSwitch] = None

  override def preStart(): Unit = {

    kinesisConfig.kinesisServiceName foreach { kinesisName =>
      log.debug("Looking up kinesis service with name [{}] from service locator for at least once source",
        kinesisName)
      locateService(kinesisName).map((ServiceType.kinesisService(kinesisName), _)) pipeTo self
    }

    kinesisConfig.dynamodbServiceName foreach { dynamoName =>
      log.debug("Looking up dynamo service with name [{}] from service locator for at least once source",
        dynamoName)
      locateService(dynamoName).map((ServiceType.dynamoService(dynamoName), _)) pipeTo self
    }

    if(kinesisConfig.kinesisServiceName.isDefined || kinesisConfig.dynamodbServiceName.isDefined) {
      context.become(locatingServices(
        kinesisDone = kinesisConfig.kinesisServiceName.isEmpty,
        dynamoDone = kinesisConfig.dynamodbServiceName.isEmpty))
    } else {
      run(None, None)
    }
  }

  private def locatingServices(kinesisDone: Boolean = false,
                               dynamoDone: Boolean = false,
                               kinesisEndpoint: Option[String] = None,
                               dynamoEndpoint: Option[String] = None): Receive = {
    case Status.Failure(e) =>
      log.error(s"Error locating Kinesis or Dynamo service for topic [{}]: [{}]", topicId, e)
      throw e

    case (KinesisService(name), None) =>
      log.error("Unable to locate Kinesis service named [{}]", name)
      context.stop(self)

    case (DynamoService(name), None) =>
      log.error("Unable to locate DynamoDb service named [{}]", name)
      context.stop(self)

    case (KinesisService(name), Some(uri: URI)) =>
      log.debug("Kinesis service [{}] located at URI [{}] for subscriber of [{}]", name, uri, topicId)

      if (dynamoDone) run(kinesisEndpoint, dynamoEndpoint)
      else context.become(locatingServices(
        kinesisDone = true,
        dynamoDone,
        kinesisEndpoint = Some(uri.toString),
        dynamoEndpoint
      ))

    case (DynamoService(name), Some(uri: URI)) =>
      log.debug("DynamoDB service [{}] located at URI [{}] for subscriber of [{}]", name, uri, topicId)

      if (kinesisDone) run(kinesisEndpoint, dynamoEndpoint)
      else context.become(locatingServices(
        kinesisDone = true,
        dynamoDone,
        kinesisEndpoint,
        dynamoEndpoint = Some(uri.toString)
      ))
  }

  private def run(kinesisEndpoint: Option[String], dynamoEndpoint: Option[String]) = {
    val (killSwitch, streamDone) =
      atLeastOnce(flow, kinesisEndpoint, dynamoEndpoint)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.both)
        .run()

    shutdown = Some(killSwitch)
    streamDone pipeTo self
    context.become(running)
  }


  override def postStop(): Unit = {
    shutdown.foreach(_.shutdown())
  }

  private def running: Receive = {
    case Status.Failure(e) =>
      log.error("Topic subscription interrupted due to failure", e)
      throw e

    case Done =>
      log.info("Kinesis subscriber stream for topic {} was completed.", topicId)
      streamCompleted.success(Done)
      context.stop(self)
  }

  override def receive: Receive = PartialFunction.empty

  private def atLeastOnce(flow: Flow[Message, Done, _],
                          kinesisEndpoint: Option[String],
                          dynamoEndpoint: Option[String]): Source[Done, _] = {
    val streamConfig = buildKinesisStreamConsumerConfig[Message](consumerConfig, topicId, groupId, kinesisConfig,
      kinesisEndpoint, dynamoEndpoint)
    val pairedCommittableSource = KinesisNonBlockingStreamSource(streamConfig)

    pairedCommittableSource.via(flow)
  }
}

object KinesisSubscriberActor {
  def buildKinesisStreamConsumerConfig[T](consumerConfig: ConsumerConfig,
                                          topicId: String,
                                          applicationName: String,
                                          kinesisConfig: KinesisConfig,
                                          kinesisEndpoint: Option[String],
                                          dynamoDbEndpoint: Option[String]): KinesisStreamConsumerConfig[T] = {
    val credentialsProvider =
      new AWSCredentialsProviderChain(
        new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(consumerConfig.awsAccessKey.orNull, consumerConfig.awsSecretKey.orNull)),
        new DefaultAWSCredentialsProviderChain()
      )

    val endpoints = for {
      k <- kinesisEndpoint.orElse(kinesisConfig.kinesisEndpoint)
      d <- dynamoDbEndpoint.orElse(kinesisConfig.dynamodbEndpoint)
    } yield {
      KinesisClientEndpoints(k, d)
    }

    KinesisStreamConsumerConfig(
      topicId,
      applicationName,
      credentialsProvider,
      credentialsProvider,
      credentialsProvider,
      initialPositionInStream = consumerConfig.initialPositionInStream,
      regionName = consumerConfig.regionName,
      kinesisClientEndpoints = endpoints
    )
  }

  def props[Message](kinesisConfig: KinesisConfig,
                     consumerConfig: ConsumerConfig,
                     locateService: String => Future[Option[URI]],
                     topicId: String,
                     groupId: String,
                     flow: Flow[Message, Done, _],
                     recordReader: KinesisRecordReader[Message],
                     streamCompleted: Promise[Done]
                    )(implicit mat: Materializer, ec: ExecutionContext) =
    Props(new KinesisSubscriberActor[Message](
      kinesisConfig,
      consumerConfig,
      locateService,
      topicId,
      groupId,
      flow,
      recordReader,
      streamCompleted))
}
