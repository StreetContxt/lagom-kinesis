/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.internal.broker.kinesis

import java.net.URI
import java.util.UUID

import akka.Done
import akka.actor.{Status, Actor, ActorLogging, Props}
import akka.pattern.pipe
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, GraphDSL, Source, Zip, Flow, Unzip}
import akka.util.ByteString
import com.amazonaws.auth._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{DataFetchingStrategy, InitialPositionInStream, KinesisClientLibConfiguration}
import com.contxt.kinesis.{KinesisRecord, KinesisSource}
import com.lightbend.lagom.internal.broker.kinesis.KinesisSubscriberActor._
import com.lightbend.lagom.internal.broker.kinesis.ServiceType._

import scala.concurrent.{Future, ExecutionContext, Promise}

private[lagom] class KinesisSubscriberActor[Message](
  kinesisConfig: KinesisConfig,
  consumerConfig: ConsumerConfig,
  locateService: String => Future[Option[URI]],
  topicId: String,
  groupId: String,
  flow: Flow[Message, Done, _],
  recordReader: ByteString => Message,
  streamCompleted: Promise[Done]
)(implicit
  mat: ActorMaterializer,
  ec: ExecutionContext) extends Actor
  with ActorLogging {

  /** Switch used to terminate the on-going Kinesis publishing stream when this actor fails. */
  private var shutdown: Option[KillSwitch] = None

  override def preStart(): Unit = {

    kinesisConfig.kinesisServiceName foreach { kinesisName =>
      log.debug(
        "Looking up kinesis service with name [{}] from service locator for at least once source",
        kinesisName
      )
      locateService(kinesisName).map((ServiceType.kinesisService(kinesisName), _)) pipeTo self
    }

    kinesisConfig.dynamodbServiceName foreach { dynamoName =>
      log.debug(
        "Looking up dynamo service with name [{}] from service locator for at least once source",
        dynamoName
      )
      locateService(dynamoName).map((ServiceType.dynamoService(dynamoName), _)) pipeTo self
    }

    if (kinesisConfig.kinesisServiceName.isDefined || kinesisConfig.dynamodbServiceName.isDefined) {
      context.become(locatingServices(
        kinesisDone = kinesisConfig.kinesisServiceName.isEmpty,
        dynamoDone = kinesisConfig.dynamodbServiceName.isEmpty
      ))
    } else {
      run(None, None)
    }
  }

  private def locatingServices(
    kinesisDone: Boolean = false,
    dynamoDone: Boolean = false,
    kinesisEndpoint: Option[String] = None,
    dynamoEndpoint: Option[String] = None
  ): Receive = {
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

  private def atLeastOnce(
    flow: Flow[Message, Done, _],
    kinesisEndpoint: Option[String],
    dynamoEndpoint: Option[String]
  ): Source[Done, _] = {
    val streamConfig = buildKinesisStreamConsumerConfig[Message](consumerConfig, topicId, groupId, kinesisConfig,
      kinesisEndpoint, dynamoEndpoint)
    val pairedCommittableSource = KinesisSource(streamConfig)(mat).map(kr => (kr, recordReader(kr.data)))

    val committOffsetFlow =
      Flow.fromGraph(GraphDSL.create(flow) { implicit builder => flow =>
        import GraphDSL.Implicits._
        val unzip = builder.add(Unzip[KinesisRecord, Message])
        val zip = builder.add(Zip[KinesisRecord, Done])
        val committer = {
          val commitFlow = Flow[(KinesisRecord, Done)].map {
            case (rec, _) =>
              rec.markProcessed()
              Done.getInstance()
          }
          builder.add(commitFlow)
        }
        // To allow the user flow to do its own batching, the offset side of the flow needs to effectively buffer
        // infinitely to give full control of backpressure to the user side of the flow.
        val offsetBuffer = Flow[KinesisRecord].buffer(consumerConfig.offsetBuffer, OverflowStrategy.backpressure)

        unzip.out0 ~> offsetBuffer ~> zip.in0
        unzip.out1 ~> flow ~> zip.in1
        zip.out ~> committer.in

        FlowShape(unzip.in, committer.out)
      })

    pairedCommittableSource.via(committOffsetFlow)
  }
}

object KinesisSubscriberActor {
  def buildKinesisStreamConsumerConfig[T](
    consumerConfig: ConsumerConfig,
    topicId: String,
    applicationName: String,
    kinesisConfig: KinesisConfig,
    kinesisEndpoint: Option[String],
    dynamoDbEndpoint: Option[String]
  ): KinesisClientLibConfiguration = {

    val credentialsProvider = buildCredentialsProviderChain(consumerConfig)
    val configuration = new KinesisClientLibConfiguration(
      applicationName,
      topicId,
      credentialsProvider,
      UUID.randomUUID().toString
    )
      .withCallProcessRecordsEvenForEmptyRecordList(true)
      .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
      .withDataFetchingStrategy(DataFetchingStrategy.DEFAULT.toString)
      .withMaxRecords(consumerConfig.batchSize)

    (for {
      withMetricsLevel <- applyIfDefined(configuration, consumerConfig.metricsLevel) { _.withMetricsLevel(_) }
      withRegion <- applyIfDefined(withMetricsLevel, consumerConfig.regionName) { _.withRegionName(_) }
      withKinesisEndpoint <- applyIfDefined(withRegion,kinesisEndpoint.orElse(kinesisConfig.kinesisEndpoint)) {
        _.withKinesisEndpoint(_)
      }
      withDynamoEndpoint <- applyIfDefined(withKinesisEndpoint,dynamoDbEndpoint.orElse(kinesisConfig.dynamodbEndpoint)) {
        _.withDynamoDBEndpoint(_)
      }
    } yield withDynamoEndpoint).getOrElse(configuration)
  }


  def applyIfDefined[T, V](t: T, maybeV: Option[V])(f: (T, V) => T): Option[T] =
    maybeV.map(v => f(t, v)).orElse(Some(t))


  def props[Message](
    kinesisConfig: KinesisConfig,
    consumerConfig: ConsumerConfig,
    locateService: String => Future[Option[URI]],
    topicId: String,
    groupId: String,
    flow: Flow[Message, Done, _],
    recordReader: ByteString => Message,
    streamCompleted: Promise[Done]
  )(implicit mat: ActorMaterializer, ec: ExecutionContext) =
    Props(new KinesisSubscriberActor[Message](
      kinesisConfig,
      consumerConfig,
      locateService,
      topicId,
      groupId,
      flow,
      recordReader,
      streamCompleted
    ))

  private def buildCredentialsProviderChain(consumerConfig: ConsumerConfig): AWSCredentialsProviderChain = {
    if (consumerConfig.awsAccessKey.isDefined && consumerConfig.awsSecretKey.isDefined) {
      new AWSCredentialsProviderChain(
        new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(consumerConfig.awsAccessKey.orNull, consumerConfig.awsSecretKey.orNull)),
        new DefaultAWSCredentialsProviderChain()
      )
    } else {
      new AWSCredentialsProviderChain(new DefaultAWSCredentialsProviderChain())
    }
  }
}
