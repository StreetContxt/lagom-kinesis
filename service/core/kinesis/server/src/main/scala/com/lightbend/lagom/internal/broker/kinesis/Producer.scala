/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.internal.broker.kinesis

import java.net.URI

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, SupervisorStrategy}
import akka.cluster.sharding.ClusterShardingSettings
import akka.pattern.{BackoffSupervisor, pipe}
import akka.persistence.query.Offset
import akka.stream._
import akka.stream.scaladsl._
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.gilt.gfc.aws.kinesis.client.{KinesisPublisher, KinesisRecordWriter}
import com.lightbend.lagom.internal.broker.kinesis.ServiceType.{DynamoService, KinesisService}
import com.lightbend.lagom.internal.persistence.cluster.ClusterDistribution.EnsureActive
import com.lightbend.lagom.internal.persistence.cluster.{ClusterDistribution, ClusterDistributionSettings}
import com.lightbend.lagom.spi.persistence.{OffsetDao, OffsetStore}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

/**
  * A Producer for publishing messages in Kinesis using the akka-stream-kinesis API.
  */
private[lagom] object Producer {

  def startTaggedOffsetProducer[Message](
                                          system: ActorSystem,
                                          tags: immutable.Seq[String],
                                          kinesisConfig: KinesisConfig,
                                          locateService: String => Future[Option[URI]],
                                          topicId: String,
                                          eventStreamFactory: (String, Offset) => Source[(Message, Offset), _],
                                          serializer: KinesisRecordWriter[Message],
                                          offsetStore: OffsetStore
                                        )(implicit mat: Materializer, ec: ExecutionContext): Unit = {

    val producerConfig: ProducerConfig = ProducerConfig(system.settings.config)
    val publisherProps = TaggedOffsetProducerActor.props(kinesisConfig, producerConfig, locateService, topicId,
      eventStreamFactory, serializer, offsetStore)

    val backoffPublisherProps = BackoffSupervisor.propsWithSupervisorStrategy(
      publisherProps, s"producer", producerConfig.minBackoff, producerConfig.maxBackoff,
      producerConfig.randomBackoffFactor, SupervisorStrategy.stoppingStrategy
    )
    val clusterShardingSettings = ClusterShardingSettings(system).withRole(producerConfig.role)

    ClusterDistribution(system).start(
      s"kinesisProducer-$topicId",
      backoffPublisherProps,
      tags.toSet,
      ClusterDistributionSettings(system).copy(clusterShardingSettings = clusterShardingSettings)
    )
  }

  private class TaggedOffsetProducerActor[Message](
                                                    kinesisConfig: KinesisConfig,
                                                    producerConfig: ProducerConfig,
                                                    locateService: String => Future[Option[URI]],
                                                    topicId: String,
                                                    eventStreamFactory: (String, Offset) => Source[(Message, Offset), _],
                                                    serializer: KinesisRecordWriter[Message],
                                                    offsetStore: OffsetStore
                                                  )(implicit mat: Materializer, ec: ExecutionContext) extends Actor with ActorLogging {

    /** Switch used to terminate the on-going Kinesis publishing stream when this actor fails. */
    private var shutdown: Option[KillSwitch] = None

    override def postStop(): Unit = {
      shutdown.foreach(_.shutdown())
    }

    override def receive: Receive = {
      case EnsureActive(tag) =>
        offsetStore.prepare(s"topicProducer-$topicId", tag) pipeTo self
        // Left means no service lookup was attempted, Right means a service lookup was done - this allows us to
        // distinguish between no service lookup, and a not found service lookup.


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

        context.become(locatingServices(tag,
          kinesisDone = kinesisConfig.kinesisServiceName.isEmpty,
          dynamoDone = kinesisConfig.dynamodbServiceName.isEmpty))
    }

    def generalHandler: Receive = {
      case Failure(e) =>
        throw e

      case EnsureActive(_) =>
    }


    private def locatingServices(tag: String,
                                 kinesisDone: Boolean = false,
                                 dynamoDone: Boolean = false,
                                 kinesisEndpoint: Option[String] = None,
                                 dynamoEndpoint: Option[String] = None,
                                 offsetDao: Option[OffsetDao] = None): Receive = {

      case (KinesisService(name), None) =>
        log.error("Unable to locate Kinesis service named [{}]", name)
        context.stop(self)

      case (DynamoService(name), None) =>
        log.error("Unable to locate DynamoDb service named [{}]", name)
        context.stop(self)

      case (KinesisService(name), Some(uri: URI)) =>
        log.debug("Kinesis service [{}] located at URI [{}] for subscriber of [{}]", name, uri, topicId)

        if (dynamoDone && kinesisDone && offsetDao.isDefined) {
          offsetDao foreach { od => run(tag, kinesisEndpoint, dynamoEndpoint, od) }
        } else {
          context.become(locatingServices(
            tag,
            kinesisDone = true,
            dynamoDone,
            kinesisEndpoint = Some(uri.toString),
            dynamoEndpoint,
            offsetDao
          ))
        }

      case (DynamoService(name), Some(uri: URI)) =>
        log.debug("DynamoDB service [{}] located at URI [{}] for subscriber of [{}]", name, uri, topicId)

        if (dynamoDone && kinesisDone && offsetDao.isDefined) {
          offsetDao foreach { od => run(tag, kinesisEndpoint, dynamoEndpoint, od) }
        } else {
          context.become(locatingServices(
            tag,
            kinesisDone = true,
            dynamoDone,
            kinesisEndpoint,
            dynamoEndpoint = Some(uri.toString),
            offsetDao
          ))
        }

      case od: OffsetDao =>
        log.debug("OffsetDao prepared for subscriber of [{}]", topicId)

        if (dynamoDone && kinesisDone) {
          run(tag, kinesisEndpoint, dynamoEndpoint, od)
        } else {
          context.become(locatingServices(
            tag,
            kinesisDone = true,
            dynamoDone,
            kinesisEndpoint,
            dynamoEndpoint,
            Some(od)
          ))
        }

    }

    private def active: Receive = generalHandler.orElse {
      case Done =>
        log.info("Kinesis producer stream for topic {} was completed.", topicId)
        context.stop(self)
    }

    private def run(tag: String, kinesisUri: Option[String], dynamoUri: Option[String], dao: OffsetDao) = {
      val readSideSource = eventStreamFactory(tag, dao.loadedOffset)

      val (killSwitch, streamDone) = readSideSource
        .viaMat(KillSwitches.single)(Keep.right)
        .via(eventsPublisherFlow(kinesisUri, dao))
        .toMat(Sink.ignore)(Keep.both)
        .run()

      shutdown = Some(killSwitch)
      streamDone pipeTo self
      context.become(active)
    }

    private def eventsPublisherFlow(serviceLocatorUri: Option[String], offsetDao: OffsetDao) =
      Flow.fromGraph(GraphDSL.create(kinesisFlowPublisher(serviceLocatorUri)) { implicit builder =>
        publishFlow =>
          import GraphDSL.Implicits._
          val unzip = builder.add(Unzip[Message, Offset])
          val zip = builder.add(Zip[Any, Offset])
          val offsetCommitter = builder.add(Flow.fromFunction { e: (Any, Offset) =>
            offsetDao.saveOffset(e._2)
          })

          unzip.out0 ~> publishFlow ~> zip.in0
          unzip.out1 ~> zip.in1
          zip.out ~> offsetCommitter.in
          FlowShape(unzip.in, offsetCommitter.out)
      })

    private def kinesisFlowPublisher(serviceLocatorUri: Option[String]): Flow[Message, _, _] = {
      val builder = AmazonKinesisClientBuilder.standard()

      if (kinesisConfig.kinesisEndpoint.isDefined ^ producerConfig.regionName.isDefined)
        throw new IllegalStateException("kinesis endpoint and region name must either both be defined or both be blank")

      serviceLocatorUri.orElse(kinesisConfig.kinesisEndpoint).zip(producerConfig.regionName)
        .foreach{ case (kinesisEndpoint, regionName) =>
          builder.withEndpointConfiguration(new EndpointConfiguration(kinesisEndpoint, regionName))
        }

      if (producerConfig.awsAccessKey.isDefined ^ producerConfig.awsSecretKey.isDefined)
        throw new IllegalStateException("AWS access key and secret key must either both be defined or both be blank")

      producerConfig.awsAccessKey.zip(producerConfig.awsSecretKey)
        .foreach{ case (awsAccessKey, awsSecretKey) =>
          builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey, awsSecretKey)))
        }


      val publisher = KinesisPublisher(kinesisClient = Some(builder.build()))

      Flow
        .fromFunction[Message, Message](identity)
        .mapAsync(1)(msg => publisher.publishBatch(topicId, List(msg))(serializer))
    }

  }

  private object TaggedOffsetProducerActor {
    def props[Message](
                        kinesisConfig: KinesisConfig,
                        producerConfig: ProducerConfig,
                        locateService: String => Future[Option[URI]],
                        topicId: String,
                        eventStreamFactory: (String, Offset) => Source[(Message, Offset), _],
                        serializer: KinesisRecordWriter[Message],
                        offsetStore: OffsetStore
                      )(implicit mat: Materializer, ec: ExecutionContext) =
      Props(new TaggedOffsetProducerActor[Message](kinesisConfig, producerConfig, locateService,
        topicId, eventStreamFactory,
        serializer, offsetStore))
  }

}
