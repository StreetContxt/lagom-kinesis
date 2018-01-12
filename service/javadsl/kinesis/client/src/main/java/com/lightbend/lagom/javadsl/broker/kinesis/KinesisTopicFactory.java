/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.javadsl.broker.kinesis;

import javax.inject.Inject;

import com.lightbend.lagom.internal.broker.kinesis.KinesisConfig;
import com.lightbend.lagom.internal.broker.kinesis.KinesisConfig$;
import com.lightbend.lagom.internal.javadsl.api.broker.TopicFactory;
import com.lightbend.lagom.internal.javadsl.broker.kinesis.JavadslKinesisTopic;
import com.lightbend.lagom.javadsl.api.Descriptor;
import com.lightbend.lagom.javadsl.api.ServiceInfo;
import com.lightbend.lagom.javadsl.api.ServiceLocator;
import com.lightbend.lagom.javadsl.api.broker.Topic;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import scala.concurrent.ExecutionContext;

/**
 * Factory for creating topics instances.
 */
public class KinesisTopicFactory implements TopicFactory {
    private final ServiceInfo serviceInfo;
    private final ActorSystem system;
    private final ActorMaterializer materializer;
    private final ExecutionContext executionContext;
    private final KinesisConfig config;
    private final ServiceLocator serviceLocator;

    @Inject
    public KinesisTopicFactory(ServiceInfo serviceInfo, ActorSystem system, ActorMaterializer materializer,
            ExecutionContext executionContext, ServiceLocator serviceLocator) {
        this.serviceInfo = serviceInfo;
        this.system = system;
        this.materializer = materializer;
        this.executionContext = executionContext;
        this.config = KinesisConfig$.MODULE$.apply(system.settings().config());
        this.serviceLocator = serviceLocator;
    }

    @Override
    public <Message> Topic<Message> create(Descriptor.TopicCall<Message> topicCall) {
        return new JavadslKinesisTopic<>(config, topicCall, serviceInfo, system, serviceLocator, materializer, executionContext);
    }
}
