/*
 * Copyright (C) 2017 Quantify Labs Inc. <https://github.com/streetcontxt>
 */
package com.lightbend.lagom.javadsl.broker.kinesis;

import com.google.inject.AbstractModule;
import com.lightbend.lagom.internal.javadsl.api.broker.TopicFactory;

public class KinesisClientModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(TopicFactory.class).to(KinesisTopicFactory.class);
    }
}
