/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.nats.integration;

import io.nats.client.PushSubscribeOptions;
import org.apache.camel.EndpointInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.component.nats.NatsConstants;
import org.apache.camel.spi.Registry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

@DisabledIfSystemProperty(named = "ci.env.name", matches = "github.com", disabledReason = "Flaky on GitHub Actions")
class NatsJetStreamPushConsumerIT extends NatsJetStreamITSupport {

    private static final String BODY = "NATS JetStream Push - Hello, World!", OPTIONS_TYPE
            = "pushSubscribeOptions", OPTIONS_NAME = "testOptions", SUBJECT_NAME = "push.test";

    NatsJetStreamPushConsumerIT() {
        super(SUBJECT_NAME);
    }

    @EndpointInject("mock:pushResult")
    protected MockEndpoint mockResultEndpoint;

    @Test
    public void testConsumer() throws Exception {
        mockResultEndpoint.expectedBodiesReceived(BODY);
        mockResultEndpoint.expectedHeaderReceived(NatsConstants.NATS_SUBJECT, SUBJECT_NAME);

        template.sendBody("nats:" + SUBJECT_NAME + "?flushConnection=true&jetStream=true", BODY);

        mockResultEndpoint.assertIsSatisfied();
    }

    @Override
    protected void bindToRegistry(Registry registry) {
        final PushSubscribeOptions.Builder options = PushSubscribeOptions.builder()
                .stream(NatsJetStreamITSupport.TEST_STREAM_NAME);

        registry.bind(OPTIONS_NAME, options);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("nats:" + SUBJECT_NAME + "?flushConnection=true&" + OPTIONS_TYPE + "=#bean:" + OPTIONS_NAME)
                        .to(mockResultEndpoint);
            }
        };
    }
}
