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

import java.io.IOException;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.api.StreamConfiguration;
import org.apache.camel.CamelContext;
import org.apache.camel.component.nats.NatsComponent;
import org.apache.camel.test.infra.nats.services.NatsLocalContainerJetStreamService;
import org.apache.camel.test.infra.nats.services.NatsLocalContainerService;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.junit.jupiter.api.extension.RegisterExtension;

class NatsJetStreamITSupport extends CamelTestSupport {

    @RegisterExtension
    static NatsLocalContainerService SERVICE = new NatsLocalContainerJetStreamService();

    protected static final String TEST_STREAM_NAME = "test-stream";

    private final String[] subjects;

    NatsJetStreamITSupport(String... subjects) {
        super();
        this.subjects = subjects;
    }

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();
        NatsComponent nats = context.getComponent("nats", NatsComponent.class);
        nats.setServers(SERVICE.getServiceAddress());

        createTestStream();

        return context;
    }

    private void createTestStream() throws IOException, InterruptedException, JetStreamApiException {
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .name(TEST_STREAM_NAME)
                .subjects(subjects)
                .build();

        try (Connection connection = Nats.connect(SERVICE.getServiceAddress())) {
            connection.jetStreamManagement().addStream(streamConfiguration);
        }
    }
}
