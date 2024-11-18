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
package org.apache.camel.component.nats;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import io.nats.client.Connection;
import io.nats.client.Connection.Status;
import io.nats.client.Dispatcher;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.spi.HeaderFilterStrategy;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.util.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NatsConsumer extends DefaultConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(NatsConsumer.class);

    private final AtomicBoolean active = new AtomicBoolean();
    private final Processor processor;
    private ExecutorService executor;
    private Connection connection;
    private Dispatcher dispatcher;

    public NatsConsumer(NatsEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.processor = processor;
    }

    @Override
    public NatsEndpoint getEndpoint() {
        return (NatsEndpoint) super.getEndpoint();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        LOG.debug("Starting Nats Consumer");
        this.executor = this.getEndpoint().createExecutor();

        LOG.debug("Getting Nats Connection");
        this.connection = this.getEndpoint().getConfiguration().getConnection() != null
                ? this.getEndpoint().getConfiguration().getConnection()
                : this.getEndpoint().getConnection();

        this.executor.submit(new NatsConsumingTask(this.connection, this.getEndpoint().getConfiguration()));
    }

    @Override
    protected void doStop() throws Exception {
        final NatsConfiguration configuration = this.getEndpoint().getConfiguration();

        if (configuration.isFlushConnection() && ObjectHelper.isNotEmpty(this.connection)) {
            LOG.debug("Flushing Messages before stopping");
            this.connection.flush(Duration.ofMillis(configuration.getFlushTimeout()));
        }

        if (ObjectHelper.isNotEmpty(this.dispatcher)) {
            try {
                this.dispatcher.unsubscribe(configuration.getTopic());
            } catch (final Exception e) {
                this.getExceptionHandler().handleException("Error during unsubscribing", e);
            }
        }

        LOG.debug("Stopping Nats Consumer");
        if (this.executor != null) {
            if (this.getEndpoint() != null && this.getEndpoint().getCamelContext() != null) {
                this.getEndpoint().getCamelContext().getExecutorServiceManager().shutdownNow(this.executor);
            } else {
                this.executor.shutdownNow();
            }
        }
        this.executor = null;

        if (ObjectHelper.isEmpty(configuration.getConnection()) && ObjectHelper.isNotEmpty(this.connection)) {
            LOG.debug("Closing Nats Connection");
            if (!this.connection.getStatus().equals(Status.CLOSED)) {
                this.connection.close();
            }
        }
        super.doStop();
    }

    public boolean isActive() {
        return this.active.get();
    }

    public void setActive(boolean active) {
        this.active.set(active);
    }

    class NatsConsumingTask implements Runnable {

        private final Connection connection;
        private final NatsConfiguration configuration;

        NatsConsumingTask(Connection connection, NatsConfiguration configuration) {
            this.connection = connection;
            this.configuration = configuration;
        }

        @Override
        public void run() {
            try {
                if (configuration.isJetStreamConsumer()) {
                    runJetStream();
                } else {
                    runNonJetStream();
                }
            } catch (final Exception e) {
                NatsConsumer.this.getExceptionHandler().handleException("Error during processing", e);
            }
        }

        private void runNonJetStream() {
            dispatcher = connection.createDispatcher(new CamelNatsMessageHandler());

            if (ObjectHelper.isNotEmpty(configuration.getQueueName())) {
                dispatcher = dispatcher.subscribe(configuration.getTopic(), configuration.getQueueName());
            } else {
                dispatcher = dispatcher.subscribe(configuration.getTopic());
            }

            final String maxMessages = configuration.getMaxMessages();
            if (ObjectHelper.isNotEmpty(maxMessages)) {
                dispatcher.unsubscribe(configuration.getTopic(), Integer.parseInt(maxMessages));
            }

            if (dispatcher.isActive()) {
                setActive(true);
            }
        }

        private void runJetStream() throws IOException, JetStreamApiException {
            final MessageHandler messageHandler = new CamelNatsMessageHandler();
            dispatcher = connection.createDispatcher(messageHandler);

            final JetStreamSubscription subscription;
            if (configuration.isPullSubscribe()) {
                if (configuration.isPushSubscribe()) {
                    throw new IllegalArgumentException("Cannot configure both a Pull- and Push-Subscribe JetStream consumer");
                }

                final ConsumerConfiguration.Builder consumerConfigurationBuilder = configuration.getConsumerConfiguration();
                final ConsumerConfiguration consumerConfiguration
                        = consumerConfigurationBuilder == null ? null : consumerConfigurationBuilder.build();

                final PullSubscribeOptions options = configuration.getPullSubscribeOptions()
                        .configuration(consumerConfiguration)
                        .build();

                subscription = connection.jetStream().subscribe(
                        configuration.getTopic(),
                        dispatcher,
                        messageHandler,
                        options);

                // submit a task to pull the messages:
                executor.submit(new PullSubscribeFetcher(configuration, subscription));
            } else {
                final ConsumerConfiguration.Builder consumerConfigurationBuilder = configuration.getConsumerConfiguration();
                final ConsumerConfiguration consumerConfiguration
                        = consumerConfigurationBuilder == null ? null : consumerConfigurationBuilder.build();

                final PushSubscribeOptions options = configuration.getPushSubscribeOptions()
                        .configuration(consumerConfiguration)
                        .build();

                subscription = connection.jetStream().subscribe(
                        configuration.getTopic(),
                        configuration.getQueueName(),
                        dispatcher,
                        messageHandler,
                        configuration.isAutoAck(),
                        options);
            }

            if (subscription.isActive()) {
                setActive(true);
            }
        }

        class CamelNatsMessageHandler implements MessageHandler {

            @Override
            public void onMessage(Message msg) {
                LOG.debug("Received Message: {}", msg);
                final Exchange exchange = NatsConsumer.this.createExchange(false);
                try {
                    exchange.getIn().setBody(msg.getData());
                    exchange.getIn().setHeader(NatsConstants.NATS_REPLY_TO, msg.getReplyTo());
                    exchange.getIn().setHeader(NatsConstants.NATS_SID, msg.getSID());
                    exchange.getIn().setHeader(NatsConstants.NATS_SUBJECT, msg.getSubject());
                    exchange.getIn().setHeader(NatsConstants.NATS_QUEUE_NAME, msg.getSubscription().getQueueName());
                    exchange.getIn().setHeader(NatsConstants.NATS_MESSAGE_TIMESTAMP, System.currentTimeMillis());
                    if (msg.getHeaders() != null) {
                        final HeaderFilterStrategy strategy = NatsConsumer.this.getEndpoint()
                                .getConfiguration()
                                .getHeaderFilterStrategy();
                        msg.getHeaders().entrySet().forEach(entry -> {
                            if (!strategy.applyFilterToExternalHeaders(entry.getKey(), entry.getValue(), exchange)) {
                                if (entry.getValue().size() == 1) {
                                    // going from camel to nats add all headers in lists, so we extract them in the opposite
                                    // way if it contains a single value
                                    exchange.getIn().setHeader(entry.getKey(), entry.getValue().get(0));
                                } else {
                                    exchange.getIn().setHeader(entry.getKey(), entry.getValue());
                                }
                            } else {
                                LOG.debug("Excluding header {} as per strategy", entry.getKey());
                            }
                        });
                    }
                    NatsConsumer.this.processor.process(exchange);

                    // Is there a reply?
                    if (!NatsConsumingTask.this.configuration.isReplyToDisabled()
                            && msg.getReplyTo() != null && msg.getConnection() != null) {
                        final Connection con = msg.getConnection();
                        final byte[] data = exchange.getMessage().getBody(byte[].class);
                        if (data != null) {
                            LOG.debug("Publishing replyTo: {} message", msg.getReplyTo());
                            con.publish(msg.getReplyTo(), data);
                        }
                    }

                    final long ackSync = configuration.getAckSync();
                    if (ackSync == 0) {
                        msg.ack();
                    } else {
                        msg.ackSync(Duration.ofMillis(ackSync));
                    }
                } catch (final Exception e) {
                    NatsConsumer.this.getExceptionHandler().handleException("Error during processing", exchange, e);

                    if (msg.isJetStream()) {
                        final int maxDeliveries = configuration.getMaximumDeliveryAttempts();
                        if (maxDeliveries > 0 && maxDeliveries <= msg.metaData().deliveredCount()) {
                            msg.term();
                        }

                        final long retryDelay = configuration.getRetryDelay();
                        msg.nakWithDelay(retryDelay);
                    }
                } finally {
                    NatsConsumer.this.releaseExchange(exchange, false);
                }
            }
        }
    }

    private final class PullSubscribeFetcher implements Runnable {

        private final NatsConfiguration configuration;
        private final JetStreamSubscription subscription;

        private PullSubscribeFetcher(NatsConfiguration configuration, JetStreamSubscription subscription) {
            this.configuration = configuration;
            this.subscription = subscription;
        }

        @Override
        public void run() {
            while (subscription.isActive() && !executor.isShutdown()) {
                fetch();
            }
        }

        private void fetch() {
            try {
                subscription.pullExpiresIn(configuration.getPullBatchSize(), configuration.getPullDuration());
            } catch (final Exception e) {
                getExceptionHandler().handleException("Error pulling messages for pull subscription", e);
            }
        }
    }

}
