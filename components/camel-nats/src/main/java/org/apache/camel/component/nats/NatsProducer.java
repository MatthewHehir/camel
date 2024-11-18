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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.nats.client.Connection;
import io.nats.client.Connection.Status;
import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangeTimedOutException;
import org.apache.camel.InvalidPayloadException;
import org.apache.camel.spi.ExecutorServiceManager;
import org.apache.camel.spi.HeaderFilterStrategy;
import org.apache.camel.spi.ThreadPoolProfile;
import org.apache.camel.support.DefaultAsyncProducer;
import org.apache.camel.util.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NatsProducer extends DefaultAsyncProducer {

    private static final Logger LOG = LoggerFactory.getLogger(NatsProducer.class);

    private final ExecutorServiceManager executorServiceManager;

    private ScheduledExecutorService scheduler;

    private Connection connection;

    public NatsProducer(NatsEndpoint endpoint) {
        super(endpoint);
        this.executorServiceManager = endpoint.getCamelContext().getExecutorServiceManager();
    }

    @Override
    public NatsEndpoint getEndpoint() {
        return (NatsEndpoint) super.getEndpoint();
    }

    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        final NatsConfiguration config = this.getEndpoint().getConfiguration();
        byte[] body = exchange.getIn().getBody(byte[].class);
        if (body == null) {
            // fallback to use string
            try {
                body = exchange.getIn().getMandatoryBody(String.class).getBytes();
            } catch (final InvalidPayloadException e) {
                exchange.setException(e);
                callback.done(true);
                return true;
            }
        }

        final NatsMessage msg = NatsMessage.builder()
                .data(body)
                .subject(config.getTopic())
                .headers(this.buildHeaders(exchange))
                .replyTo(config.getReplySubject())
                .build();

        // If we're out-capable, then don't use JetStream if configured - we'll sit and wait for the response
        // which gives us our 'ack', and publishing to JetStream won't let us get the response back directly.
        if (exchange.getPattern().isOutCapable()) {
            LOG.debug("Requesting to topic: {}", config.getTopic());

            final CompletableFuture<Message> requestFuture = this.connection.request(msg);
            final CompletableFuture<ExchangeTimedOutException> timeoutFuture = this.failAfter(
                    exchange,
                    Duration.ofMillis(config.getRequestTimeout()));
            CompletableFuture.anyOf(requestFuture, timeoutFuture).whenComplete((message, e) -> {
                if (e == null) {
                    final Message response = (Message) message;
                    exchange.getMessage().setBody(response.getData());
                    exchange.getMessage().setHeader(NatsConstants.NATS_REPLY_TO, response.getReplyTo());
                    exchange.getMessage().setHeader(NatsConstants.NATS_SID, response.getSID());
                    exchange.getMessage().setHeader(NatsConstants.NATS_SUBJECT, response.getSubject());
                    exchange.getMessage().setHeader(NatsConstants.NATS_QUEUE_NAME, response.getSubscription().getQueueName());
                    exchange.getMessage().setHeader(NatsConstants.NATS_MESSAGE_TIMESTAMP, System.currentTimeMillis());
                } else {
                    exchange.setException(e.getCause());
                }
                callback.done(false);
                if (!requestFuture.isDone()) {
                    requestFuture.cancel(true);
                }
                if (!timeoutFuture.isDone()) {
                    timeoutFuture.cancel(true);
                }
            });
            return false;
        } else if (config.isJetStream()) {
            LOG.debug("Publishing using JetStream to topic: {}", config.getTopic());
            try {
                PublishAck ack = this.connection.jetStream().publish(msg);
                exchange.getMessage().setHeader(NatsConstants.NATS_SUBJECT, msg.getSubject());
                exchange.getMessage().setHeader(NatsConstants.NATS_STREAM_NAME, ack.getStream());
                exchange.getMessage().setHeader(NatsConstants.NATS_SEQUENCE_NO, ack.getSeqno());
                exchange.getMessage().setHeader(NatsConstants.NATS_DOMAIN_NAME, ack.getDomain());
                exchange.getMessage().setHeader(NatsConstants.NATS_WAS_DUPLICATE, ack.isDuplicate());
                exchange.getMessage().setHeader(NatsConstants.NATS_MESSAGE_TIMESTAMP, System.currentTimeMillis());
            } catch (IOException | JetStreamApiException e) {
                exchange.setException(e);
                callback.done(true);
                return true;
            }
        } else {
            LOG.debug("Publishing to topic: {}", config.getTopic());
            this.connection.publish(msg);
        }

        callback.done(true);
        return true;
    }

    private Headers buildHeaders(final Exchange exchange) {
        final Headers headers = new Headers();
        final HeaderFilterStrategy filteringStrategy = this.getEndpoint().getConfiguration().getHeaderFilterStrategy();
        exchange.getIn().getHeaders().forEach((key, value) -> {
            if (!filteringStrategy.applyFilterToCamelHeaders(key, value, exchange)) {
                String headerValue;
                if (value instanceof byte[]) {
                    headerValue = new String((byte[]) value, StandardCharsets.UTF_8);
                } else {
                    headerValue = String.valueOf(value);
                }
                if (headers.get(key) != null) {
                    headers.get(key).add(headerValue);
                } else {
                    headers.add(key, headerValue);
                }
            } else {
                LOG.debug("Excluding header {} as per strategy", key);
            }

        });
        return headers;
    }

    private CompletableFuture<ExchangeTimedOutException> failAfter(Exchange exchange, Duration duration) {
        final CompletableFuture<ExchangeTimedOutException> future = new CompletableFuture<>();
        this.scheduler.schedule(() -> {
            final ExchangeTimedOutException ex = new ExchangeTimedOutException(exchange, duration.toMillis());
            return future.completeExceptionally(ex);
        }, duration.toNanos(), TimeUnit.NANOSECONDS);
        return future;
    }

    @Override
    protected void doStart() throws Exception {
        // try to lookup a pool first based on profile
        ThreadPoolProfile profile
                = this.executorServiceManager.getThreadPoolProfile(NatsConstants.NATS_REQUEST_TIMEOUT_THREAD_PROFILE_NAME);
        if (profile == null) {
            profile = this.executorServiceManager.getDefaultThreadPoolProfile();
        }
        this.scheduler
                = this.executorServiceManager.newScheduledThreadPool(this,
                        NatsConstants.NATS_REQUEST_TIMEOUT_THREAD_PROFILE_NAME, profile);
        super.doStart();
        LOG.debug("Starting Nats Producer");

        LOG.debug("Getting Nats Connection");
        this.connection = this.getEndpoint().getConfiguration().getConnection() != null
                ? this.getEndpoint().getConfiguration().getConnection()
                : this.getEndpoint().getConnection();
    }

    @Override
    protected void doStop() throws Exception {
        if (this.scheduler != null) {
            this.executorServiceManager.shutdownNow(this.scheduler);
        }
        LOG.debug("Stopping Nats Producer");
        if (ObjectHelper.isEmpty(this.getEndpoint().getConfiguration().getConnection())) {
            LOG.debug("Closing Nats Connection");
            if (this.connection != null && !this.connection.getStatus().equals(Status.CLOSED)) {
                if (this.getEndpoint().getConfiguration().isFlushConnection()) {
                    LOG.debug("Flushing Nats Connection");
                    this.connection.flush(Duration.ofMillis(this.getEndpoint().getConfiguration().getFlushTimeout()));
                }
                this.connection.close();
            }
        }
        super.doStop();
    }
}
