/* Generated by camel build tools - do NOT edit this file! */
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
package org.apache.camel.builder.endpoint.dsl;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;
import javax.annotation.processing.Generated;
import org.apache.camel.builder.EndpointConsumerBuilder;
import org.apache.camel.builder.EndpointProducerBuilder;
import org.apache.camel.builder.endpoint.AbstractEndpointBuilder;

/**
 * Send requests to ElasticSearch via Java Client API.
 * 
 * Generated by camel build tools - do NOT edit this file!
 */
@Generated("org.apache.camel.maven.packaging.EndpointDslMojo")
public interface ElasticsearchEndpointBuilderFactory {

    /**
     * Builder for endpoint for the Elasticsearch component.
     */
    public interface ElasticsearchEndpointBuilder
            extends
                EndpointProducerBuilder {
        default AdvancedElasticsearchEndpointBuilder advanced() {
            return (AdvancedElasticsearchEndpointBuilder) this;
        }

        /**
         * The time in ms to wait before connection will time out.
         * 
         * The option is a: <code>int</code> type.
         * 
         * Default: 30000
         * Group: producer
         * 
         * @param connectionTimeout the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder connectionTimeout(int connectionTimeout) {
            doSetProperty("connectionTimeout", connectionTimeout);
            return this;
        }
        /**
         * The time in ms to wait before connection will time out.
         * 
         * The option will be converted to a <code>int</code> type.
         * 
         * Default: 30000
         * Group: producer
         * 
         * @param connectionTimeout the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder connectionTimeout(String connectionTimeout) {
            doSetProperty("connectionTimeout", connectionTimeout);
            return this;
        }
        /**
         * Disconnect after it finish calling the producer.
         * 
         * The option is a: <code>boolean</code> type.
         * 
         * Default: false
         * Group: producer
         * 
         * @param disconnect the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder disconnect(boolean disconnect) {
            doSetProperty("disconnect", disconnect);
            return this;
        }
        /**
         * Disconnect after it finish calling the producer.
         * 
         * The option will be converted to a <code>boolean</code> type.
         * 
         * Default: false
         * Group: producer
         * 
         * @param disconnect the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder disconnect(String disconnect) {
            doSetProperty("disconnect", disconnect);
            return this;
        }
        /**
         * Indicates whether the body of the message contains only documents. By
         * default, it is set to false to be able to do the same requests as
         * what the Document API supports (see
         * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs.html for more details). To ease the migration of routes based on the legacy component camel-elasticsearch-rest, you should consider enabling the mode, especially if your routes do update operations.
         * 
         * The option is a: <code>boolean</code> type.
         * 
         * Default: false
         * Group: producer
         * 
         * @param enableDocumentOnlyMode the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder enableDocumentOnlyMode(boolean enableDocumentOnlyMode) {
            doSetProperty("enableDocumentOnlyMode", enableDocumentOnlyMode);
            return this;
        }
        /**
         * Indicates whether the body of the message contains only documents. By
         * default, it is set to false to be able to do the same requests as
         * what the Document API supports (see
         * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs.html for more details). To ease the migration of routes based on the legacy component camel-elasticsearch-rest, you should consider enabling the mode, especially if your routes do update operations.
         * 
         * The option will be converted to a <code>boolean</code> type.
         * 
         * Default: false
         * Group: producer
         * 
         * @param enableDocumentOnlyMode the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder enableDocumentOnlyMode(String enableDocumentOnlyMode) {
            doSetProperty("enableDocumentOnlyMode", enableDocumentOnlyMode);
            return this;
        }
        /**
         * Starting index of the response.
         * 
         * The option is a: <code>java.lang.Integer</code> type.
         * 
         * Group: producer
         * 
         * @param from the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder from(Integer from) {
            doSetProperty("from", from);
            return this;
        }
        /**
         * Starting index of the response.
         * 
         * The option will be converted to a <code>java.lang.Integer</code>
         * type.
         * 
         * Group: producer
         * 
         * @param from the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder from(String from) {
            doSetProperty("from", from);
            return this;
        }
        /**
         * Comma separated list with ip:port formatted remote transport
         * addresses to use.
         * 
         * The option is a: <code>java.lang.String</code> type.
         * 
         * Group: producer
         * 
         * @param hostAddresses the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder hostAddresses(String hostAddresses) {
            doSetProperty("hostAddresses", hostAddresses);
            return this;
        }
        /**
         * The name of the index to act against.
         * 
         * The option is a: <code>java.lang.String</code> type.
         * 
         * Group: producer
         * 
         * @param indexName the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder indexName(String indexName) {
            doSetProperty("indexName", indexName);
            return this;
        }
        /**
         * The time in ms before retry.
         * 
         * The option is a: <code>int</code> type.
         * 
         * Default: 30000
         * Group: producer
         * 
         * @param maxRetryTimeout the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder maxRetryTimeout(int maxRetryTimeout) {
            doSetProperty("maxRetryTimeout", maxRetryTimeout);
            return this;
        }
        /**
         * The time in ms before retry.
         * 
         * The option will be converted to a <code>int</code> type.
         * 
         * Default: 30000
         * Group: producer
         * 
         * @param maxRetryTimeout the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder maxRetryTimeout(String maxRetryTimeout) {
            doSetProperty("maxRetryTimeout", maxRetryTimeout);
            return this;
        }
        /**
         * What operation to perform.
         * 
         * The option is a:
         * <code>org.apache.camel.component.es.ElasticsearchOperation</code>
         * type.
         * 
         * Group: producer
         * 
         * @param operation the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder operation(org.apache.camel.component.es.ElasticsearchOperation operation) {
            doSetProperty("operation", operation);
            return this;
        }
        /**
         * What operation to perform.
         * 
         * The option will be converted to a
         * <code>org.apache.camel.component.es.ElasticsearchOperation</code>
         * type.
         * 
         * Group: producer
         * 
         * @param operation the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder operation(String operation) {
            doSetProperty("operation", operation);
            return this;
        }
        /**
         * Time in ms during which elasticsearch will keep search context alive.
         * 
         * The option is a: <code>int</code> type.
         * 
         * Default: 60000
         * Group: producer
         * 
         * @param scrollKeepAliveMs the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder scrollKeepAliveMs(int scrollKeepAliveMs) {
            doSetProperty("scrollKeepAliveMs", scrollKeepAliveMs);
            return this;
        }
        /**
         * Time in ms during which elasticsearch will keep search context alive.
         * 
         * The option will be converted to a <code>int</code> type.
         * 
         * Default: 60000
         * Group: producer
         * 
         * @param scrollKeepAliveMs the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder scrollKeepAliveMs(String scrollKeepAliveMs) {
            doSetProperty("scrollKeepAliveMs", scrollKeepAliveMs);
            return this;
        }
        /**
         * Size of the response.
         * 
         * The option is a: <code>java.lang.Integer</code> type.
         * 
         * Group: producer
         * 
         * @param size the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder size(Integer size) {
            doSetProperty("size", size);
            return this;
        }
        /**
         * Size of the response.
         * 
         * The option will be converted to a <code>java.lang.Integer</code>
         * type.
         * 
         * Group: producer
         * 
         * @param size the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder size(String size) {
            doSetProperty("size", size);
            return this;
        }
        /**
         * The timeout in ms to wait before the socket will time out.
         * 
         * The option is a: <code>int</code> type.
         * 
         * Default: 30000
         * Group: producer
         * 
         * @param socketTimeout the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder socketTimeout(int socketTimeout) {
            doSetProperty("socketTimeout", socketTimeout);
            return this;
        }
        /**
         * The timeout in ms to wait before the socket will time out.
         * 
         * The option will be converted to a <code>int</code> type.
         * 
         * Default: 30000
         * Group: producer
         * 
         * @param socketTimeout the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder socketTimeout(String socketTimeout) {
            doSetProperty("socketTimeout", socketTimeout);
            return this;
        }
        /**
         * Enable scroll usage.
         * 
         * The option is a: <code>boolean</code> type.
         * 
         * Default: false
         * Group: producer
         * 
         * @param useScroll the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder useScroll(boolean useScroll) {
            doSetProperty("useScroll", useScroll);
            return this;
        }
        /**
         * Enable scroll usage.
         * 
         * The option will be converted to a <code>boolean</code> type.
         * 
         * Default: false
         * Group: producer
         * 
         * @param useScroll the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder useScroll(String useScroll) {
            doSetProperty("useScroll", useScroll);
            return this;
        }
        /**
         * Index creation waits for the write consistency number of shards to be
         * available.
         * 
         * The option is a: <code>int</code> type.
         * 
         * Default: 1
         * Group: producer
         * 
         * @param waitForActiveShards the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder waitForActiveShards(int waitForActiveShards) {
            doSetProperty("waitForActiveShards", waitForActiveShards);
            return this;
        }
        /**
         * Index creation waits for the write consistency number of shards to be
         * available.
         * 
         * The option will be converted to a <code>int</code> type.
         * 
         * Default: 1
         * Group: producer
         * 
         * @param waitForActiveShards the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder waitForActiveShards(String waitForActiveShards) {
            doSetProperty("waitForActiveShards", waitForActiveShards);
            return this;
        }
        /**
         * The certificate that can be used to access the ES Cluster. It can be
         * loaded by default from classpath, but you can prefix with classpath:,
         * file:, or http: to load the resource from different systems.
         * 
         * This option can also be loaded from an existing file, by prefixing
         * with file: or classpath: followed by the location of the file.
         * 
         * The option is a: <code>java.lang.String</code> type.
         * 
         * Group: security
         * 
         * @param certificatePath the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder certificatePath(String certificatePath) {
            doSetProperty("certificatePath", certificatePath);
            return this;
        }
        /**
         * Enable SSL.
         * 
         * The option is a: <code>boolean</code> type.
         * 
         * Default: false
         * Group: security
         * 
         * @param enableSSL the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder enableSSL(boolean enableSSL) {
            doSetProperty("enableSSL", enableSSL);
            return this;
        }
        /**
         * Enable SSL.
         * 
         * The option will be converted to a <code>boolean</code> type.
         * 
         * Default: false
         * Group: security
         * 
         * @param enableSSL the value to set
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder enableSSL(String enableSSL) {
            doSetProperty("enableSSL", enableSSL);
            return this;
        }
    }

    /**
     * Advanced builder for endpoint for the Elasticsearch component.
     */
    public interface AdvancedElasticsearchEndpointBuilder
            extends
                EndpointProducerBuilder {
        default ElasticsearchEndpointBuilder basic() {
            return (ElasticsearchEndpointBuilder) this;
        }

        /**
         * Whether the producer should be started lazy (on the first message).
         * By starting lazy you can use this to allow CamelContext and routes to
         * startup in situations where a producer may otherwise fail during
         * starting and cause the route to fail being started. By deferring this
         * startup to be lazy then the startup failure can be handled during
         * routing messages via Camel's routing error handlers. Beware that when
         * the first message is processed then creating and starting the
         * producer may take a little time and prolong the total processing time
         * of the processing.
         * 
         * The option is a: <code>boolean</code> type.
         * 
         * Default: false
         * Group: producer (advanced)
         * 
         * @param lazyStartProducer the value to set
         * @return the dsl builder
         */
        default AdvancedElasticsearchEndpointBuilder lazyStartProducer(boolean lazyStartProducer) {
            doSetProperty("lazyStartProducer", lazyStartProducer);
            return this;
        }
        /**
         * Whether the producer should be started lazy (on the first message).
         * By starting lazy you can use this to allow CamelContext and routes to
         * startup in situations where a producer may otherwise fail during
         * starting and cause the route to fail being started. By deferring this
         * startup to be lazy then the startup failure can be handled during
         * routing messages via Camel's routing error handlers. Beware that when
         * the first message is processed then creating and starting the
         * producer may take a little time and prolong the total processing time
         * of the processing.
         * 
         * The option will be converted to a <code>boolean</code> type.
         * 
         * Default: false
         * Group: producer (advanced)
         * 
         * @param lazyStartProducer the value to set
         * @return the dsl builder
         */
        default AdvancedElasticsearchEndpointBuilder lazyStartProducer(String lazyStartProducer) {
            doSetProperty("lazyStartProducer", lazyStartProducer);
            return this;
        }
        /**
         * The class to use when deserializing the documents.
         * 
         * The option is a: <code>java.lang.Class&lt;java.lang.Object&gt;</code>
         * type.
         * 
         * Default: ObjectNode
         * Group: advanced
         * 
         * @param documentClass the value to set
         * @return the dsl builder
         */
        default AdvancedElasticsearchEndpointBuilder documentClass(Class<java.lang.Object> documentClass) {
            doSetProperty("documentClass", documentClass);
            return this;
        }
        /**
         * The class to use when deserializing the documents.
         * 
         * The option will be converted to a
         * <code>java.lang.Class&lt;java.lang.Object&gt;</code> type.
         * 
         * Default: ObjectNode
         * Group: advanced
         * 
         * @param documentClass the value to set
         * @return the dsl builder
         */
        default AdvancedElasticsearchEndpointBuilder documentClass(String documentClass) {
            doSetProperty("documentClass", documentClass);
            return this;
        }
        /**
         * Enable automatically discover nodes from a running Elasticsearch
         * cluster. If this option is used in conjunction with Spring Boot, then
         * it's managed by the Spring Boot configuration (see: Disable Sniffer
         * in Spring Boot).
         * 
         * The option is a: <code>boolean</code> type.
         * 
         * Default: false
         * Group: advanced
         * 
         * @param enableSniffer the value to set
         * @return the dsl builder
         */
        default AdvancedElasticsearchEndpointBuilder enableSniffer(boolean enableSniffer) {
            doSetProperty("enableSniffer", enableSniffer);
            return this;
        }
        /**
         * Enable automatically discover nodes from a running Elasticsearch
         * cluster. If this option is used in conjunction with Spring Boot, then
         * it's managed by the Spring Boot configuration (see: Disable Sniffer
         * in Spring Boot).
         * 
         * The option will be converted to a <code>boolean</code> type.
         * 
         * Default: false
         * Group: advanced
         * 
         * @param enableSniffer the value to set
         * @return the dsl builder
         */
        default AdvancedElasticsearchEndpointBuilder enableSniffer(String enableSniffer) {
            doSetProperty("enableSniffer", enableSniffer);
            return this;
        }
        /**
         * The delay of a sniff execution scheduled after a failure (in
         * milliseconds).
         * 
         * The option is a: <code>int</code> type.
         * 
         * Default: 60000
         * Group: advanced
         * 
         * @param sniffAfterFailureDelay the value to set
         * @return the dsl builder
         */
        default AdvancedElasticsearchEndpointBuilder sniffAfterFailureDelay(int sniffAfterFailureDelay) {
            doSetProperty("sniffAfterFailureDelay", sniffAfterFailureDelay);
            return this;
        }
        /**
         * The delay of a sniff execution scheduled after a failure (in
         * milliseconds).
         * 
         * The option will be converted to a <code>int</code> type.
         * 
         * Default: 60000
         * Group: advanced
         * 
         * @param sniffAfterFailureDelay the value to set
         * @return the dsl builder
         */
        default AdvancedElasticsearchEndpointBuilder sniffAfterFailureDelay(String sniffAfterFailureDelay) {
            doSetProperty("sniffAfterFailureDelay", sniffAfterFailureDelay);
            return this;
        }
        /**
         * The interval between consecutive ordinary sniff executions in
         * milliseconds. Will be honoured when sniffOnFailure is disabled or
         * when there are no failures between consecutive sniff executions.
         * 
         * The option is a: <code>int</code> type.
         * 
         * Default: 300000
         * Group: advanced
         * 
         * @param snifferInterval the value to set
         * @return the dsl builder
         */
        default AdvancedElasticsearchEndpointBuilder snifferInterval(int snifferInterval) {
            doSetProperty("snifferInterval", snifferInterval);
            return this;
        }
        /**
         * The interval between consecutive ordinary sniff executions in
         * milliseconds. Will be honoured when sniffOnFailure is disabled or
         * when there are no failures between consecutive sniff executions.
         * 
         * The option will be converted to a <code>int</code> type.
         * 
         * Default: 300000
         * Group: advanced
         * 
         * @param snifferInterval the value to set
         * @return the dsl builder
         */
        default AdvancedElasticsearchEndpointBuilder snifferInterval(String snifferInterval) {
            doSetProperty("snifferInterval", snifferInterval);
            return this;
        }
    }

    public interface ElasticsearchBuilders {
        /**
         * Elasticsearch (camel-elasticsearch)
         * Send requests to ElasticSearch via Java Client API.
         * 
         * Category: search,monitoring
         * Since: 3.19
         * Maven coordinates: org.apache.camel:camel-elasticsearch
         * 
         * @return the dsl builder for the headers' name.
         */
        default ElasticsearchHeaderNameBuilder elasticsearch() {
            return ElasticsearchHeaderNameBuilder.INSTANCE;
        }
        /**
         * Elasticsearch (camel-elasticsearch)
         * Send requests to ElasticSearch via Java Client API.
         * 
         * Category: search,monitoring
         * Since: 3.19
         * Maven coordinates: org.apache.camel:camel-elasticsearch
         * 
         * Syntax: <code>elasticsearch:clusterName</code>
         * 
         * Path parameter: clusterName (required)
         * Name of the cluster
         * 
         * @param path clusterName
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder elasticsearch(String path) {
            return ElasticsearchEndpointBuilderFactory.endpointBuilder("elasticsearch", path);
        }
        /**
         * Elasticsearch (camel-elasticsearch)
         * Send requests to ElasticSearch via Java Client API.
         * 
         * Category: search,monitoring
         * Since: 3.19
         * Maven coordinates: org.apache.camel:camel-elasticsearch
         * 
         * Syntax: <code>elasticsearch:clusterName</code>
         * 
         * Path parameter: clusterName (required)
         * Name of the cluster
         * 
         * @param componentName to use a custom component name for the endpoint
         * instead of the default name
         * @param path clusterName
         * @return the dsl builder
         */
        default ElasticsearchEndpointBuilder elasticsearch(String componentName, String path) {
            return ElasticsearchEndpointBuilderFactory.endpointBuilder(componentName, path);
        }

    }
    /**
     * The builder of headers' name for the Elasticsearch component.
     */
    public static class ElasticsearchHeaderNameBuilder {
        /**
         * The internal instance of the builder used to access to all the
         * methods representing the name of headers.
         */
        private static final ElasticsearchHeaderNameBuilder INSTANCE = new ElasticsearchHeaderNameBuilder();

        /**
         * The operation to perform.
         * 
         * The option is a: {@code
         * org.apache.camel.component.es.ElasticsearchOperation} type.
         * 
         * Group: producer
         * 
         * @return the name of the header {@code operation}.
         */
        public String operation() {
            return "operation";
        }
        /**
         * The id of the indexed document.
         * 
         * The option is a: {@code String} type.
         * 
         * Group: producer
         * 
         * @return the name of the header {@code indexId}.
         */
        public String indexId() {
            return "indexId";
        }
        /**
         * The name of the index to act against.
         * 
         * The option is a: {@code String} type.
         * 
         * Group: producer
         * 
         * @return the name of the header {@code indexName}.
         */
        public String indexName() {
            return "indexName";
        }
        /**
         * The full qualified name of the class of the document to unmarshall.
         * 
         * The option is a: {@code Class} type.
         * 
         * Default: ObjectNode
         * Group: producer
         * 
         * @return the name of the header {@code documentClass}.
         */
        public String documentClass() {
            return "documentClass";
        }
        /**
         * The index creation waits for the write consistency number of shards
         * to be available.
         * 
         * The option is a: {@code Integer} type.
         * 
         * Group: producer
         * 
         * @return the name of the header {@code waitForActiveShards}.
         */
        public String waitForActiveShards() {
            return "waitForActiveShards";
        }
        /**
         * The starting index of the response.
         * 
         * The option is a: {@code Integer} type.
         * 
         * Group: producer
         * 
         * @return the name of the header {@code scrollKeepAliveMs}.
         */
        public String scrollKeepAliveMs() {
            return "scrollKeepAliveMs";
        }
        /**
         * Set to true to enable scroll usage.
         * 
         * The option is a: {@code Boolean} type.
         * 
         * Group: producer
         * 
         * @return the name of the header {@code useScroll}.
         */
        public String useScroll() {
            return "useScroll";
        }
        /**
         * The size of the response.
         * 
         * The option is a: {@code Integer} type.
         * 
         * Group: producer
         * 
         * @return the name of the header {@code size}.
         */
        public String size() {
            return "size";
        }
        /**
         * The starting index of the response.
         * 
         * The option is a: {@code Integer} type.
         * 
         * Group: producer
         * 
         * @return the name of the header {@code from}.
         */
        public String from() {
            return "from";
        }
        /**
         * Indicates whether the body of the message contains only documents.
         * 
         * The option is a: {@code Boolean} type.
         * 
         * Default: false
         * Group: producer
         * 
         * @return the name of the header {@code enableDocumentOnlyMode}.
         */
        public String enableDocumentOnlyMode() {
            return "enableDocumentOnlyMode";
        }
    }
    static ElasticsearchEndpointBuilder endpointBuilder(String componentName, String path) {
        class ElasticsearchEndpointBuilderImpl extends AbstractEndpointBuilder implements ElasticsearchEndpointBuilder, AdvancedElasticsearchEndpointBuilder {
            public ElasticsearchEndpointBuilderImpl(String path) {
                super(componentName, path);
            }
        }
        return new ElasticsearchEndpointBuilderImpl(path);
    }
}